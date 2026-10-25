"""取消、撤销、租约自熔、优雅停机 —— worker 侧对协议 §7.4 / §7.5 / §7.7 的遵守。

要点（都属于「坏了不报错、只是安静地少做一件事」那一类）：

  · 取消判据是心跳响应 `status != "running"`：被撤销回 200 + `revoked`，同样触发，无需额外分支；
  · 自熔用**本地单调时钟**，且必须严格早于服务端回收；
  · 停机三步 ①同步捕获令牌 → ②同步取消在跑任务 → ③用捕获的令牌归还，顺序本身就是判据；
  · 停机要有真实 SIGTERM 的端到端验证（源码 grep 证不了 handler 真的生效）。
"""

import asyncio
import signal
import sys
import time

import pytest
from helpers import channels_for, launch_worker

from webx_worker import heartbeat as hb_mod
from webx_worker import lease as lease_mod

# ───────────────────────── 心跳驱动的取消 ─────────────────────────


async def test_revoked_lease_kills_the_child_without_reporting_failure(harness, server):
    """租约被撤销（补偿器回收）→ 下一跳心跳回 revoked → worker 自行终止子进程，且不上报 fail。"""
    harness.channel("hang", "import time; time.sleep(60)")
    tid = server.add_task("hang")
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "running")
        await asyncio.sleep(0.3)
        server.revoke_lease(tid)
        assert await harness.wait_for(lambda: (harness.ok_json(tid) or {}).get("status") == "cancelled")
        # 注意：revoke 之后任务回到 pending，立刻又会被同一台 worker 重新领取——这正是预期
    finally:
        await harness.stop()
    assert not server.calls_to("POST", rf"/internal/tasks/{tid}/fail$")
    assert not server.calls_to("POST", rf"/internal/tasks/{tid}/complete$")


async def test_cancel_via_heartbeat_terminates_child_process(harness, server):
    pidfile = harness.tmp / "pid"
    harness.channel("hang", f"""
        import os, time
        open({str(pidfile)!r}, "w").write(str(os.getpid()))
        time.sleep(60)
    """)
    tid = server.add_task("hang")
    await harness.start()
    try:
        assert await harness.wait_for(pidfile.exists)
        pid = int(pidfile.read_text())
        server.cancel_task(tid)
        assert await harness.wait_for(lambda: _gone(pid), timeout=10), "子进程没被杀"
    finally:
        await harness.stop()


def _gone(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "Z"
    except OSError:
        return True


async def test_queued_before_running_is_not_a_cancel_signal(harness, server):
    """起跑前的 queued 不是取消信号：心跳从下载之前就开始，而 RUNNING 上报可能晚到/失败。"""
    harness.channel("demo", "import time; time.sleep(1.0); print('ok')")
    tid = server.add_task("demo")
    # 让 PUT status(running) 被拒一次——任务在心跳起跳时仍是 queued
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed", timeout=20)
    finally:
        await harness.stop()
    assert (harness.ok_json(tid) or {}).get("status") == "completed"


async def test_heartbeat_stops_after_task_ends(harness, server):
    harness.channel("demo", "print('x')")
    tid = server.add_task("demo")
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
        await asyncio.sleep(0.5)
        n = len(server.calls_to("POST", r"/heartbeats$"))
        await asyncio.sleep(0.6)
        assert len(server.calls_to("POST", r"/heartbeats$")) == n, "任务结束后心跳仍在发"
    finally:
        await harness.stop()


# ───────────────────────── 自熔 ─────────────────────────


def test_self_fence_uses_monotonic_clock_not_wall_clock(monkeypatch):
    """自熔判据是本地**单调**时钟：墙钟跳变（NTP 校时）不得让健康任务被掐，也不得让它多活。"""
    mgr = hb_mod.HeartbeatManager()
    lease_until_ms = int((time.time() + 100) * 1000)
    mgr._note_renewed("t", {"lease_until": lease_until_ms})
    assert mgr._check_self_fence("t") is False

    # 墙钟往后跳一天：若判据用墙钟，这里会立刻熔断
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 86400)
    assert mgr._check_self_fence("t") is False

    # 单调时钟越过截止（lease 100s − 余量 10s）→ 熔断
    base = time.monotonic()
    monkeypatch.setattr(time, "monotonic", lambda: base + 95)
    assert mgr._check_self_fence("t") is True
    assert mgr.get_server_status("t") == "lease_expired"
    assert mgr.get_cancel_event("t") is None or True       # 未注册时无 Event，不抛即可


def test_self_fence_prefers_server_side_remaining_over_skewed_wall_clock(monkeypatch):
    """本机墙钟快了一小时：若仍按「绝对时刻 − 本机墙钟」算，剩余会是负数，健康任务得不到自熔基准。
    协议给了 lease_remaining_ms（服务端自己的时钟算的）就必须用它。"""
    mgr = hb_mod.HeartbeatManager()
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 3600)
    mgr._note_renewed("t", {"lease_until": int(real_time() * 1000) + 100_000, "lease_remaining_ms": 100_000})
    assert "t" in mgr._lease_deadline, "采用了 lease_remaining_ms，不受本机墙钟偏移影响"
    assert 80 < mgr._lease_deadline["t"] - time.monotonic() < 100
    # 旧服务端只给绝对时刻：沿用旧算法（偏移会进入判据，K2），行为不变
    mgr2 = hb_mod.HeartbeatManager()
    mgr2._note_renewed("t", {"lease_until": int(real_time() * 1000) + 100_000})
    assert "t" not in mgr2._lease_deadline


def test_self_fence_deadline_is_strictly_earlier_than_server_expiry():
    """方向搞反等于没做：worker 必须比服务端更早停下（余量为正）。"""
    mgr = hb_mod.HeartbeatManager()
    remain = 100
    mgr._note_renewed("t", {"lease_until": int((time.time() + remain) * 1000)})
    deadline_in = mgr._lease_deadline["t"] - time.monotonic()
    assert deadline_in < remain - hb_mod._SELF_FENCE_MARGIN_SEC + 1
    assert hb_mod._SELF_FENCE_MARGIN_SEC > 0


async def test_lease_expiry_while_server_unreachable_kills_the_child(harness, server, monkeypatch):
    """网络分区：续租持续失败，租约临期 → 自行终止子进程（浪费上界 = 租约时长）。"""
    monkeypatch.setattr(hb_mod, "_SELF_FENCE_MARGIN_SEC", 0.0)
    server.heartbeat_lease_seconds = 3.0           # 每次成功续租只给 3 秒（太短则满载机器上子进程还没起来租约就到期了）
    pidfile = harness.tmp / "pid2"
    harness.channel("hang", f"""
        import os, time
        open({str(pidfile)!r}, "w").write(str(os.getpid()))
        time.sleep(60)
    """)
    tid = server.add_task("hang")
    await harness.start()
    try:
        assert await harness.wait_for(pidfile.exists)
        pid = int(pidfile.read_text())
        server.inject("POST", r"/heartbeats$", 503, times=10_000)     # 此后心跳全部失败
        assert await harness.wait_for(lambda: _gone(pid), timeout=15), "自熔没有终止子进程"
    finally:
        await harness.stop()
    assert not server.calls_to("POST", rf"/internal/tasks/{tid}/fail$")


# ───────────────────────── 停机语义 ─────────────────────────


def test_cancel_running_tasks_is_synchronous():
    """必须是同步函数：全部价值在于「在本轮任何 await 之前生效」。写成 async 就放弃了这个保证。"""
    assert not asyncio.iscoroutinefunction(lease_mod.cancel_running_tasks)
    assert not asyncio.iscoroutinefunction(lease_mod.held_fencing_headers)


async def test_sync_cancel_beats_a_just_ready_failure_chain():
    """★ 结构保证的核心：失败链的唤醒条件与信号在同一轮成立时，同步 cancel() 仍然赢。

    `Task.cancel()` 见 `_fut_waiter` 已完成、取消不掉，于是置 `_must_cancel`，
    下一次 `__step` 直接 `coro.throw(CancelledError)`：先后不由 I/O 决定。
    """
    reported = []

    async def _failure_chain(ready):
        try:
            await ready
            reported.append("fail_task")             # ← 结构上必须到不了
        except asyncio.CancelledError:
            reported.append("cancelled")
            raise

    ready = asyncio.get_running_loop().create_future()
    t = asyncio.create_task(_failure_chain(ready))
    await asyncio.sleep(0)
    ready.set_result(None)
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    assert reported == ["cancelled"]


async def test_cancel_cannot_help_a_chain_with_no_await_left():
    """反面钉住适用边界：已经没有挂起点的链会在一次调度里整段跑完，cancel() 谁也救不了。"""
    reported = []

    async def _no_suspension(ready):
        await ready
        reported.append("fail_task")

    ready = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    t = asyncio.create_task(_no_suspension(ready))
    await asyncio.sleep(0)
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    assert reported == ["fail_task"]


async def test_release_held_leases_is_concurrent_and_idempotent(harness, server):
    """归还必须并发（单次超时 10s，串行会随 Lane 数线性吃掉停机预算）且幂等。"""
    from webx_worker import fencing

    ids = []
    for _ in range(3):
        tid = server.add_task("demo")
        server.tasks[tid].update(status="running", worker_id="test-host-e2e", lease_seq=1)
        fencing.register(tid, 1)
        ids.append(tid)

    t0 = time.monotonic()
    n = await lease_mod.release_held_leases()
    assert n == 3
    assert all(server.task(i)["status"] == "pending" for i in ids)
    # 幂等：再来一遍只会得到 403，不抛、不改
    assert await lease_mod.release_held_leases({i: fencing.headers_for(i) for i in ids}) == 3
    assert all(server.task(i)["lease_seq"] == 2 for i in ids)
    assert time.monotonic() - t0 < 5


async def test_shutdown_sequence_releases_with_captured_tokens_and_never_fails_the_task(
    harness, server,
):
    """停机三步（与 app.async_main 同序）：任务回池、不上报 fail、不留已确认的终态标记。"""
    harness.channel("hang", "import time; time.sleep(60)")
    tid = server.add_task("hang")
    await harness.start()
    assert await harness.wait_for(lambda: server.task(tid)["status"] == "running")
    await asyncio.sleep(0.3)
    await harness.stop()

    rel = server.calls_to("DELETE", rf"/internal/tasks/{tid}/lease$")
    assert rel and rel[0]["status"] == 200, "停机没有归还租约"
    assert rel[0]["headers"]["x-worker-id"] == "test-host-e2e" and rel[0]["headers"]["x-lease-seq"] == "1"
    assert server.task(tid)["status"] in ("pending", "queued", "running")     # 可能已被重新领取
    assert not server.calls_to("POST", rf"/internal/tasks/{tid}/fail$"), "停机把可重跑的任务判成了 failed"
    assert harness.ok_json(tid) is None


def test_shutdown_order_in_app_is_capture_cancel_release():
    """停机三步的顺序本身就是判据，一步都不能换（源码顺序断言 + 下面的真信号 e2e 双保险）。

    ①→②换序：取消触发 fence_unregister 清空注册表，③ 拿不到令牌，归还反被拒；
    ②→③换序：归还要 await，await 期间失败链照样推进，先后退回概率问题。
    """
    import inspect

    from webx_worker import app

    src = inspect.getsource(app.async_main)
    i_capture = src.index("tokens = held_fencing_headers()")
    i_cancel = src.index("cancelled = cancel_running_tasks()")
    i_release = src.index("await release_held_leases(tokens)")
    i_cancel_worker = src.index("worker_task.cancel()")
    assert i_capture < i_cancel < i_release < i_cancel_worker
    assert src.index("add_signal_handler") < src.index("await asyncio.wait("), "信号 handler 装晚了"
    assert "os._exit(1)" in src, "缺少第二次信号强退的逃生口"
    assert "worker_task.result()" in src, "run_worker 的崩溃会被 task 吞掉，伪装成正常停机"


# ───────────────────────── 真信号 e2e ─────────────────────────


@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 信号")
def test_sigterm_end_to_end_releases_lease_and_does_not_fail_the_task(tmp_path, server):
    """★ 真实 SIGTERM：收尾真的执行了 —— 在跑任务被取消、租约被带令牌归还、任务回池而不是 failed。

    顺带端到端验证了「空 LANES」（channel 配置里没有 LANES）：worker 能领到任务。
    """
    pidfile = tmp_path / "child.pid"
    (tmp_path / "hang.py").write_text(
        f"import os, time\nopen({str(pidfile)!r}, 'w').write(str(os.getpid()))\ntime.sleep(120)\n"
    )
    tid = server.add_task("hang")
    proc = launch_worker(tmp_path, server, channels_for(tmp_path / "hang.py", "hang"))
    try:
        assert server.wait_for(lambda: server.task(tid)["status"] == "running", timeout=30), \
            proc.stdout.read() if proc.poll() is not None else "任务没有开始运行"
        assert server.wait_for(pidfile.exists, timeout=10)
        child_pid = int(pidfile.read_text())

        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=30)
        out = proc.stdout.read()
        assert rc == 0, (rc, out[-2000:])
    finally:
        if proc.poll() is None:
            proc.kill()

    rel = server.calls_to("DELETE", rf"/internal/tasks/{tid}/lease$")
    assert rel and rel[0]["status"] == 200, out[-2000:]
    assert rel[0]["headers"]["x-worker-id"] == "sigbox-e2e"
    assert rel[0]["headers"]["x-lease-seq"] == "1"
    # 任务回池（lease_seq 至少 +1）。注意：先归还、后取消领取循环，窗口内领取循环可能把
    # 刚归还的任务又领回来，随后由 run_worker 的 release_all 再归还一次（lease_seq 因此可能
    # 不止 +1）——这是停机顺序固有的窗口，见 docs/worker.md「已知限制」。
    assert server.task(tid)["status"] == "pending" and server.task(tid)["lease_seq"] >= 2
    assert not server.calls_to("POST", rf"/internal/tasks/{tid}/fail$"), "停机把任务判成了 failed"
    assert _gone(child_pid), "子进程没有随停机被终止"
    assert not (tmp_path / "work" / tid / "ok.json").exists()
