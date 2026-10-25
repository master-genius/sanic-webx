"""审查修复的回归用例：全部轻量（毫秒到一两秒），只验证基本逻辑。"""

import asyncio
import json
import logging
import sys
import time

import httpx
import pytest

from webx_worker import callback as cb_mod
from webx_worker import lease as lease_mod
from webx_worker.callback import callback_client
from webx_worker.lane_manager import LaneManager
from webx_worker.pending_reporter import PendingReporter

from tests.worker.test_executor import _make


# ───────────── W1：传输层异常要按“可重试”处理 ─────────────


@pytest.mark.parametrize("exc", [httpx.ConnectTimeout("syn dropped"), httpx.WriteTimeout("w"), httpx.PoolTimeout("p")])
async def test_transport_timeouts_are_retried_not_raised(monkeypatch, exc):
    """ConnectTimeout / WriteTimeout / PoolTimeout 不是 OSError 的子类。原先它们直接抛出，已跑完的任务
    因此落进“失败”分支被作废重跑（防火墙丢 SYN 就能触发）。"""

    class _Client:
        async def request(self, *a, **k):
            raise exc

    async def get_client():
        return _Client()

    monkeypatch.setattr(callback_client, "_get_client", get_client)
    monkeypatch.setattr(cb_mod, "_CALLBACK_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(cb_mod, "_CALLBACK_BACKOFF_BASE_SEC", 0)
    resp, last = await callback_client._send_with_retry("POST", "http://x/internal/tasks/abc123/complete", {})
    assert resp is None and last is exc


# ───────────── W2：通用异常分支要冷却 ─────────────


async def test_generic_failure_in_run_one_cools_down_instead_of_hot_looping(monkeypatch):
    class _LM:
        num_lanes = 1
        task_to_lane = {"demo": 0}

        def types_of(self, _):
            return ["demo"]

    class _Exec:
        async def execute(self, *a):
            raise OSError("No space left on device")

    class _CB:
        async def ack_lease(self, tid):
            return True

        async def update_status(self, tid, st):
            return True

        async def release_lease(self, tid, extra_headers=None):
            return True

    monkeypatch.setattr(cb_mod, "callback_client", _CB())
    monkeypatch.setattr(lease_mod, "_ACK_FAIL_COOLDOWN_SEC", 0.3)
    d = lease_mod.LeaseDispatcher(_LM(), _Exec())
    t0 = time.monotonic()
    await d._run_one(0, {"id": "t-1", "task_type": "demo", "lease_seq": 1})
    assert time.monotonic() - t0 >= 0.25, "释放租约后必须冷却，否则同一台 worker 会立刻又领到同一个任务"


# ───────────── W3 / W9：子进程组与退出码 ─────────────


def _alive(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/status") as f:
            return "\nState:\tZ" not in f.read()
    except OSError:
        return False


async def test_descendants_left_behind_by_a_finished_script_are_killed(tmp_path):
    """脚本主进程退出了，却留下继承 stdout 的后台子孙：以 stdout EOF 判结束会让 Lane 被占到超时，
    且 killpg(getpgid(...)) 在组长被回收后杀不掉它们。"""
    ex = _make(tmp_path)
    work = tmp_path / "w"
    work.mkdir()
    pidfile = tmp_path / "child.pid"
    script = (
        "import subprocess,sys;"
        f"p=subprocess.Popen(['sleep','60']);open({str(pidfile)!r},'w').write(str(p.pid))"
    )
    t0 = time.monotonic()
    rc = await ex.run_subprocess([sys.executable, "-c", script], "t1", work, timeout=30, heartbeat=None)
    assert rc == 0 and time.monotonic() - t0 < 10, "主进程退出后不能等遗留子孙的管道"
    pid = int(pidfile.read_text())
    for _ in range(40):
        if not _alive(pid):
            break
        await asyncio.sleep(0.05)
    assert not _alive(pid), "遗留的子孙进程必须随任务结束被清理"


async def test_a_child_killed_by_sighup_is_a_failure_not_a_cancellation(tmp_path):
    """run_subprocess 里 -1 专指“被取消”。子进程自己被 SIGHUP 杀死时 returncode 也是 -1，原样返回会被当成用户取消
    （不上报失败、ok.json 写 cancelled），任务一直 running 到租约到期。"""
    ex = _make(tmp_path)
    work = tmp_path / "w"
    work.mkdir()
    rc = await ex.run_subprocess(
        [sys.executable, "-c", "import os,signal;os.kill(os.getpid(),signal.SIGHUP)"], "t1", work,
        timeout=30, heartbeat=None)
    assert rc not in (0, -1)


# ───────────── W6：领取响应不是预期 JSON ─────────────


@pytest.mark.parametrize("body", ["<html>bad gateway</html>", "[1,2]", '{"tasks": "x"}'])
async def test_acquire_with_a_200_that_is_not_the_expected_json_backs_off_instead_of_raising(monkeypatch, body):
    class _Client:
        async def post(self, *a, **k):
            return httpx.Response(200, text=body)

    monkeypatch.setattr(lease_mod.lease_client, "_get", lambda: _Client())
    assert await lease_mod.lease_client.acquire([["demo"]]) is None


# ───────────── W8：LANES 里有 CHANNELS 没有的类型 ─────────────


def test_lanes_entries_not_declared_in_channels_are_excluded_from_capability(caplog):
    """拼写错误的类型若进入能力声明，worker 会领走任务再因“未配置执行环境”把用户任务判成永久失败。"""
    with caplog.at_level(logging.ERROR):
        lm = LaneManager([["demo", "typo"]], channel_types=["demo"])
    assert lm.types_of(0) == ["demo"] and "typo" not in lm.task_to_lane
    assert "typo" in caplog.text


# ───────────── W7：停机时重试池不丢条目 ─────────────


async def test_sweep_cancelled_midway_puts_every_unprocessed_entry_back(tmp_path):
    rep = PendingReporter(callback=None, work_base_dir=str(tmp_path))
    entered = asyncio.Event()

    async def slow(entry):
        entered.set()
        await asyncio.sleep(30)

    rep._try_report = slow
    rep._retry_pool = [{"n": i} for i in range(5)]
    sweep = asyncio.create_task(rep._sweep_retry_pool())
    await entered.wait()
    sweep.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sweep
    assert [e["n"] for e in rep._retry_pool] == [0, 1, 2, 3, 4], "含正在发送的那条：重放幂等，宁可多送也不丢"


# ───────────── W10：IPC 大事件 ─────────────


async def test_ipc_accepts_a_line_larger_than_64kib_and_keeps_the_connection(tmp_path):
    from webx_worker.ipc_server import IPCServer

    got = []

    class _Rep:
        def enqueue_nowait(self, entry):
            got.append(entry)

    srv = IPCServer(tmp_path / "ipc.sock", _Rep())
    await srv.start()
    try:
        _, w = await asyncio.open_unix_connection(str(tmp_path / "ipc.sock"))
        big = {"task_id": "t1", "action": "x", "payload": {"blob": "a" * 200_000}}
        w.write((json.dumps(big) + "\n" + json.dumps({"task_id": "t1", "action": "y"}) + "\n").encode())
        await w.drain()
        for _ in range(50):
            if len(got) >= 2:
                break
            await asyncio.sleep(0.05)
        w.close()
    finally:
        await srv.close()
    assert [e["action"] for e in got] == ["x", "y"], "超过 64KiB 的行以前会让这条和之后的事件一起静默丢失"
