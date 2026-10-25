"""整机端到端（真实进程）：worker 进程 + 子进程 SDK + uploader 子进程 + 假服务端。

覆盖单测拼不出来的接线：
  · 子进程经 PYTHONPATH 能 import SDK；令牌/地址/存储配置经环境注入；
  · SDK 的步骤上报带 fencing；大产物经磁盘队列交给 uploader，传完 `POST /deliveries` 交付；
  · 失败上报（complete）在服务端不可用时持久化，worker 重启后自动重放（跨重启）。
"""

import signal
import sys
import textwrap

import pytest

from helpers import channels_for, launch_worker

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 信号")


def _stop(proc, sig=signal.SIGTERM, timeout=30):
    if proc.poll() is None:
        proc.send_signal(sig)
    try:
        rc = proc.wait(timeout=timeout)
    finally:
        if proc.poll() is None:
            proc.kill()
    return rc, proc.stdout.read()


def test_sdk_steps_and_async_upload_through_the_whole_stack(tmp_path, server):
    """子进程用 SDK 报步骤、投递大产物 → uploader 子进程分片上传并交付地址。"""
    script = tmp_path / "job.py"
    script.write_text(textwrap.dedent("""
        import os, sys
        from pathlib import Path
        from webx_worker.sdk import result_client as rc
        from webx_worker.sdk.upload_route import deliver_only, deliver_required, deliver

        tid = sys.argv[sys.argv.index("--task-id") + 1]
        rc.update_step_status(tid, sub_step="work", sub_status="running")
        big = Path("big.bin"); big.write_bytes(b"B" * 3000)
        small = Path("small.bin"); small.write_bytes(b"s" * 10)
        # 阈值 0MB：纯交付产物一律走 uploader；required 恒同步
        u1 = deliver_only(tid, big, "big.bin", ["outputs", "big_url"])
        u2 = deliver_required(tid, small, "small.bin", ["outputs", "small_url"])
        rc.update_task_fields(tid, result_path='{"outputs": {"small_url": "%s"}}' % u2, mode="merge")
        print("deliver_only ->", repr(u1), "| deliver_required ->", repr(u2))
    """))
    tid = server.add_task("job")
    proc = launch_worker(tmp_path, server, channels_for(script), extra_env={
        "WEBX_WORKER_UPLOADER_ENABLED": "true", "WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB": "0",
        "WEBX_WORKER_UPLOADER_SCAN_INTERVAL": "0.5",
    })
    try:
        ok = server.wait_for(
            lambda: server.task(tid)["status"] == "completed"
            and (tid, "big.bin") in server.storage
            and "big_url" in server.task(tid)["result_path"].get("outputs", {}),
            timeout=60,
        )
        assert ok, (server.task(tid), proc.stdout.read() if proc.poll() is not None else "")
    finally:
        rc, out = _stop(proc)
    assert rc == 0, out[-3000:]

    t = server.task(tid)
    assert t["sub_step"] == "work"                                   # SDK 经环境里的令牌通过 fencing
    assert server.storage[(tid, "big.bin")] == b"B" * 3000
    assert server.storage[(tid, "small.bin")] == b"s" * 10
    # 落库地址恒用【对外】存储地址（本测试里 STORAGE_URL 指向假服务端）
    assert t["result_path"]["outputs"]["big_url"] == f"{server.url}/public/tasks/{tid}/results/big.bin"
    assert t["result_path"]["outputs"]["small_url"] == f"{server.url}/public/tasks/{tid}/results/small.bin"
    # 交付端点不带 fencing（uploader 手里没有租约令牌）
    deliver = server.calls_to("POST", r"/deliveries$")
    assert deliver and all("x-worker-id" not in c["headers"] for c in deliver)
    # 上传的“先问后传”：交付之前至少查过一次任务状态
    assert server.calls_to("GET", rf"^/internal/tasks/{tid}$")


def test_failed_complete_report_survives_a_restart_and_is_replayed(tmp_path, server):
    """★ 服务端不可用期间 complete 上报失败 → worker 持久化队列 → 重启后自动重放并完成。

    令牌随条目快照落盘（不靠重放时的注册表）：第二个 worker 进程里没有任何租约记忆，
    重放仍能通过服务端的 fencing，完成本该完成的迁移。
    """
    script = tmp_path / "quick.py"
    script.write_text("print('done quickly')")
    tid = server.add_task("job")
    # complete 一律 503：前台 5 次退避后入持久队列
    server.inject("POST", r"/complete$", 503, times=10_000)

    p1 = launch_worker(tmp_path, server, channels_for(script))
    try:
        assert server.wait_for(lambda: len(server.calls_to("POST", r"/complete$")) >= 5, timeout=60), \
            p1.stdout.read() if p1.poll() is not None else "没有尝试 complete"
        # 前台重试耗尽 → 已入队；留一点时间让条目进 PendingReporter
        server.wait_for(lambda: False, timeout=0.5)
    finally:
        rc, out = _stop(p1)
    assert rc == 0, out[-3000:]
    assert server.task(tid)["status"] == "running", "服务端还没收到 complete"
    snap = tmp_path / "work" / "pending_reports.snapshot.jsonl"
    assert snap.exists(), "停机时失败上报没有落盘（SIGTERM 收尾没跑到？）\n" + out[-2000:]

    # 服务端恢复；第二个 worker 进程启动 → 恢复快照并重放
    server._rules.clear()
    p2 = launch_worker(tmp_path, server, channels_for(script))
    try:
        assert server.wait_for(lambda: server.task(tid)["status"] == "completed", timeout=60), \
            p2.stdout.read() if p2.poll() is not None else "重启后没有重放"
        # 服务端处理完到 worker 读到响应之间还有一个往返；别在这个窗口里发 SIGTERM
        # （窗口内被打断的重放会被重新快照——至少一次语义，下一次重放会得到 already_final 而被丢弃，
        # 无害，但会让下面“快照已消费”的断言抖动）
        server.wait_for(lambda: False, timeout=1.0)
    finally:
        rc2, out2 = _stop(p2)
    assert rc2 == 0, out2[-3000:]
    assert not snap.exists(), "重放后快照应被消费（含恢复扫描入队的无令牌条目——它们被服务端永久拒绝并丢弃）\n" + out2[-2000:]


def test_worker_refuses_a_missing_channel_config_by_creating_a_template(tmp_path, server):
    """首次启动没有配置文件：写出最小模板并退出，不带着空配置跑起来。"""
    import subprocess

    from webx_worker._paths import PACKAGE_PARENT

    home = tmp_path / "h"
    out = subprocess.run(
        [sys.executable, "-m", "webx_worker", "--home", str(home)],
        env={"PYTHONPATH": PACKAGE_PARENT, "PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0
    tpl = home / "channel_config" / "default.py"
    assert tpl.exists() and "CHANNELS" in tpl.read_text(encoding="utf-8")
    assert "Channel 配置文件已创建" in out.stdout
