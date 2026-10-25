"""端到端生命周期 —— worker 对着假服务端的真 HTTP 跑完整一轮。

这是对 docs/worker-protocol.md 的「遵守度」测试：路径、方法、fencing 头、调用顺序。
假服务端对写端点强制 fencing、对旧路径回 404、错方法回 405，所以：

  · 漏注入令牌（父进程请求钩子 / 子进程环境）→ 对应用例红；
  · 用了旧路径或旧方法 → 404/405 → 状态推不上去 → 用例红。
"""

import json
import sys

import pytest

# 子进程里的任务脚本：只用标准库 + envnames，经环境变量里的 fencing 令牌回调服务端。
# （SDK 的完整测试在 test_sdk_*；这里刻意不依赖它，隔离「父进程 → 子进程令牌传递」这一件事。）
CHILD_REPORT = """
import json, os, sys, urllib.request
from webx_worker import envnames as E

task_id = sys.argv[sys.argv.index("--task-id") + 1]
base = os.environ[E.ENV_WEB_BASE_URL]
req = urllib.request.Request(
    f"{base}/internal/tasks/{task_id}/steps", method="PATCH",
    data=json.dumps({"sub_step": "work", "sub_status": "running"}).encode(),
    headers={
        "Content-Type": "application/json",
        E.HEADER_INTERNAL_TOKEN: os.environ[E.ENV_INTERNAL_TOKEN],
        E.HEADER_WORKER_ID: os.environ.get(E.ENV_WORKER_ID, ""),
        E.HEADER_LEASE_SEQ: os.environ.get(E.ENV_LEASE_SEQ, ""),
    },
)
try:
    urllib.request.urlopen(req, timeout=10)
    print("child-report-ok")
except urllib.error.HTTPError as e:
    print("child-report-rejected", e.code, e.read().decode())
    sys.exit(3)
"""


async def test_happy_path_full_lifecycle(harness, server):
    """领取 → ack → running → 执行 → 日志上传 → complete，全程路径/方法/fencing 合规。"""
    harness.channel("demo", CHILD_REPORT)
    tid = server.add_task("demo", params={"n": 3})
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
        assert await harness.wait_for(
            lambda: (harness.ok_json(tid) or {}).get("server_confirmed") is True
        )
    finally:
        await harness.stop()

    t = server.task(tid)
    # 子进程经环境里的令牌通过了 fencing（若令牌没注入，这里 steps 不会更新、脚本退出码为 3）
    assert t["sub_step"] == "work" and t["sub_status"] == "running"

    # 领取请求：POST /task/leases，header 带 worker_id，body 带 slots（能力声明）
    lease_calls = server.calls_to("POST", r"^/task/leases$")
    assert lease_calls, "没有发出领取请求"
    first = lease_calls[0]
    assert first["headers"]["x-worker-id"] == "test-host-e2e"
    assert first["json"]["worker_id"] == "test-host-e2e"
    assert first["json"]["slots"] == [["demo"]]
    assert "max_level" not in first["json"] and "max_compute_points" not in first["json"]

    # 协议 §7.2 的认领后顺序：ack → 上报 running → ... → complete
    paths = [(c["method"], c["path"]) for c in server.calls
             if c["path"].startswith(f"/internal/tasks/{tid}")]
    seq = [p for p in paths if p[1].endswith(("/ack", "/status", "/complete"))]
    assert seq[0] == ("POST", f"/internal/tasks/{tid}/ack")
    assert seq[1] == ("PUT", f"/internal/tasks/{tid}/status")
    assert seq[-1] == ("POST", f"/internal/tasks/{tid}/complete")

    # 所有写请求都带 fencing 头，且与认领时拿到的 lease_seq 一致
    writes = [c for c in server.calls
              if c["method"] in ("POST", "PUT", "PATCH", "DELETE")
              and c["path"].startswith("/internal/tasks/")
              and "/results" not in c["path"]]
    assert writes
    for c in writes:
        assert c["headers"].get("x-worker-id") == "test-host-e2e", c["path"]
        assert c["headers"].get("x-lease-seq") == "1", c["path"]
        assert c["status"] == 200, (c["method"], c["path"], c["status"])

    # 步骤日志走存储服务（POST /internal/tasks/<id>/results，201），再用 PATCH /fields 写 log_path
    uploads = server.calls_to("POST", rf"^/internal/tasks/{tid}/results$")
    assert uploads and uploads[0]["status"] == 201
    assert (tid, "run.log") in server.storage
    assert t["log_path"].get("log", "").endswith(f"/public/tasks/{tid}/results/run.log")
    # 本地完成标记
    ok = harness.ok_json(tid)
    assert ok["status"] == "completed" and ok["server_confirmed"] is True
    # 令牌随终态标记落盘：崩溃后 recovery 重试 complete 要靠它（否则是无令牌写，必被拒）
    assert ok["lease_seq"] == 1 and ok["worker_id"] == t["worker_id"], ok


async def test_child_without_fencing_env_is_rejected_by_server(harness, server):
    """对照：子进程不带令牌时，假服务端确实会 403（证明上面的通过不是服务端放水）。"""
    harness.channel("demo", """
        import json, os, sys, urllib.request, urllib.error
        from webx_worker import envnames as E
        task_id = sys.argv[sys.argv.index("--task-id") + 1]
        req = urllib.request.Request(
            os.environ[E.ENV_WEB_BASE_URL] + f"/internal/tasks/{task_id}/steps", method="PATCH",
            data=b'{"sub_step": "x"}',
            headers={"Content-Type": "application/json",
                     E.HEADER_INTERNAL_TOKEN: os.environ[E.ENV_INTERNAL_TOKEN]})  # 故意不带令牌
        try:
            urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as e:
            print(e.code, e.read().decode()); sys.exit(0 if e.code == 403 else 5)
        sys.exit(6)
    """)
    tid = server.add_task("demo")
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
    finally:
        await harness.stop()
    assert server.task(tid)["sub_step"] == ""          # 未被更新
    assert any(c["status"] == 403 for c in server.calls_to("PATCH", r"/steps$"))


async def test_failed_script_reports_fail_with_log_tail(harness, server):
    """脚本非零退出 → fail（带日志尾部摘要），ok.json 记 failed 并在上报成功后确认。"""
    harness.channel("boom", """
        import sys
        print("about to fail"); sys.exit(7)
    """)
    tid = server.add_task("boom")
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "failed")
        assert await harness.wait_for(
            lambda: (harness.ok_json(tid) or {}).get("server_confirmed") is True
        )
    finally:
        await harness.stop()
    err = server.task(tid)["error_message"]
    assert "退出码: 7" in err and "about to fail" in err
    assert harness.ok_json(tid)["status"] == "failed"


async def test_old_protocol_paths_are_not_used(harness, server):
    """worker 发出的所有请求都命中新协议的路由（没有一个 404/405）。"""
    harness.channel("demo", "print('hi')")
    tid = server.add_task("demo")
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
    finally:
        await harness.stop()
    bad = [(c["method"], c["path"], c["status"]) for c in server.calls if c["status"] in (404, 405)]
    assert bad == [], f"出现了旧路径/错方法: {bad}"


async def test_inputs_are_downloaded_with_percent_encoded_names(harness, server):
    """输入文件按协议 §4.11 下载，文件名必须百分号编码（含空格/中文）。"""
    harness.channel("demo", """
        import os, sys
        assert open("in/样 本 1.txt", "rb").read() == b"payload", "输入文件内容不对"
        print("inputs-ok")
    """)
    tid = server.add_task("demo", files={"样 本 1.txt": b"payload"})
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
    finally:
        await harness.stop()
    gets = server.calls_to("GET", rf"^/internal/tasks/{tid}/files/")
    assert gets and gets[0]["status"] == 200
    p = harness.work_dir / tid
    assert json.loads((p / "params.json").read_text()) == {}
    assert json.loads((p / "files.json").read_text())[0]["name"] == "样 本 1.txt"
    assert json.loads((p / "meta.json").read_text())["task_type"] == "demo"


async def test_lease_filters_are_sent_when_configured(harness, server, monkeypatch):
    """max_level / max_compute_points 是通用筛选配置：配置了才带，且是整台机器的属性。"""
    from webx_worker.config import worker_settings

    monkeypatch.setattr(worker_settings, "LEASE_MAX_LEVEL", "0")
    monkeypatch.setattr(worker_settings, "LEASE_MAX_COMPUTE_POINTS", "100")
    harness.channel("demo", "print('x')")
    cheap = server.add_task("demo", level=0, compute_points=50)
    pricey = server.add_task("demo", level=2, compute_points=500)
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(cheap)["status"] == "completed")
        await harness.wait_for(lambda: False, timeout=0.6)      # 给越界任务一个被误领的机会
    finally:
        await harness.stop()
    body = server.calls_to("POST", r"^/task/leases$")[0]["json"]
    assert body["max_level"] == 0 and body["max_compute_points"] == 100
    assert server.task(pricey)["status"] == "pending", "越过上限的任务不该被领取"


@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 进程组")
async def test_two_tasks_run_serially_on_one_lane(harness, server):
    """Lane 是槽位不是队列：单 Lane 下两个任务严格串行（第二个在第一个结束后才被领取）。"""
    harness.channel("slow", """
        import time; time.sleep(0.6); print("done")
    """)
    a = server.add_task("slow")
    b = server.add_task("slow")
    await harness.start()
    try:
        assert await harness.wait_for(
            lambda: server.task(a)["status"] == "completed" and server.task(b)["status"] == "completed",
            timeout=30,
        )
    finally:
        await harness.stop()
    runs = [c for c in server.calls if c["method"] == "PUT" and c["path"].endswith("/status")]
    completes = [c for c in server.calls if c["path"].endswith("/complete")]
    # 第二个任务的 running 上报晚于第一个任务的 complete
    assert runs[1]["t"] > completes[0]["t"]
