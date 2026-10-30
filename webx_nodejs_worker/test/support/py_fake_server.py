"""跨实现验证用：起 Python 版 FakeTaskServer（协议参考实现），提交一成功一失败两个任务，
等到两个都进入终态后把最终状态以一行 JSON 打到 stdout。第一行 stdout 是 {"url":..., "token":..., "ok":..., "bad":...}。
只在测试里使用，不改动 webx_worker。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from webx_worker.fake_server import FakeTaskServer  # noqa: E402

TERMINAL = ("completed", "failed", "cancelled")

with FakeTaskServer(hold_seconds=0.3) as s:
    ok = s.add_task("demo", params={"sleep": 0.3})
    bad = s.add_task("demo", params={"sleep": 0.1, "fail": True})
    print(json.dumps({"url": s.url, "token": s.token, "ok": ok, "bad": bad}), flush=True)
    s.wait_for(lambda: all(s.task(t)["status"] in TERMINAL for t in (ok, bad)), timeout=40)
    s.wait_for(lambda: bool(s.task(ok)["log_path"]), timeout=5)
    writes = [c for c in s.calls if c["method"] in ("POST", "PUT", "PATCH", "DELETE")
              and c["path"].startswith("/internal/tasks/") and not c["path"].endswith(("/results", "/deliveries"))]
    print(json.dumps({
        "ok": s.task(ok)["status"], "bad": s.task(bad)["status"],
        "bad_error": s.task(bad)["error_message"],
        "rejected": [[c["method"], c["path"], c["status"]] for c in s.calls
                     if c["status"] in (403, 404, 405) and not c["path"].startswith("/internal/tasks/probe")],
        "writes_without_token": [c["path"] for c in writes if not c["headers"].get("x-worker-id")],
        "log_path": s.task(ok)["log_path"],
    }), flush=True)
