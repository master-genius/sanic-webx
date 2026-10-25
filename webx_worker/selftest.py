"""`python -m webx_worker selftest` —— 在本机起一个最小假服务端，跑一遍 worker 的完整生命周期。

无需任何外部依赖（不需要真服务、不需要数据库、不需要网络）：

  1. 起 `fake_server.FakeTaskServer`（协议参考实现：强制 fencing、拒绝旧路径）；
  2. 在临时主目录里放好示例 channel 配置（`examples/channels.py` + `demo_task.py`）；
  3. 以真实子进程拉起 worker（与 systemd 托管时同一个入口 `python -m webx_worker`），
     并开启 uploader（阈值 0 MB，让大产物走异步上传）；
  4. 提交两个任务：一个成功、一个故意失败，逐项核对协议行为；
  5. 向 worker 发 SIGTERM，核对优雅停机（退出码 0）。

用途：升级/迁移后的冒烟、确认本机 Python 环境齐全、给新同事看一遍 worker 到底做了什么。
它验证的是**这份代码对这份协议的遵守**，不是你的真实服务端 —— 后者用 `doctor`。

退出码：0 = 全部通过；1 = 有失败项。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

from ._paths import PACKAGE_PARENT
from .fake_server import FakeTaskServer

_EXAMPLES = Path(__file__).resolve().parent / "examples"


class _Checks:
    def __init__(self) -> None:
        self.failed = 0
        self.total = 0

    def check(self, ok: bool, what: str, detail: str = "") -> bool:
        self.total += 1
        if not ok:
            self.failed += 1
        print(f"  {'[通过]' if ok else '[失败]'} {what}" + (f" —— {detail}" if detail and not ok else ""),
              flush=True)
        return ok


def run(timeout: float = 90.0, keep: bool = False) -> int:
    ck = _Checks()
    tmp = Path(tempfile.mkdtemp(prefix="webx-worker-selftest-"))
    home, work = tmp / "home", tmp / "work"
    (home / "channel_config").mkdir(parents=True)
    shutil.copy(_EXAMPLES / "channels.py", home / "channel_config" / "default.py")
    shutil.copy(_EXAMPLES / "demo_task.py", home / "channel_config" / "demo_task.py")

    print(f"webx_worker selftest —— 临时目录 {tmp}\n")
    server = FakeTaskServer().start()
    print(f"  假服务端 {server.url}")
    ok_id = server.add_task("demo", params={"seconds": 0.5})
    bad_id = server.add_task("demo", params={"seconds": 0.2, "fail": True})

    env = {
        **os.environ,
        "PYTHONPATH": PACKAGE_PARENT,
        "WEBX_WORKER_WEB_BASE_URL": server.url, "WEBX_WORKER_INTERNAL_TOKEN": server.token,
        "WEBX_WORKER_STORAGE_URL": server.url, "WEBX_WORKER_WORK_DIR": str(work),
        "WEBX_WORKER_PREFIX": "selftest", "WEBX_WORKER_LEASE_HOLD_SECONDS": "1",
        "WEBX_WORKER_UPLOADER_ENABLED": "true", "WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB": "0",
        "WEBX_WORKER_UPLOADER_SCAN_INTERVAL": "0.5",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "webx_worker", "run", "--home", str(home)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    out = ""

    def _ok(tid):
        p = work / tid / "ok.json"
        try:
            return json.loads(p.read_text()) if p.exists() else {}
        except ValueError:
            return {}

    try:
        # 等到两个任务的本地完成标记都已“服务端确认”再停机：服务端收到 complete/fail 到 worker
        # 写下 server_confirmed 之间还有一个往返，别在这个窗口里发 SIGTERM
        done = server.wait_for(
            lambda: server.task(ok_id)["status"] == "completed"
            and server.task(bad_id)["status"] == "failed"
            and "payload_url" in server.task(ok_id)["result_path"].get("outputs", {})
            and _ok(ok_id).get("server_confirmed") and _ok(bad_id).get("server_confirmed"),
            timeout=timeout,
        )
        print()
        ck.check(done, "两个任务在时限内走完生命周期（一个 completed、一个 failed）",
                 f"ok={server.task(ok_id)['status']} bad={server.task(bad_id)['status']}")

        # 优雅停机
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = -9
        out = proc.stdout.read() if proc.stdout else ""
        ck.check(rc == 0, "收到 SIGTERM 后优雅退出（退出码 0）", f"rc={rc}")
    finally:
        if proc.poll() is None:
            proc.kill()
        server.stop()

    t_ok, t_bad = server.task(ok_id), server.task(bad_id)
    wid = "selftest-default"

    # —— 协议遵守度 ——
    leases = server.calls_to("POST", r"^/task/leases$")
    ck.check(bool(leases) and leases[0]["json"].get("slots") == [["demo"]],
             "领取请求 POST /task/leases 带能力声明（空 LANES → 单 Lane 装下全部 channel）",
             str(leases[0]["json"] if leases else None))
    paths = [(c["method"], c["path"]) for c in server.calls if c["path"].startswith(f"/internal/tasks/{ok_id}")]
    seq = [p for p in paths if p[1].endswith(("/ack", "/status", "/complete"))]
    ck.check(seq[:2] == [("POST", f"/internal/tasks/{ok_id}/ack"), ("PUT", f"/internal/tasks/{ok_id}/status")]
             and seq[-1] == ("POST", f"/internal/tasks/{ok_id}/complete"),
             "认领后顺序：ack → 上报 running → … → complete", str(seq))
    writes = [c for c in server.calls if c["method"] in ("POST", "PUT", "PATCH", "DELETE")
              and c["path"].startswith("/internal/tasks/")
              and not c["path"].endswith(("/results", "/results/uploads", "/deliveries"))]
    ck.check(bool(writes) and all(c["headers"].get("x-worker-id") == wid and c["headers"].get("x-lease-seq")
                                  for c in writes),
             "所有任务写请求都带 fencing 令牌（X-Worker-Id / X-Lease-Seq）")
    ck.check(all(c["status"] != 403 for c in writes), "没有任何写请求被 fencing 拒绝（子进程令牌注入正常）",
             str([(c['method'], c['path'], c['status']) for c in writes if c['status'] == 403][:3]))
    ck.check(not [c for c in server.calls if c["status"] in (404, 405)
                  and not c["path"].startswith("/internal/tasks/probe")],
             "没有请求命中过期路径/错误方法")

    # —— 子进程 SDK ——
    ck.check(t_ok["sub_step"] == "" and bool(server.calls_to("PATCH", rf"/internal/tasks/{ok_id}/steps$")),
             "子进程经 SDK 上报了步骤进度（PATCH /steps）")
    ck.check((ok_id, "result.txt") in server.storage
             and "report_url" in t_ok["result_path"].get("outputs", {}),
             "同步产物 result.txt 已上传到存储服务并写入 result_path")
    ck.check((ok_id, "payload.bin") in server.storage and "payload_url" in t_ok["result_path"].get("outputs", {}),
             "大产物 payload.bin 经 uploader 异步上传并 POST /deliveries 交付")
    ck.check(all("x-worker-id" not in c["headers"] for c in server.calls_to("POST", r"/deliveries$")),
             "交付端点不带 fencing（uploader 手里没有租约令牌）")
    ck.check(t_ok["log_path"].get("log", "").endswith("/run.log") and (ok_id, "run.log") in server.storage,
             "步骤日志已上传并写入 log_path")

    # —— 失败路径 ——
    ck.check(t_bad["status"] == "failed" and "退出码: 3" in t_bad["error_message"]
             and "failing on purpose" in t_bad["error_message"],
             "失败任务上报 fail，错误信息带子进程退出码与日志尾部")

    # —— 本地完成标记 ——
    ck.check(_ok(ok_id).get("status") == "completed" and _ok(ok_id).get("server_confirmed") is True,
             "本地完成标记 ok.json：先写后确认（completed / server_confirmed=true）")
    ck.check(_ok(bad_id).get("status") == "failed" and _ok(bad_id).get("server_confirmed") is True,
             "失败任务的 ok.json 同样在服务端确认后标记已确认")

    print(f"\n{ck.total - ck.failed}/{ck.total} 项通过" + ("" if not ck.failed else f"，{ck.failed} 项失败"))
    if ck.failed:
        print("\n── worker 输出（末 4000 字符）──\n" + out[-4000:])
    if keep or ck.failed:
        print(f"\n临时目录保留在 {tmp}")
    else:
        shutil.rmtree(tmp, ignore_errors=True)
    return 1 if ck.failed else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m webx_worker selftest",
                                 description="本机最小假服务端 + 完整生命周期自检（无外部依赖）")
    ap.add_argument("--timeout", type=float, default=90.0, help="等待任务走完的秒数上限（默认 90）")
    ap.add_argument("--keep", action="store_true", help="保留临时目录（排查用）")
    args = ap.parse_args(argv)
    return run(args.timeout, args.keep)
