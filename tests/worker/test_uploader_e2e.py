"""uploader 对着假服务端（任务服务 + 存储服务）的端到端行为。

协议要点（docs/worker-protocol.md §4.9 / §5 与 docs/storage-service.md）：
  · 上传前先 `GET /internal/tasks/<id>` 复查任务状态（已取消/失败/不存在 → 丢弃，不传）；
  · 小文件整传 `POST /internal/tasks/<id>/results`（201），大文件分片：
    `POST .../results/uploads` → `PATCH /internal/uploads/<id>`（Upload-Offset）→
    `POST /internal/uploads/<id>/complete`；
  · 传完 `POST /internal/tasks/<id>/deliveries`：**不带 fencing**（uploader 手里没有租约令牌）。
"""

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from webx_worker import uploader as U
from webx_worker.config import worker_settings
from webx_worker.link_health import observed_async_client
from webx_worker.sdk import upload_queue as q


@pytest.fixture
def up(server, tmp_path, monkeypatch):
    for k, v in {
        "WEB_BASE_URL": server.url, "INTERNAL_TOKEN": server.token,
        "STORAGE_URL": "", "STORAGE_INTERNAL_URL": server.url, "STORAGE_INTERNAL_TOKEN": "",
        "WORK_DIR": str(tmp_path / "work"), "UPLOADER_SCAN_INTERVAL": 0.5,
    }.items():
        monkeypatch.setattr(worker_settings, k, v)
    work = tmp_path / "work"
    q.ensure_dirs(work)
    return work


def _entry(work: Path, tid: str, size: int, key=("step2", "out_url"), name="step2.out"):
    f = work.parent / f"{tid}_{name}"
    f.write_bytes(bytes(i % 251 for i in range(size)))
    assert q.enqueue(work, tid, f, name, list(key))
    return q.iter_pending(work)[0], f


async def _process(work, entry):
    async with observed_async_client(timeout=U._HTTP_TIMEOUT) as client:
        await U._process(client, entry, str(work))


async def test_small_file_whole_upload_then_delivery_without_fencing(server, up):
    tid = server.add_task("demo")
    entry, f = _entry(up, tid, 700)
    await _process(up, entry)

    assert server.storage[(tid, "step2.out")] == f.read_bytes()
    assert q.iter_pending(up) == []
    # 顺序：先复查任务状态，再上传，最后交付
    seq = [(c["method"], c["path"]) for c in server.calls if c["path"].startswith("/internal/tasks/")]
    assert seq == [("GET", f"/internal/tasks/{tid}"),
                   ("POST", f"/internal/tasks/{tid}/results"),
                   ("POST", f"/internal/tasks/{tid}/deliveries")]
    deliver = server.calls_to("POST", r"/deliveries$")[0]
    assert "x-worker-id" not in deliver["headers"] and "x-lease-seq" not in deliver["headers"], \
        "交付端点刻意不做 fencing，uploader 不该带租户信息"
    assert deliver["json"] == {"result_path": {"step2": {"out_url": f"/public/tasks/{tid}/results/step2.out"}}}
    assert server.task(tid)["result_path"] == {"step2": {"out_url": f"/public/tasks/{tid}/results/step2.out"}}


async def test_public_url_prefix_is_applied_to_delivered_address(server, up, monkeypatch):
    monkeypatch.setattr(worker_settings, "STORAGE_URL", "https://files.example.com/")
    tid = server.add_task("demo")
    entry, _ = _entry(up, tid, 10)
    await _process(up, entry)
    assert server.task(tid)["result_path"]["step2"]["out_url"] == \
        f"https://files.example.com/public/tasks/{tid}/results/step2.out"


async def test_large_file_uses_resumable_session_protocol(server, up, monkeypatch):
    monkeypatch.setattr(U, "CHUNK_SIZE", 1000)
    tid = server.add_task("demo")
    entry, f = _entry(up, tid, 4500)
    await _process(up, entry)

    assert server.storage[(tid, "step2.out")] == f.read_bytes()
    create = server.calls_to("POST", rf"/internal/tasks/{tid}/results/uploads$")
    assert len(create) == 1 and create[0]["status"] == 201
    assert create[0]["json"] == {"filename": "step2.out", "total": 4500}
    patches = server.calls_to("PATCH", r"^/internal/uploads/")
    assert [p["headers"]["upload-offset"] for p in patches] == ["0", "1000", "2000", "3000", "4000"]
    assert server.calls_to("POST", r"/complete$")[0]["status"] == 200
    assert not server.calls_to("POST", rf"/internal/tasks/{tid}/results$"), "大文件不该整传"
    assert q.iter_pending(up) == []


async def test_resume_from_session_after_crash_sends_only_the_remaining_bytes(server, up, monkeypatch):
    """崩溃重启后凭 session_id 取 Upload-Offset 响应头接着传，不从头再来几 GB。"""
    monkeypatch.setattr(U, "CHUNK_SIZE", 1000)
    tid = server.add_task("demo")
    entry, f = _entry(up, tid, 3500)
    data = f.read_bytes()
    server.uploads["up_crashed"] = {"task_id": tid, "filename": "step2.out", "total": 3500,
                                    "data": bytearray(data[:2000])}
    entry.session_id = "up_crashed"
    claimed = q.claim(up, entry)
    q.update(up, claimed)
    q.requeue_inflight(up)
    entry = q.iter_pending(up)[0]
    assert entry.session_id == "up_crashed"

    await _process(up, entry)
    patches = server.calls_to("PATCH", r"^/internal/uploads/")
    assert [p["headers"]["upload-offset"] for p in patches] == ["2000", "3000"]
    assert server.storage[(tid, "step2.out")] == data
    assert not server.calls_to("POST", r"/results/uploads$"), "会话还在，不该重新创建"


async def test_expired_session_is_recreated(server, up, monkeypatch):
    monkeypatch.setattr(U, "CHUNK_SIZE", 1000)
    tid = server.add_task("demo")
    entry, f = _entry(up, tid, 2500)
    entry.session_id = "up_gone"                    # 服务端已回收的会话
    q.claim(up, entry)
    q.update(up, entry)
    q.requeue_inflight(up)
    await _process(up, q.iter_pending(up)[0])
    assert server.storage[(tid, "step2.out")] == f.read_bytes()
    assert server.calls_to("POST", r"/results/uploads$"), "会话过期必须重新创建"


class _StubPatchClient:
    """只为 409 重同步场景：第一次 PATCH 回 409 + Upload-Offset 头，之后成功。"""

    def __init__(self):
        self.patches = []

    async def post(self, url, **kw):
        import httpx
        if url.endswith("/results/uploads"):
            return httpx.Response(201, json={"upload_id": "u1", "offset": 0},
                                  request=httpx.Request("POST", url))
        return httpx.Response(200, json={"download_url": "/public/x"},
                              request=httpx.Request("POST", url))

    async def patch(self, url, **kw):
        import httpx
        self.patches.append((kw["headers"]["Upload-Offset"], len(kw["content"])))
        if len(self.patches) == 1:
            return httpx.Response(409, headers={"Upload-Offset": "3000"},
                                  request=httpx.Request("PATCH", url))
        return httpx.Response(200, headers={"Upload-Offset": "x"}, request=httpx.Request("PATCH", url))


async def test_409_resyncs_from_the_upload_offset_response_header(up, monkeypatch, tmp_path):
    """服务端 offset 与我们的不一致 → 409，客户端按响应头 Upload-Offset 重新对齐（不是 JSON 字段）。"""
    monkeypatch.setattr(U, "CHUNK_SIZE", 1000)
    f = tmp_path / "a.bin"
    f.write_bytes(b"z" * 4000)
    entry = q.Entry(task_id="t1", local_path=str(f), remote_name="a.bin", key_path=["a", "u"],
                    size=4000)
    stub = _StubPatchClient()
    rel = await U._upload_resumable(stub, entry, str(up))
    assert rel == "/public/x"
    assert stub.patches[0] == ("0", 1000)
    assert stub.patches[1][0] == "3000", f"没有按 409 响应头重同步: {stub.patches}"
    assert stub.patches[1][1] == 1000


async def test_cancelled_midway_aborts_the_session_and_drops_the_entry(server, up, monkeypatch):
    """几 GB 传一半任务被取消 → 周期性复查发现 → 放弃会话、删条目，不再白烧带宽。"""
    monkeypatch.setattr(U, "CHUNK_SIZE", 1000)
    monkeypatch.setattr(U, "RECHECK_EVERY_CHUNKS", 2)
    tid = server.add_task("demo")
    entry, _ = _entry(up, tid, 10_000)

    def hook(rec):
        if rec["method"] == "PATCH" and rec["headers"].get("upload-offset") == "1000":
            server.cancel_task(tid)

    server.hooks.append(hook)
    await _process(up, entry)
    assert (tid, "step2.out") not in server.storage
    assert server.uploads == {} and server.calls_to("DELETE", r"^/internal/uploads/"), \
        "会话必须被放弃（幂等 DELETE）"
    assert len(server.calls_to("PATCH", r"^/internal/uploads/")) < 10, "取消后仍在继续传"
    # 中途放弃时条目先被退回队列（session_id 已清）；下一轮扫描在上传前的复查就会把它丢掉
    left = q.iter_pending(up)
    assert len(left) == 1 and left[0].session_id == ""
    await _process(up, left[0])
    assert q.iter_pending(up) == []


async def test_dead_task_is_not_uploaded_at_all(server, up):
    tid = server.add_task("demo")
    server.cancel_task(tid)
    entry, _ = _entry(up, tid, 100)
    await _process(up, entry)
    assert q.iter_pending(up) == [] and not server.storage
    assert not server.calls_to("POST", r"/results$")


async def test_soft_deleted_task_is_not_uploaded(server, up):
    """协议 §4.10：软删任务对 GET 回 404 → 判为不存在 → 丢弃，不白传已删任务的产物。"""
    tid = server.add_task("demo")
    server.soft_delete(tid)
    entry, _ = _entry(up, tid, 100)
    await _process(up, entry)
    assert q.iter_pending(up) == [] and not server.storage


async def test_delivery_404_with_error_body_drops_but_route_404_retains(server, up):
    tid = server.add_task("demo")
    entry, _ = _entry(up, tid, 50)
    server.inject("POST", r"/deliveries$", 404, times=1, body={"error": "task not found"})
    await _process(up, entry)
    assert q.iter_pending(up) == [], "任务不存在（带 error 体）→ 丢弃"

    tid2 = server.add_task("demo")
    entry2, _ = _entry(up, tid2, 50, name="o2")
    server.inject("POST", r"/deliveries$", 404, times=1,
                  body={"description": "Not Found", "message": "Requested URL not found"})
    await _process(up, entry2)
    kept = q.iter_pending(up)
    assert len(kept) == 1 and kept[0].uploaded_url, "路由缺失的 404 必须保留，且不丢已上传的 URL"


async def test_auth_failure_on_upload_keeps_the_entry(server, up, monkeypatch):
    monkeypatch.setattr(worker_settings, "STORAGE_INTERNAL_TOKEN", "wrong-token")
    tid = server.add_task("demo")
    entry, _ = _entry(up, tid, 50)
    await _process(up, entry)
    kept = q.iter_pending(up)
    assert len(kept) == 1 and kept[0].attempts == 1 and not kept[0].uploaded_url


# ───────────────────── 真进程：python -m webx_worker uploader ─────────────────────

def _uploader_env(server, work: Path) -> dict:
    from webx_worker._paths import PACKAGE_PARENT

    return {
        **os.environ, "PYTHONPATH": PACKAGE_PARENT,
        "WEBX_WORKER_WEB_BASE_URL": server.url, "WEBX_WORKER_INTERNAL_TOKEN": server.token,
        "WEBX_WORKER_STORAGE_INTERNAL_URL": server.url, "WEBX_WORKER_WORK_DIR": str(work),
        "WEBX_WORKER_UPLOADER_SCAN_INTERVAL": "0.5",
    }


@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 信号")
def test_uploader_process_delivers_and_exits_cleanly_on_sigterm(server, tmp_path):
    work = tmp_path / "work"
    q.ensure_dirs(work)
    tid = server.add_task("demo")
    f = tmp_path / "o.bin"
    f.write_bytes(b"q" * 2048)
    q.enqueue(work, tid, f, "o.bin", ["s", "u"])

    proc = subprocess.Popen([sys.executable, "-m", "webx_worker", "uploader"],
                            env=_uploader_env(server, work), stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    try:
        assert server.wait_for(lambda: server.task(tid)["result_path"], timeout=30), \
            proc.stdout.read() if proc.poll() is not None else "uploader 没有交付"
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
    assert server.storage[(tid, "o.bin")] == b"q" * 2048
    assert q.iter_pending(work) == []


@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 信号")
def test_uploader_process_refuses_to_start_without_storage(server, tmp_path):
    env = _uploader_env(server, tmp_path / "work")
    env["WEBX_WORKER_STORAGE_INTERNAL_URL"] = ""
    env["WEBX_WORKER_STORAGE_URL"] = ""
    out = subprocess.run([sys.executable, "-m", "webx_worker", "uploader"], env=env,
                         capture_output=True, text=True, timeout=30)
    assert out.returncode == 0
    assert "未配置存储服务" in out.stdout + out.stderr


async def test_supervisor_injects_config_into_the_child_and_stops_it(server, tmp_path, monkeypatch):
    """父进程按真环境变量把配置注入 uploader 子进程（配置文件层传不过去），退出时收走。"""
    from webx_worker.uploader_supervisor import UploaderSupervisor

    work = tmp_path / "work"
    q.ensure_dirs(work)
    for k, v in {
        "WEB_BASE_URL": server.url, "INTERNAL_TOKEN": server.token, "STORAGE_URL": "",
        "STORAGE_INTERNAL_URL": server.url, "STORAGE_INTERNAL_TOKEN": "",
        "WORK_DIR": str(work), "UPLOADER_ENABLED": True, "UPLOADER_SCAN_INTERVAL": 0.5,
    }.items():
        monkeypatch.setattr(worker_settings, k, v)
    tid = server.add_task("demo")
    f = tmp_path / "s.bin"
    f.write_bytes(b"s" * 100)
    q.enqueue(work, tid, f, "s.bin", ["s", "u"])

    sup = UploaderSupervisor()
    sup.start()
    try:
        deadline = asyncio.get_running_loop().time() + 30
        while asyncio.get_running_loop().time() < deadline and not server.task(tid)["result_path"]:
            await asyncio.sleep(0.2)
        assert server.task(tid)["result_path"], "监督下的 uploader 没有交付"
    finally:
        await sup.stop()
    assert sup._proc is None


async def test_supervisor_does_not_start_without_storage(tmp_path, monkeypatch, caplog):
    from webx_worker.uploader_supervisor import UploaderSupervisor

    for k, v in {"STORAGE_URL": "", "STORAGE_INTERNAL_URL": "", "UPLOADER_ENABLED": True,
                 "WORK_DIR": str(tmp_path / "w")}.items():
        monkeypatch.setattr(worker_settings, k, v)
    sup = UploaderSupervisor()
    sup.start()
    assert sup._task is None and "未配置存储服务" in caplog.text
    await sup.stop()


async def test_backpressure_gate_reads_the_queue_on_disk(tmp_path, monkeypatch):
    from webx_worker import uploader_supervisor as S

    work = tmp_path / "work"
    q.ensure_dirs(work)
    f = tmp_path / "big"
    with open(f, "wb") as fh:
        fh.truncate(2 * 1024 ** 3)
    q.enqueue(work, "t1", f, "big", ["a", "u"])
    monkeypatch.setattr(worker_settings, "WORK_DIR", str(work))
    monkeypatch.setattr(worker_settings, "UPLOADER_PENDING_LIMIT_GB", 1.0)
    assert S.is_backpressured() is True
    monkeypatch.setattr(worker_settings, "UPLOADER_PENDING_LIMIT_GB", 0)
    assert S.is_backpressured() is False, "0 = 不限"
    monkeypatch.setattr(worker_settings, "UPLOADER_PENDING_LIMIT_GB", 100.0)
    assert S.is_backpressured() is False
