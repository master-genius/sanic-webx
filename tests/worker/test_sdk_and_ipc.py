"""子进程侧 SDK（webx_worker.sdk）与 IPC（AF_UNIX）。

SDK 在任务子进程里运行：令牌与地址只能来自环境变量（executor 注入）。
这里直接在测试进程里设置同样的环境变量，对着强制 fencing 的假服务端验证。
"""

import asyncio
import json
import os
import sys

import httpx
import pytest

from webx_worker import envnames as E
from webx_worker.ipc_server import IPCServer
from webx_worker.sdk import ipc_client, progress, result_client as RC


@pytest.fixture
def child_env(server, harness, tmp_path, monkeypatch):
    """模拟 executor 注入给子进程的环境，并让任务 tid 处于 running + 租户=本机。"""
    tid = server.add_task("demo")
    server.tasks[tid].update(status="running", worker_id="test-host-e2e", lease_seq=4)
    for k, v in {
        E.ENV_WORKER_ID: "test-host-e2e", E.ENV_LEASE_SEQ: "4", E.ENV_TASK_ID: tid,
        E.ENV_INTERNAL_TOKEN: server.token, E.ENV_WEB_BASE_URL: server.url,
        E.ENV_STORAGE_INTERNAL_URL: server.url, E.ENV_STORAGE_URL: "",
        E.ENV_WORK_DIR: str(tmp_path / "work"),
    }.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv(E.ENV_IPC_SOCK, raising=False)
    monkeypatch.delenv(E.ENV_STORAGE_INTERNAL_TOKEN, raising=False)
    monkeypatch.setattr(RC, "_REPORT_BACKOFF_BASE_SEC", 0.0)
    monkeypatch.setattr(RC, "_UPLOAD_BACKOFF_BASE_SEC", 0.0)
    # 隔离 ipc_client 的模块级状态
    monkeypatch.setattr(ipc_client, "_SOCK", None)
    monkeypatch.setattr(ipc_client, "_FILE_FALLBACK_ACTIVE", False)
    return tid


async def _sync(fn, *a, **k):
    """SDK 是同步阻塞的；放进线程，让测试进程里的事件循环（IPC server）继续转。"""
    return await asyncio.to_thread(lambda: fn(*a, **k))


# ───────────────────────── result_client ─────────────────────────

async def test_update_step_status_patches_with_fencing_from_env(server, child_env):
    tid = child_env
    r = await _sync(RC.update_step_status, tid, steps=["a"], sub_step="b", sub_status="running")
    assert r["ok"] is True
    c = server.calls_to("PATCH", r"/steps$")[0]
    assert c["headers"]["x-worker-id"] == "test-host-e2e" and c["headers"]["x-lease-seq"] == "4"
    assert server.task(tid)["sub_step"] == "b"


async def test_update_fields_merge_and_overwrite_modes(server, child_env):
    tid = child_env
    await _sync(RC.update_task_fields, tid, result_path='{"a": {"x": 1}}', mode="overwrite")
    await _sync(RC.update_task_fields, tid, result_path='{"a": {"y": 2}}', mode="merge")
    assert server.task(tid)["result_path"] == {"a": {"x": 1, "y": 2}}
    qs = [c["query"] for c in server.calls_to("PATCH", r"/fields$")]
    assert qs == [{}, {"mode": "merge"}]


async def test_missing_token_in_env_is_rejected_by_the_server(server, child_env, monkeypatch):
    """★ 漏注入令牌的后果：子进程所有写入被 403。SDK 不会伪造令牌。

    （403 不在 SDK 的“业务拒绝”集合里，所以会按 5xx 路径把 5 次都试满再转 IPC 兜底——
    与父进程侧对 401/403 “只试 1 次”不一致，属原实现遗留，见 docs/worker.md 已知限制。）"""
    tid = child_env
    monkeypatch.delenv(E.ENV_WORKER_ID)
    monkeypatch.delenv(E.ENV_LEASE_SEQ)
    sent = []
    monkeypatch.setattr(RC, "send_report", lambda action, payload, task_id=None: sent.append(action))
    r = await _sync(RC.update_step_status, tid, sub_step="x")
    assert r["message"] == "Queued for async retry" and sent == ["update_step_status"]
    calls = server.calls_to("PATCH", r"/steps$")
    assert len(calls) == RC._REPORT_MAX_ATTEMPTS and all(c["status"] == 403 for c in calls)
    assert all("x-worker-id" not in c["headers"] for c in calls)
    assert server.task(tid)["sub_step"] == ""


async def test_5xx_exhaustion_hands_over_to_ipc_for_durable_retry(server, child_env, monkeypatch):
    tid = child_env
    sent = []
    monkeypatch.setattr(RC, "send_report", lambda action, payload, task_id=None: sent.append(
        (action, payload, task_id)))
    server.inject("PATCH", r"/steps$", 503, times=99)
    r = await _sync(RC.update_step_status, tid, sub_step="b", sub_status="running")
    assert r["message"] == "Queued for async retry"
    assert len(server.calls_to("PATCH", r"/steps$")) == RC._REPORT_MAX_ATTEMPTS
    assert sent == [("update_step_status", {"sub_step": "b", "sub_status": "running"}, tid)]


async def test_fields_5xx_exhaustion_carries_the_merge_mode_over_ipc(server, child_env, monkeypatch):
    tid = child_env
    sent = []
    monkeypatch.setattr(RC, "send_report", lambda action, payload, task_id=None: sent.append(
        (action, payload)))
    server.inject("PATCH", r"/fields$", 502, times=99)
    await _sync(RC.update_task_fields, tid, log_path='{"s": "u"}', mode="merge")
    assert sent == [("update_task_fields", {"log_path": '{"s": "u"}', "_mode": "merge"})]


async def test_business_reject_raises_and_is_not_queued(server, child_env, monkeypatch):
    tid = child_env
    sent = []
    monkeypatch.setattr(RC, "send_report", lambda *a, **k: sent.append(a))
    server.inject("PATCH", r"/steps$", 400, times=1, body={"error": "bad"})
    with pytest.raises(httpx.HTTPError):
        await _sync(RC.update_step_status, tid, sub_step="x")
    assert sent == [] and len(server.calls_to("PATCH", r"/steps$")) == 1


async def test_progress_message_is_best_effort_and_never_queued(server, child_env, monkeypatch):
    """进度消息失败即弃：绝不能为一条提示让计算干等，更不能入持久队列（过期进度补发是倒退）。"""
    tid = child_env
    sent = []
    monkeypatch.setattr(RC, "send_report", lambda *a, **k: sent.append(a))
    server.inject("PATCH", r"/steps$", 503, times=99)
    await _sync(progress.report_step_message, tid, "", "Packing items")
    assert sent == []
    assert len(server.calls_to("PATCH", r"/steps$")) == RC._PROGRESS_MAX_ATTEMPTS
    # None 才是“没有”；空串是合法 sub_step（单阶段任务）
    server.calls.clear()
    await _sync(progress.report_step_message, None, "", "x")
    await _sync(progress.report_step_message, tid, None, "x")
    assert server.calls == []


async def test_upload_result_goes_to_storage_and_normalizes_the_public_url(server, child_env, monkeypatch, tmp_path):
    tid = child_env
    monkeypatch.setenv(E.ENV_STORAGE_URL, "https://files.example.com")
    f = tmp_path / "a.bin"
    f.write_bytes(b"abc")
    r = await _sync(RC.upload_result, tid, f, "renamed.bin")
    assert r["download_url"] == f"https://files.example.com/public/tasks/{tid}/results/renamed.bin"
    assert server.storage[(tid, "renamed.bin")] == b"abc"
    assert server.calls_to("POST", r"/results$")[0]["status"] == 201


async def test_upload_without_storage_config_fails_loudly(server, child_env, monkeypatch, tmp_path):
    monkeypatch.setenv(E.ENV_STORAGE_INTERNAL_URL, "")
    f = tmp_path / "a.bin"
    f.write_bytes(b"abc")
    with pytest.raises(EnvironmentError, match="没有回退路径"):
        await _sync(RC.upload_result, child_env, f)


async def test_storage_token_precedence(server, child_env, monkeypatch, tmp_path):
    f = tmp_path / "a.bin"
    f.write_bytes(b"abc")
    server.storage_token = "dedicated"
    monkeypatch.setenv(E.ENV_STORAGE_INTERNAL_TOKEN, "dedicated")
    assert (await _sync(RC.upload_result, child_env, f))["size"] == 3
    monkeypatch.delenv(E.ENV_STORAGE_INTERNAL_TOKEN)           # 回退主令牌 → 与服务端不符 → 401
    with pytest.raises(httpx.HTTPError):
        await _sync(RC.upload_result, child_env, f)


async def test_download_artifact_and_list(server, child_env, tmp_path):
    tid = child_env
    server.storage[(tid, "r.bin")] = b"hello"
    assert await _sync(RC.list_artifacts, tid) == ["r.bin"]
    dst = tmp_path / "dl" / "r.bin"
    assert await _sync(RC.download_artifact, tid, "r.bin", dst) is True and dst.read_bytes() == b"hello"
    assert await _sync(RC.download_artifact, tid, "nope.bin", tmp_path / "x") is False
    assert await _sync(RC.download_artifact_stream, tid, "r.bin", tmp_path / "s.bin") is True
    assert (tmp_path / "s.bin").read_bytes() == b"hello"


def test_stream_download_resumes_from_part_file_with_range(tmp_path, monkeypatch):
    """断点续传：从 .part 现有长度发 Range，服务端回 206 就追加；完成后原子改名。"""
    monkeypatch.setenv(E.ENV_STORAGE_INTERNAL_URL, "http://storage.test")
    data = bytes(range(200))
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        rng = request.headers.get("Range")
        seen.append(rng)
        start = int(rng.split("=")[1].rstrip("-")) if rng else 0
        status = 206 if rng else 200
        return httpx.Response(status, stream=httpx.ByteStream(data[start:]), headers={
            "Content-Range": f"bytes {start}-{len(data) - 1}/{len(data)}"} if rng else {})

    final = tmp_path / "f.bin"
    (tmp_path / "f.bin.part").write_bytes(data[:80])
    with httpx.Client(transport=httpx.MockTransport(handler)) as c:
        assert RC.download_artifact_stream("t1", "f.bin", final, client=c, backoff_base=0) is True
    assert seen == ["bytes=80-"] and final.read_bytes() == data
    assert not (tmp_path / "f.bin.part").exists()


def test_resolve_storage_url():
    os.environ[E.ENV_STORAGE_INTERNAL_URL] = "http://10.0.0.5:9010/"
    try:
        assert RC.resolve_storage_url("/public/tasks/t/results/a") == "http://10.0.0.5:9010/public/tasks/t/results/a"
        assert RC.resolve_storage_url("https://x/y") == "https://x/y" and RC.resolve_storage_url("") == ""
    finally:
        del os.environ[E.ENV_STORAGE_INTERNAL_URL]


# ───────────────────────── IPC：子进程 → 父进程 ─────────────────────────

class _Reporter:
    def __init__(self):
        self.entries = []

    def enqueue_nowait(self, entry):
        self.entries.append(entry)


@pytest.mark.skipif(sys.platform == "win32", reason="AF_UNIX")
async def test_ipc_roundtrip_carries_fencing_snapshot_and_context(child_env, tmp_path, monkeypatch):
    sock = tmp_path / "ipc.sock"
    rep = _Reporter()
    srv = IPCServer(sock, rep)
    await srv.start()
    try:
        monkeypatch.setenv(E.ENV_IPC_SOCK, str(sock))
        ipc_client.set_task_context(child_env, sub_step="s1", steps_done=["s0"])
        await _sync(ipc_client.send_report, "update_step_status", {"sub_step": "s1"})
        for _ in range(100):
            if rep.entries:
                break
            await asyncio.sleep(0.02)
    finally:
        await srv.close()
    e = rep.entries[0]
    assert e["task_id"] == child_env and e["action"] == "update_step_status"
    assert e["fencing"] == {"worker_id": "test-host-e2e", "lease_seq": 4}
    assert e["context"]["sub_step"] == "s1" and e["context"]["steps_done"] == ["s0"]


async def test_ipc_server_survives_garbage_lines(tmp_path):
    sock = tmp_path / "ipc2.sock"
    rep = _Reporter()
    srv = IPCServer(sock, rep)
    await srv.start()
    try:
        r, w = await asyncio.open_unix_connection(str(sock))
        w.write(b"not json\n[1,2]\n" + json.dumps({"no_task_id": 1}).encode() + b"\n"
                + json.dumps({"task_id": "ok1", "action": "x"}).encode() + b"\n")
        await w.drain()
        for _ in range(100):
            if rep.entries:
                break
            await asyncio.sleep(0.02)
        w.close()
    finally:
        await srv.close()
    assert [e["task_id"] for e in rep.entries] == ["ok1"]
    assert not sock.exists(), "close 后必须清理 socket 文件"


async def test_ipc_unavailable_falls_back_to_a_file_for_startup_recovery(child_env, tmp_path, monkeypatch):
    monkeypatch.setenv(E.ENV_IPC_SOCK, str(tmp_path / "nobody-listens.sock"))
    await _sync(ipc_client.send_report, "update_task_fields", {"result_path": "{}"}, task_id=child_env)
    f = tmp_path / "work" / child_env / "pending_reports.jsonl"
    entry = json.loads(f.read_text().splitlines()[0])
    assert entry["action"] == "update_task_fields" and entry["fencing"]["lease_seq"] == 4
    # 一旦降级，本次子进程内不再重试 IPC
    assert ipc_client._FILE_FALLBACK_ACTIVE is True


async def test_send_report_without_any_task_id_is_dropped_with_a_log(monkeypatch, caplog):
    monkeypatch.setattr(ipc_client._CTX, "task_id", "")
    ipc_client.send_report("update_status", {})
    assert "未提供 task_id" in caplog.text


def test_ipc_fencing_snapshot_rejects_malformed_tokens(monkeypatch):
    monkeypatch.setenv(E.ENV_WORKER_ID, "w")
    monkeypatch.setenv(E.ENV_LEASE_SEQ, "not-int")
    assert ipc_client._fencing_snapshot() is None
    monkeypatch.setenv(E.ENV_LEASE_SEQ, "5")
    assert ipc_client._fencing_snapshot() == {"worker_id": "w", "lease_seq": 5}
    monkeypatch.delenv(E.ENV_WORKER_ID)
    assert ipc_client._fencing_snapshot() is None
