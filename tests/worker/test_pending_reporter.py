"""PendingReporter —— 失败上报的持久重放：「永不退出，永不放弃」，但永久拒绝必须出队。

判据（协议 §7.6）：**持久队列区分「永不放弃」与「永久拒绝」的依据是响应体里的 `reason`，
不是状态码。** `not_lessee` / `gone` 永久放弃，`already_final` 视为成功；而 401（令牌填错）
和不带终态 reason 的 403 保留——一次配置错误不能清空整个队列。
"""

import json

import pytest

from webx_worker import fencing
from webx_worker import pending_reporter as PR
from webx_worker.callback import callback_client
from webx_worker.pending_reporter import PendingReporter


@pytest.fixture
def rep(server, harness, tmp_path):
    r = PendingReporter(callback=callback_client, work_base_dir=str(tmp_path / "work"))
    PR.set_global_reporter(r)
    yield r
    PR.set_global_reporter(None)


def _running_task(server, seq=3, worker="test-host-e2e"):
    tid = server.add_task("demo")
    server.tasks[tid].update(status="running", worker_id=worker, lease_seq=seq)
    return tid


def _entry(tid, action, payload=None, seq=3, worker="test-host-e2e", fenced=True):
    e = {"task_id": tid, "action": action, "payload": payload or {}, "context": {}, "ts": 0.0}
    if fenced:
        e["fencing"] = {"worker_id": worker, "lease_seq": seq}
    return e


# ───────────────────── 分类：判据是 reason，不是状态码 ─────────────────────

async def test_success_dequeues(server, rep):
    tid = _running_task(server)
    assert await rep._try_report(_entry(tid, "complete_task")) == "success"
    assert server.task(tid)["status"] == "completed"


@pytest.mark.parametrize("code", [400, 404])
async def test_business_rejects_are_dropped(server, rep, code):
    tid = _running_task(server)
    server.inject("POST", r"/complete$", code, times=1)
    assert await rep._try_report(_entry(tid, "complete_task")) == "drop"


@pytest.mark.parametrize("reason", ["not_lessee", "gone", "already_final"])
async def test_fencing_terminal_reasons_are_dropped(server, rep, reason):
    tid = _running_task(server)
    server.inject("POST", r"/complete$", 403, times=1, body={"ok": False, "reason": reason})
    assert await rep._try_report(_entry(tid, "complete_task")) == "drop"


async def test_real_stale_token_is_dropped_via_the_servers_reason(server, rep):
    """不靠注入：令牌与服务端不一致（任务已被换代），服务端的 403 not_lessee 被识别并丢弃。"""
    tid = _running_task(server, seq=5)
    assert await rep._try_report(_entry(tid, "complete_task", seq=4)) == "drop"
    assert server.task(tid)["status"] == "running"


async def test_401_is_retained_for_the_operator_to_fix(server, rep):
    tid = _running_task(server)
    server.inject("POST", r"/complete$", 401, times=1, body={"error": "unauthorized"})
    assert await rep._try_report(_entry(tid, "complete_task")) == "retry"


async def test_403_without_a_terminal_reason_is_retained_not_dropped(server, rep):
    """★ 判据是 reason 而不是状态码：任何别的 403（或解析不出 reason）一律保守保留，
    否则一次配置错误就会清空整个队列。"""
    tid = _running_task(server)
    for body in ({"ok": False, "reason": "missing_fencing_token"}, {"error": "forbidden"}, None):
        server.inject("POST", r"/complete$", 403, times=1, body=body)
        assert await rep._try_report(_entry(tid, "complete_task")) == "retry", body


async def test_5xx_and_network_errors_are_retried(server, rep, monkeypatch):
    from webx_worker.config import worker_settings

    tid = _running_task(server)
    server.inject("POST", r"/complete$", 503, times=1)
    assert await rep._try_report(_entry(tid, "complete_task")) == "retry"
    monkeypatch.setattr(worker_settings, "WEB_BASE_URL", "http://127.0.0.1:1")   # 连不上
    assert await rep._try_report(_entry(tid, "complete_task")) == "retry"


async def test_unknown_action_is_dropped(server, rep):
    tid = _running_task(server)
    assert await rep._try_report(_entry(tid, "no_such_action")) == "drop"


# ───────────────────── 令牌随条目走，不靠重放时的注册表 ─────────────────────

async def test_replay_uses_the_token_snapshotted_in_the_entry(server, rep):
    """★ 重放发生在租约注销之后（跨重启更是从盘上恢复）：注册表此刻是空的，
    令牌只能来自条目自己。走 JSON 体第二通道（协议 §2）。"""
    tid = _running_task(server)
    assert fencing.lease_seq_of(tid) is None
    assert await rep._try_report(_entry(tid, "complete_task")) == "success"
    call = server.calls_to("POST", r"/complete$")[-1]
    assert "x-worker-id" not in call["headers"], "重放不该依赖请求头钩子"
    assert call["json"] == {"worker_id": "test-host-e2e", "lease_seq": 3}


async def test_entry_without_token_is_a_permanent_rejection(server, rep):
    """缺令牌的条目永远不可能被接受（没有谁会事后给它补令牌）：服务端回 not_lessee → 丢弃，
    不能在“永不放弃”的队列里变成僵尸。（recovery 重试 complete 时入队的就是这种条目。）"""
    tid = _running_task(server)
    assert await rep._try_report(_entry(tid, "complete_task", fenced=False)) == "drop"


# ───────────────────── action → 方法/路径 ─────────────────────

async def test_actions_map_to_the_protocol_methods(server, rep):
    tid = _running_task(server)
    server.set_status(tid, "queued")
    assert await rep._try_report(_entry(tid, "update_status", {"status": "running"})) == "success"
    assert server.calls_to("PUT", rf"/internal/tasks/{tid}/status$")

    assert await rep._try_report(
        _entry(tid, "update_step_status", {"sub_step": "a", "sub_status": "running"})) == "success"
    assert server.calls_to("PATCH", rf"/internal/tasks/{tid}/steps$")
    assert server.task(tid)["sub_step"] == "a"

    assert await rep._try_report(
        _entry(tid, "update_task_fields", {"result_path": '{"k": 1}', "_mode": "merge"})) == "success"
    c = server.calls_to("PATCH", rf"/internal/tasks/{tid}/fields$")[-1]
    assert c["query"] == {"mode": "merge"} and "_mode" not in c["json"]
    assert server.task(tid)["result_path"] == {"k": 1}

    assert await rep._try_report(_entry(tid, "fail_task", {"error_message": "boom"})) == "success"
    assert server.task(tid)["status"] == "failed"


# ───────────────────── fail_task 前置校验 ─────────────────────

async def test_fail_task_is_skipped_when_server_state_moved_on(server, rep):
    """避免过期失败信号覆盖补偿器重投后的工作：服务端已非 running → 丢弃本次 fail。"""
    tid = _running_task(server)
    server.set_status(tid, "queued")
    assert await rep._try_report(_entry(tid, "fail_task", {"error_message": "x"})) == "drop"
    assert not server.calls_to("POST", r"/fail$")


async def test_fail_task_is_dropped_when_server_state_cannot_be_read(server, rep):
    """前置校验读不到服务端状态（5xx / 404）时，按现行实现丢弃本次 fail_task
    （`_safe_to_fail` 返回 False → `_try_report` 返回 drop；其 docstring 写的“让它 retry”与实现不符，
    属原实现的遗留不一致，见 docs/worker.md 已知限制）。"""
    tid = _running_task(server)
    server.inject("GET", rf"/internal/tasks/{tid}$", 503, times=1)
    assert await rep._try_report(_entry(tid, "fail_task", {"error_message": "x"})) == "drop"
    assert await rep._try_report(_entry("nosuchtask", "fail_task", {"error_message": "x"})) == "drop"
    assert not server.calls_to("POST", r"/fail$")


# ───────────────────── 队列行为 / 快照 ─────────────────────

async def test_failed_entries_go_to_retry_pool_and_are_swept(server, rep):
    tid = _running_task(server)
    server.inject("POST", r"/complete$", 503, times=1)
    await rep._handle_entry(_entry(tid, "complete_task"))
    assert len(rep._retry_pool) == 1
    await rep._sweep_retry_pool()
    assert rep._retry_pool == [] and server.task(tid)["status"] == "completed"


async def test_run_loop_consumes_new_entries_and_sweeps_on_timeout(server, rep, monkeypatch):
    import asyncio

    monkeypatch.setattr(PR, "POLL_INTERVAL_SEC", 0.1)
    tid = _running_task(server)
    server.inject("POST", r"/complete$", 503, times=1)
    task = asyncio.create_task(rep.run_loop())
    try:
        await rep.enqueue(_entry(tid, "complete_task"))
        for _ in range(100):
            if server.task(tid)["status"] == "completed":
                break
            await asyncio.sleep(0.05)
        assert server.task(tid)["status"] == "completed", "新事件先试一次，失败后由下一轮扫描重试"
    finally:
        rep.request_stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_snapshot_round_trip_excludes_fail_task(server, rep, tmp_path):
    """退出时 dump 队列 + retry_pool；fail_task 不持久化（避免重启后覆盖补偿器重投的工作）。"""
    a = _entry("ta", "complete_task")
    b = _entry("tb", "fail_task", {"error_message": "x"})
    c = _entry("tc", "update_status", {"status": "running"})
    await rep.enqueue(a)
    rep._retry_pool.extend([b, c])
    assert rep.snapshot_to_file() == 2
    snap = tmp_path / "work" / PR.SNAPSHOT_FILENAME
    lines = [json.loads(ln) for ln in snap.read_text().splitlines()]
    assert {e["task_id"] for e in lines} == {"ta", "tc"}
    assert lines[0]["fencing"] == {"worker_id": "test-host-e2e", "lease_seq": 3}, "令牌必须随快照落盘"

    fresh = PendingReporter(callback=callback_client, work_base_dir=str(tmp_path / "work"))
    assert await fresh.restore_from_snapshot() == 2
    assert not snap.exists(), "恢复后快照文件应删除，避免重复回放"


def _write_fallback(tmp_path):
    d = tmp_path / "work" / "tx"
    d.mkdir(parents=True)
    f = d / "pending_reports.jsonl"
    f.write_text(json.dumps(_entry("tx", "update_step_status", {"sub_step": "s"})) + "\n\nnot-json\n")
    return f


async def test_restore_also_reads_per_task_fallback_files(server, rep, tmp_path):
    """子进程 IPC 不通时降级写的 {task_id}/pending_reports.jsonl，启动恢复时灌回队列。"""
    f = _write_fallback(tmp_path)
    (tmp_path / "work" / PR.SNAPSHOT_FILENAME).write_text("")        # 见下一条：当前实现要求快照文件存在
    assert await rep.restore_from_snapshot() == 1
    assert not f.exists()


async def test_known_limitation_fallback_files_ignored_without_a_snapshot(server, rep, tmp_path):
    """【已知限制·原实现遗留，未修】restore_from_snapshot 在快照文件不存在时直接 return 0，
    根本走不到扫描 per-task 降级文件的那一步 —— 子进程降级写盘的失败事件，只有在恰好
    存在快照时才会在启动恢复里被重放。见 docs/worker.md「已知限制」。"""
    f = _write_fallback(tmp_path)
    assert await rep.restore_from_snapshot() == 0
    assert f.exists()


async def test_enqueue_global_without_reporter_logs_instead_of_raising(caplog):
    PR.set_global_reporter(None)
    await PR.enqueue_global({"task_id": "t", "action": "x"})
    assert "全局实例未初始化" in caplog.text
