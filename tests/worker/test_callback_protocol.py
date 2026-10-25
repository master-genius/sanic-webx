"""callback 客户端对协议的遵守 + 回调重试分层（协议 §7.6）。

重试分层的铁律：**可丢弃的消息不该阻塞计算，必须成功的消息不该只试一次。**

  · 状态迁移（status / complete / fail）：5xx/网络错 → 前台 5 次退避重试 → 仍失败入持久队列；
  · 业务拒绝 400/404：立即出局，不重试、不入队；
  · 401/403：确定性拒绝，前台只试 1 次；
  · 字段更新：首发 + 重试 1 次，不入队（严格 fencing，跨轮重放必被拒，入队只会堆僵尸）；
  · fencing 终态拒绝（not_lessee / gone / already_final）：丢弃而不是入队。
"""

import pytest

from webx_worker import callback as cb_mod
from webx_worker import fencing
from webx_worker.callback import callback_client


@pytest.fixture
def cb(server, harness, monkeypatch):
    """已指向假服务端、退避压到 0 的 callback_client；入队动作被截获。"""
    monkeypatch.setattr(cb_mod, "_CALLBACK_BACKOFF_BASE_SEC", 0.0)
    monkeypatch.setattr(cb_mod, "_FIELDS_RETRY_DELAY_SEC", 0.0)
    enq: list[dict] = []

    async def _fake_enqueue(entry):
        enq.append(entry)

    import types, sys
    stub = types.ModuleType("webx_worker.pending_reporter")
    stub.enqueue_global = _fake_enqueue
    monkeypatch.setitem(sys.modules, "webx_worker.pending_reporter", stub)
    callback_client.enq = enq
    return callback_client


def _claimed(server, task_type="demo"):
    """让假服务端把任务置为 running + 租户为 test-host-e2e，并在本地登记令牌。"""
    tid = server.add_task(task_type)
    t = server.tasks[tid]
    t.update(status="running", worker_id="test-host-e2e", lease_seq=3)
    fencing.register(tid, 3)
    return tid


async def test_update_status_uses_put_and_fencing_headers(cb, server):
    tid = _claimed(server)
    server.set_status(tid, "queued")
    assert await cb.update_status(tid, "running") is True
    c = server.calls_to("PUT", rf"/internal/tasks/{tid}/status$")[0]
    assert c["json"] == {"status": "running"}
    assert c["headers"]["x-worker-id"] == "test-host-e2e" and c["headers"]["x-lease-seq"] == "3"
    assert c["headers"]["x-internal-token"] == server.token


async def test_status_retries_5xx_then_succeeds(cb, server):
    tid = _claimed(server)
    server.set_status(tid, "queued")
    server.inject("PUT", r"/status$", 502, times=2)
    assert await cb.update_status(tid, "running") is True
    assert len(server.calls_to("PUT", r"/status$")) == 3
    assert cb.enq == []


async def test_status_exhausted_retries_are_enqueued_for_durable_replay(cb, server):
    tid = _claimed(server)
    server.inject("PUT", r"/status$", 503, times=99)
    assert await cb.update_status(tid, "running") is False
    assert len(server.calls_to("PUT", r"/status$")) == 5          # 前台 5 次
    assert len(cb.enq) == 1
    e = cb.enq[0]
    assert e["action"] == "update_status" and e["payload"] == {"status": "running"}
    # 令牌快照进条目：重放发生在租约注销之后，只能靠条目自己记得「哪一次运行产生的」
    assert e["fencing"] == {"worker_id": "test-host-e2e", "lease_seq": 3}


@pytest.mark.parametrize("code", [400, 404])
async def test_business_rejects_are_neither_retried_nor_enqueued(cb, server, code):
    tid = _claimed(server)
    server.inject("POST", r"/complete$", code, times=99)
    assert await cb.complete_task(tid) is False
    assert len(server.calls_to("POST", r"/complete$")) == 1
    assert cb.enq == []


async def test_401_is_tried_once_then_enqueued(cb, server):
    """确定性拒绝：前台只试 1 次（重试不会改善），入队交给持久队列（运维修好令牌后自愈）。"""
    tid = _claimed(server)
    server.inject("POST", r"/complete$", 401, times=99, body={"error": "unauthorized"})
    assert await cb.complete_task(tid) is False
    assert len(server.calls_to("POST", r"/complete$")) == 1
    assert len(cb.enq) == 1


@pytest.mark.parametrize("reason", ["not_lessee", "gone", "already_final"])
async def test_fencing_terminal_rejects_are_dropped_not_enqueued(cb, server, reason):
    """403 + 终态 reason：再试一万次也是同一个答案，丢弃，别污染「队列长度即待送达数」。"""
    tid = _claimed(server)
    server.inject("POST", r"/complete$", 403, times=99, body={"ok": False, "reason": reason})
    assert await cb.complete_task(tid) is False
    assert len(server.calls_to("POST", r"/complete$")) == 1
    assert cb.enq == []


async def test_real_fencing_rejection_from_server_when_token_is_stale(cb, server):
    """不靠注入：令牌与服务端不一致时，真实的 403 not_lessee 同样被丢弃。"""
    tid = _claimed(server)
    server.tasks[tid]["lease_seq"] = 4           # 服务端已换代
    assert await cb.complete_task(tid) is False
    assert cb.enq == []
    assert server.calls_to("POST", r"/complete$")[0]["status"] == 403


async def test_stale_fence_latch_does_not_swallow_the_next_round(cb, server):
    """★ 回归：上一轮 ack 被拒留下的闩锁，绝不能吞掉**下一轮**的合法上报（闩锁键带 lease_seq）。"""
    tid = _claimed(server)
    server.tasks[tid]["lease_seq"] = 9           # 让 ack 被判 not_lessee
    assert await cb.ack_lease(tid) is False
    assert not cb._fence_rejected, "ack 是唯一置闩锁不入队的出口，必须自己清掉"

    # 手工复现「闩锁残留」：直接模拟旧实现留下的陈旧闩锁
    cb._fence_rejected[(tid, 3)] = "not_lessee"
    fencing.register(tid, 4)                     # 第 2 轮：代次已变
    server.tasks[tid]["lease_seq"] = 4
    server.inject("POST", r"/complete$", 503, times=99)
    assert await cb.complete_task(tid) is False
    assert len(cb.enq) == 1, "第 2 轮的上报被上一轮的陈旧闩锁吞掉了"
    assert cb.enq[0]["fencing"]["lease_seq"] == 4


async def test_fields_patch_merge_and_retry_exactly_once(cb, server):
    tid = _claimed(server)
    server.inject("PATCH", r"/fields$", 502, times=1)
    assert await cb.update_task_fields(tid, log_path='{"s1": "u1"}', mode="merge") is True
    calls = server.calls_to("PATCH", r"/fields$")
    assert len(calls) == 2 and calls[0]["query"] == {"mode": "merge"}
    assert server.task(tid)["log_path"] == {"s1": "u1"}

    server.calls.clear()
    server.inject("PATCH", r"/fields$", 502, times=99)
    assert await cb.update_task_fields(tid, log_path="{}") is False
    assert len(server.calls_to("PATCH", r"/fields$")) == 2, "首发 + 重试 1 次，然后认了"
    assert cb.enq == [], "字段更新刻意不进持久队列"


@pytest.mark.parametrize("code", [400, 404, 401, 403])
async def test_fields_deterministic_rejects_are_not_retried(cb, server, code):
    tid = _claimed(server)
    server.inject("PATCH", r"/fields$", code, times=99, body={"ok": False})
    assert await cb.update_task_fields(tid, log_path="{}") is False
    assert len(server.calls_to("PATCH", r"/fields$")) == 1


async def test_fields_default_mode_has_no_query(cb, server):
    tid = _claimed(server)
    await cb.update_task_fields(tid, result_path='{"a": 1}', mode="overwrite")
    assert server.calls_to("PATCH", r"/fields$")[0]["query"] == {}


async def test_step_status_uses_patch_steps(cb, server):
    tid = _claimed(server)
    assert await cb.update_step_status(tid, steps=["a"], sub_step="b", sub_status="running",
                                       step_message="hi") is True
    c = server.calls_to("PATCH", r"/steps$")[0]
    assert c["json"] == {"steps": ["a"], "sub_step": "b", "sub_status": "running", "step_message": "hi"}


async def test_release_lease_is_delete_without_body_and_returns_task_to_pool(cb, server):
    tid = _claimed(server)
    assert await cb.release_lease(tid) is True
    c = server.calls_to("DELETE", rf"/internal/tasks/{tid}/lease$")[0]
    assert c["size"] == 0, "DELETE 不能带请求体（网关会丢）"
    assert c["headers"]["x-worker-id"] == "test-host-e2e" and c["headers"]["x-lease-seq"] == "3"
    t = server.task(tid)
    assert t["status"] == "pending" and t["worker_id"] == "" and t["lease_seq"] == 4


async def test_release_lease_with_explicit_headers_works_after_registry_is_cleared(cb, server):
    """停机路径：注册表已被在跑任务的 finally 清空，靠预先捕获的令牌仍能归还。"""
    tid = _claimed(server)
    tokens = {tid: fencing.headers_for(tid)}
    fencing.unregister(tid)
    assert await cb.release_lease(tid) is False                   # 空手 → missing_fencing_token
    assert server.calls_to("DELETE", r"/lease$")[0]["status"] == 403
    assert await cb.release_lease(tid, extra_headers=tokens[tid]) is True


async def test_release_lease_rejection_is_normal_not_an_error(cb, server):
    tid = _claimed(server)
    server.tasks[tid]["lease_seq"] = 8                            # 已被别人接管
    assert await cb.release_lease(tid) is False
    assert server.task(tid)["status"] == "running"                # 什么都没动


async def test_heartbeat_post_heartbeats_and_revoked_is_a_200(cb, server):
    tid = _claimed(server)
    r = await cb.update_heartbeat(tid)
    assert r.is_ok and r.heartbeat_status == "running" and r.data["lease_until"] > 0
    assert server.calls_to("POST", r"/heartbeats$")[0]["status"] == 200

    server.revoke_lease(tid)
    r = await cb.update_heartbeat(tid)
    assert r.is_ok and r.heartbeat_status == "revoked", "撤销回 200，worker 靠 status != running 取消"


@pytest.mark.parametrize("wrap", [False, True])
async def test_get_task_status_accepts_both_response_shapes(server, harness, wrap):
    """协议 §4.10 没写清响应是否包在 data 里 —— 两种都得认。"""
    server.wrap_task_info_in_data = wrap
    tid = _claimed(server)
    r = await callback_client.get_task_status(tid)
    assert r.is_ok and r.task_status == "running" and r.data["lease_seq"] == 3
    assert (await callback_client.get_task_status("nosuchtask")).is_not_found


async def test_upload_result_file_goes_to_storage_with_201_and_normalizes_url(
    cb, server, monkeypatch,
):
    from webx_worker.config import worker_settings

    tid = _claimed(server)
    monkeypatch.setattr(worker_settings, "STORAGE_URL", "https://files.example.com/")
    monkeypatch.setattr(worker_settings, "STORAGE_INTERNAL_URL", server.url)
    url = await cb.upload_result_file(tid, "run.log", b"hello")
    assert url == f"https://files.example.com/public/tasks/{tid}/results/run.log"
    assert server.storage[(tid, "run.log")] == b"hello"
    c = server.calls_to("POST", rf"/internal/tasks/{tid}/results$")[0]
    assert c["status"] == 201
    assert "x-worker-id" not in c["headers"], "存储服务不做 fencing，不该带租户信息"


async def test_upload_result_file_uses_storage_token_when_configured(cb, server, monkeypatch):
    from webx_worker.config import worker_settings

    tid = _claimed(server)
    server.storage_token = "storage-secret"
    monkeypatch.setattr(worker_settings, "STORAGE_INTERNAL_TOKEN", "storage-secret")
    assert await cb.upload_result_file(tid, "a.log", b"x")
    server.storage_token = "something-else"
    assert await cb.upload_result_file(tid, "b.log", b"x") is None     # 401 → 失败


async def test_upload_without_any_storage_url_fails_loudly_not_silently_to_web(
    cb, server, monkeypatch, caplog,
):
    """没有回退路径：任务服务上没有产物端点，回退过去只会 404。"""
    from webx_worker.config import worker_settings

    monkeypatch.setattr(worker_settings, "STORAGE_URL", "")
    monkeypatch.setattr(worker_settings, "STORAGE_INTERNAL_URL", "")
    assert await cb.upload_result_file("t1", "a.log", b"x") is None
    assert "未配置存储服务地址" in caplog.text
    assert not server.calls_to("POST", r"/results$")
