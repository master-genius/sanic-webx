"""worker 回调接口 ``/internal/tasks/...``：端到端，真实 PostgreSQL。

按 ``docs/worker-protocol.md`` §4 逐条验证。认领这一步用服务层的 ``claim_one``（真实 SQL），
其余全部走 HTTP，断言里同时看响应与库。
"""

import json

import pytest

from tests.pg_helpers import execute, fetch, orm
from webx.lib.task_types import TaskStageConfig, TaskTypeSpec, register_task_type, unregister_task_type

pytestmark = pytest.mark.usefixtures("use_pg")

TOKEN = "it-token"


@pytest.fixture(autouse=True)
def internal_token(monkeypatch):
    from webx.config import get_settings

    monkeypatch.setattr(get_settings(), "INTERNAL_TOKEN", TOKEN)


@pytest.fixture
def client(use_pg):
    return use_pg


@pytest.fixture
def dsn(pg_dsn):
    return pg_dsn


@pytest.fixture
def types():
    added = []

    def add(spec):
        register_task_type(spec)
        added.append(spec.name)

    yield add
    for n in added:
        unregister_task_type(n)


def user_h(uid="u1"):
    from webx.lib.token import get_token

    return {"Authorization": "Bearer " + get_token().make_token({"user_id": uid, "username": uid, "role": "user"}, id_="user")}


def wk(worker="w1", seq=None, token=TOKEN):
    h = {"X-Internal-Token": token, "X-Worker-Id": worker}
    if seq is not None:
        h["X-Lease-Seq"] = str(seq)
    return h


def create_submit(client, uid="u1", task_type="demo", **body):
    _, r = client.post("/api/tasks", json={"task_type": task_type, "submit": True, **body}, headers=user_h(uid))
    assert r.status == 201, r.text
    return r.json["data"]["id"]


def claim(dsn, worker="w1", types=("demo",)):
    from webx.services.claim import claim_one

    async def go():
        return await claim_one(worker, list(types))

    row = orm(dsn, go)
    assert row, "池里应当有任务可领"
    return row["id"], row["lease_seq"]


def db(dsn, tid, cols="status, sub_status, sub_step, steps, worker_id, lease_seq, lease_until, result_path, log_path, error_message, requeues"):
    return dict(fetch(dsn, f"SELECT {cols} FROM tasks WHERE id=$1", tid)[0])


def running(client, dsn, **kw):
    """提交 → 认领 → ack → 上报 running，返回 (task_id, 令牌头)。"""
    tid = create_submit(client, **kw)
    _, seq = claim(dsn)
    h = wk(seq=seq)
    assert client.post(f"/internal/tasks/{tid}/ack", headers=h)[1].status == 200
    assert client.put(f"/internal/tasks/{tid}/status", json={"status": "running"}, headers=h)[1].status == 200
    return tid, h


MULTI = TaskStageConfig(end_step="encode", step_transitions={"": {"prepare"}, "prepare": {"encode"}})


# ==================== 鉴权 ====================


@pytest.mark.parametrize("hdr", [{}, {"X-Internal-Token": "wrong"}, {"X-Internal-Token": ""}])
def test_every_internal_route_requires_the_token(client, hdr):
    for method, path in [("get", "/internal/tasks/x"), ("post", "/internal/tasks/x/ack"),
                         ("post", "/internal/tasks/x/heartbeats"), ("put", "/internal/tasks/x/status"),
                         ("post", "/internal/tasks/x/complete"), ("delete", "/internal/tasks/x/lease"),
                         ("patch", "/internal/tasks/x/fields"), ("post", "/internal/tasks/x/deliveries"),
                         ("get", "/internal/tasks/x/files")]:
        assert getattr(client, method)(path, headers=hdr)[1].status == 401, (method, path)


def test_an_unconfigured_token_rejects_everything(client, monkeypatch):
    from webx.config import get_settings

    monkeypatch.setattr(get_settings(), "INTERNAL_TOKEN", "")
    assert client.get("/internal/tasks/x", headers={"X-Internal-Token": ""})[1].status == 401, "空配置绝不等价于放行"
    assert client.get("/internal/tasks/x", headers={"X-Internal-Token": "anything"})[1].status == 401


def test_a_user_token_is_not_an_internal_token(client):
    assert client.get("/internal/tasks/x", headers=user_h())[1].status == 401


# ==================== 完整生命周期 ====================


def test_full_lifecycle_submit_claim_ack_run_report_complete(client, dsn):
    tid = create_submit(client, name="job", params_text={"k": 1})
    claimed_id, seq = claim(dsn)
    assert claimed_id == tid and seq == 2, "提交进池 bump 了一次，认领再 bump 一次"
    h = wk(seq=seq)

    ack = client.post(f"/internal/tasks/{tid}/ack", headers=h)[1]
    assert ack.status == 200 and ack.json["ok"] and ack.json["lease_until"] > 0
    assert db(dsn, tid)["status"] == "queued"

    assert client.put(f"/internal/tasks/{tid}/status", json={"status": "running"}, headers=h)[1].status == 200
    d = db(dsn, tid, "status, start_time")
    assert d["status"] == "running" and d["start_time"] is not None, "start_time 由服务端写，不接受 worker 传值"

    hb = client.post(f"/internal/tasks/{tid}/heartbeats", headers=h)[1]
    assert hb.status == 200 and hb.json["ok"] and hb.json["status"] == "running" and hb.json["lease_until"] > 0

    merged = client.patch(f"/internal/tasks/{tid}/fields?mode=merge", json={"result_path": {"a": {"url": "/x"}}}, headers=h)[1]
    assert merged.status == 200
    client.patch(f"/internal/tasks/{tid}/fields?mode=merge", json={"result_path": {"a": {"size": 3}, "b": 1}}, headers=h)
    assert json.loads(db(dsn, tid)["result_path"]) == {"a": {"url": "/x", "size": 3}, "b": 1}, "merge 是深度合并"

    assert client.post(f"/internal/tasks/{tid}/complete", headers=h)[1].status == 200
    d = db(dsn, tid)
    assert (d["status"], d["sub_status"], d["lease_until"], d["worker_id"]) == ("completed", "success", None, "w1"), \
        "终态释放租约，worker_id 保留作排查线索"

    user_view = client.get(f"/api/tasks/{tid}", headers=user_h())[1].json["data"]
    assert user_view["status"] == "completed" and json.loads(user_view["result_path"])["b"] == 1


def test_the_lease_response_columns_match_the_protocol(client, dsn):
    from webx.services.claim import RETURN_COLS, claim_one

    create_submit(client, params_text={"p": 1}, files=[{"name": "in.bin"}])

    async def go():
        return await claim_one("w1", ["demo"])

    row = orm(dsn, go)
    assert set(row) == {c.strip() for c in RETURN_COLS.split(",")}
    assert json.loads(row["params_text"]) == {"p": 1} and json.loads(row["files_json"])[0]["name"] == "in.bin"


def test_task_info_for_startup_recovery(client, dsn):
    tid, h = running(client, dsn)
    info = client.get(f"/internal/tasks/{tid}", headers=wk())[1].json
    assert info["id"] == tid and info["status"] == "running" and info["worker_id"] == "w1" and info["lease_seq"] == 2
    assert client.get("/internal/tasks/nope", headers=wk())[1].status == 404
    client.post(f"/internal/tasks/{tid}/complete", headers=h)
    execute(dsn, "UPDATE tasks SET is_delete=true WHERE id=$1", tid)
    gone = client.get(f"/internal/tasks/{tid}", headers=wk())[1]
    assert gone.status == 404 and gone.json["reason"] == "gone", "已软删的不返回，否则上传进程会白传已删任务的产物"


# ==================== fencing ====================


@pytest.mark.parametrize(
    "method, path, body",
    [("put", "status", {"status": "running"}), ("post", "complete", None), ("post", "fail", {"error_message": "x"}),
     ("patch", "steps", {"sub_status": "running"}), ("patch", "fields", {"log_path": "x"})],
)
def test_writes_by_a_stale_lessee_are_rejected_without_touching_the_row(client, dsn, method, path, body):
    tid, h = running(client, dsn)
    before = db(dsn, tid)
    stale = wk(seq=h["X-Lease-Seq"] and int(h["X-Lease-Seq"]) - 1)
    kw = {"json": body} if body is not None else {}
    r = getattr(client, method)(f"/internal/tasks/{tid}/{path}", headers=stale, **kw)[1]
    assert r.status == 403 and r.json == {"ok": False, "reason": "not_lessee"}
    other = getattr(client, method)(f"/internal/tasks/{tid}/{path}", headers=wk("w2", int(h["X-Lease-Seq"])), **kw)[1]
    assert other.status == 403, "别的机器"
    assert db(dsn, tid) == before


def test_missing_and_malformed_tokens_are_rejected_loudly(client, dsn):
    tid, h = running(client, dsn)
    no_token = client.post(f"/internal/tasks/{tid}/complete", headers={"X-Internal-Token": TOKEN})[1]
    assert no_token.status == 403 and no_token.json["reason"] == "not_lessee"
    bad_seq = client.post(f"/internal/tasks/{tid}/complete", headers={**wk(), "X-Lease-Seq": "abc"})[1]
    assert bad_seq.status == 403 and bad_seq.json["reason"] == "bad_lease_seq", "带了 worker_id 却没带有效 lease_seq：响亮拒绝"
    assert db(dsn, tid)["status"] == "running"


def test_the_fencing_token_may_travel_in_the_body(client, dsn):
    """第二通道：令牌来自持久化条目快照的异步路径（上传进程、失败上报重放）。"""
    tid, h = running(client, dsn)
    r = client.post(f"/internal/tasks/{tid}/fail", headers={"X-Internal-Token": TOKEN},
                    json={"error_message": "from replay", "worker_id": "w1", "lease_seq": int(h["X-Lease-Seq"])})[1]
    assert r.status == 200 and db(dsn, tid)["status"] == "failed"


def test_a_reclaimed_task_rejects_the_old_lessee_and_accepts_the_new_one(client, dsn):
    from webx.services.lease import reclaim_expired

    tid, old = running(client, dsn)
    execute(dsn, "UPDATE tasks SET lease_until = (EXTRACT(EPOCH FROM now())*1000)::bigint - 1000 WHERE id=$1", tid)

    async def go():
        return await reclaim_expired()

    assert [r["id"] for r in orm(dsn, go)] == [tid]
    assert client.post(f"/internal/tasks/{tid}/complete", headers=old)[1].json["reason"] == "not_lessee", \
        "回池后旧租户的在途回调必须被挡住——这是 fencing 的全部意义"

    _, seq = claim(dsn, worker="w2")
    new = wk("w2", seq)
    assert client.post(f"/internal/tasks/{tid}/ack", headers=new)[1].status == 200
    assert client.post(f"/internal/tasks/{tid}/ack", headers=old)[1].status == 403


def test_a_repeated_callback_after_finishing_is_reported_as_already_final(client, dsn):
    tid, h = running(client, dsn)
    client.post(f"/internal/tasks/{tid}/complete", headers=h)
    execute(dsn, "UPDATE tasks SET lease_seq = lease_seq + 5 WHERE id=$1", tid)  # 令牌过期，但同一台机器、已终态
    r = client.post(f"/internal/tasks/{tid}/complete", headers=h)[1]
    assert r.status == 403 and r.json["reason"] == "already_final", "这是幂等成功，调用方应视为成功后丢弃，不要告警"


def test_writes_to_a_deleted_or_missing_task_say_gone(client, dsn):
    tid, h = running(client, dsn)
    execute(dsn, "UPDATE tasks SET is_delete=true WHERE id=$1", tid)
    assert client.post(f"/internal/tasks/{tid}/complete", headers=h)[1].json["reason"] == "gone"
    assert client.post("/internal/tasks/nope/complete", headers=h)[1].json["reason"] == "gone"


# ==================== ack / 心跳 / 归还 ====================


def test_ack_failure_modes(client, dsn):
    tid = create_submit(client)
    _, seq = claim(dsn)
    assert client.post(f"/internal/tasks/{tid}/ack", headers={"X-Internal-Token": TOKEN})[1].json["reason"] == "not_lessee", "无令牌"
    assert client.post(f"/internal/tasks/{tid}/ack", headers=wk())[1].json["reason"] == "bad_lease_seq", "只带 worker_id：响亮拒绝"
    r = client.post(f"/internal/tasks/{tid}/ack", headers={**wk(), "X-Lease-Seq": "x"})[1]
    assert r.status == 403 and r.json["reason"] == "bad_lease_seq"
    assert client.post(f"/internal/tasks/{tid}/ack", headers=wk(seq=seq + 3))[1].status == 403
    assert client.post(f"/internal/tasks/{tid}/ack", headers=wk(seq=seq))[1].status == 200


def test_ack_extends_the_short_claim_lease_to_the_regular_one(client, dsn):
    tid = create_submit(client)
    _, seq = claim(dsn)
    short = db(dsn, tid)["lease_until"]
    client.post(f"/internal/tasks/{tid}/ack", headers=wk(seq=seq))
    assert db(dsn, tid)["lease_until"] > short + 100_000, "短租约(60s) → 常规租约(100s) + grace(≥180s)"


def test_heartbeat_revocation_is_a_200_so_the_worker_cancels_itself(client, dsn):
    tid, h = running(client, dsn)
    revoked = client.post(f"/internal/tasks/{tid}/heartbeats", headers=wk(seq=int(h["X-Lease-Seq"]) + 1))[1]
    assert revoked.status == 200, "不是 403：worker 的取消判据是 status != running，无需额外分支"
    assert revoked.json == {"ok": False, "status": "revoked", "reason": "not_lessee"}
    assert client.post(f"/internal/tasks/{tid}/heartbeats", headers={"X-Internal-Token": TOKEN})[1].json["status"] == "revoked"
    gone = client.post("/internal/tasks/nope/heartbeats", headers=h)[1]
    assert gone.status == 404 and gone.json["reason"] == "gone", "任务不存在：worker 视为 server-gone，终止"


def test_heartbeat_after_the_user_cancelled_tells_the_worker_the_new_status(client, dsn):
    tid, h = running(client, dsn)
    assert client.post(f"/api/tasks/{tid}/cancel", headers=user_h())[1].status == 200
    hb = client.post(f"/internal/tasks/{tid}/heartbeats", headers=h)[1]
    assert hb.status == 200 and hb.json["status"] == "cancelled", "worker 据此终止子进程"


def test_release_returns_the_task_to_the_pool_without_charging_requeues(client, dsn):
    tid, h = running(client, dsn)
    before = db(dsn, tid)
    assert client.delete(f"/internal/tasks/{tid}/lease", headers=h)[1].json == {"ok": True}
    d = db(dsn, tid)
    assert (d["status"], d["worker_id"], d["lease_until"], d["requeues"]) == ("pending", "", None, 0)
    assert d["lease_seq"] == before["lease_seq"] + 1
    again = client.delete(f"/internal/tasks/{tid}/lease", headers=h)[1]
    assert again.status == 403 and again.json["reason"] == "not_lessee", "二次归还不命中——已不是租户时什么都不做才是对的"


def test_release_without_a_fencing_token_is_refused(client, dsn):
    tid, h = running(client, dsn)
    r = client.delete(f"/internal/tasks/{tid}/lease", headers={"X-Internal-Token": TOKEN})[1]
    assert r.status == 403 and r.json["reason"] == "missing_fencing_token"
    assert db(dsn, tid)["status"] == "running", "无条件归还会退掉别人的租约"


# ==================== 状态 / 完成 / 失败 ====================


def test_status_validation(client, dsn):
    tid = create_submit(client)
    _, seq = claim(dsn)
    h = wk(seq=seq)
    assert client.put(f"/internal/tasks/{tid}/status", json={"status": "bogus"}, headers=h)[1].status == 400
    assert client.put(f"/internal/tasks/{tid}/status", json={"status": "completed"}, headers=h)[1].status == 400, "queued → completed 非法"
    assert db(dsn, tid)["status"] == "queued"


def test_fail_records_the_error_and_keeps_the_worker_for_forensics(client, dsn):
    tid, h = running(client, dsn)
    assert client.post(f"/internal/tasks/{tid}/fail", json={"error_message": "boom"}, headers=h)[1].status == 200
    d = db(dsn, tid)
    assert (d["status"], d["sub_status"], d["error_message"], d["lease_until"], d["worker_id"]) == \
        ("failed", "failed", "boom", None, "w1")
    # 同一租户对已失败任务重复上报是幂等的（接受 RUNNING / FAILED 两态，保证错误信息必达），状态不变
    assert client.post(f"/internal/tasks/{tid}/fail", json={"error_message": "boom"}, headers=h)[1].status == 200
    assert db(dsn, tid)["status"] == "failed"
    # 而别的机器 / 旧令牌不行
    assert client.post(f"/internal/tasks/{tid}/fail", json={}, headers=wk("w2", int(h["X-Lease-Seq"])))[1].status == 403


def test_completion_events_are_emitted_best_effort(client, dsn):
    from webx.lib.task_events import off_task_event, on_task_event

    seen = []

    async def handler(task_id, event, payload):
        seen.append((task_id, event, payload))

    async def broken(task_id, event, payload):
        raise RuntimeError("subscriber bug")

    on_task_event(broken)
    on_task_event(handler)
    try:
        tid, h = running(client, dsn)
        assert client.post(f"/internal/tasks/{tid}/complete", headers=h)[1].status == 200, "订阅方的故障不得影响任务迁移"
    finally:
        off_task_event(broken)
        off_task_event(handler)
    assert (tid, "completed", {}) in seen


# ==================== 多阶段 ====================


def test_multistage_flow_waits_for_the_user_between_steps(client, dsn, types):
    types(TaskTypeSpec(name="staged", stage=MULTI))
    tid = create_submit(client, task_type="staged", params_text={"prepare": {"n": 1}})
    _, seq = claim(dsn, types=("staged",))
    h = wk(seq=seq)
    client.post(f"/internal/tasks/{tid}/ack", headers=h)
    client.put(f"/internal/tasks/{tid}/status", json={"status": "running"}, headers=h)
    assert db(dsn, tid)["sub_step"] == "prepare", "首次提交推导起始步骤"

    steps = client.patch(f"/internal/tasks/{tid}/steps", json={"sub_step": "prepare", "sub_status": "running", "step_message": "50%"}, headers=h)[1]
    assert steps.status == 200 and steps.json["sub_status"] == "running"
    client.patch(f"/internal/tasks/{tid}/steps", json={"steps": ["prepare"], "sub_step": "prepare", "sub_status": "success"}, headers=h)

    client.post(f"/internal/tasks/{tid}/complete", headers=h)
    d = db(dsn, tid)
    assert (d["status"], d["sub_status"], d["sub_step"], d["lease_until"]) == ("running", "success", "", None), \
        "中间步完成：停在 running + success 等用户；必须释放租约，否则回收循环会把它当成 worker 失联"

    # 回收循环不得动"等用户"的任务，即使租约字段是过期值
    execute(dsn, "UPDATE tasks SET lease_until = 1 WHERE id=$1", tid)
    from webx.services.lease import reclaim_expired

    async def go():
        return await reclaim_expired()

    # lease_until=1 的 running 任务会被回收——所以完成中间步时置 NULL 才是正确的（上面已断言）
    execute(dsn, "UPDATE tasks SET lease_until = NULL WHERE id=$1", tid)
    assert orm(dsn, go) == []

    # 用户推进下一步
    r = client.patch(f"/api/tasks/{tid}", json={"step": "encode", "params_text": {"encode": {"q": 2}}}, headers=user_h())[1]
    assert r.status == 200
    d = db(dsn, tid, "status, sub_step, sub_status, lease_seq, worker_id, requeues")
    assert (d["status"], d["sub_step"], d["sub_status"], d["worker_id"], d["requeues"]) == ("pending", "encode", "pending", "", 0)
    assert d["lease_seq"] > seq, "推进下一步必须 bump，上一步 worker 的在途回调才不会写进这一轮"

    _, seq2 = claim(dsn, types=("staged",))
    h2 = wk(seq=seq2)
    client.post(f"/internal/tasks/{tid}/ack", headers=h2)
    client.put(f"/internal/tasks/{tid}/status", json={"status": "running"}, headers=h2)  # pending→queued→running
    client.patch(f"/internal/tasks/{tid}/steps", json={"steps": ["prepare", "encode"], "sub_status": "success"}, headers=h2)
    client.post(f"/internal/tasks/{tid}/complete", headers=h2)
    assert db(dsn, tid)["status"] == "completed", "到了 end_step 才是终态"


def test_step_updates_are_dropped_once_the_user_cancelled(client, dsn):
    tid, h = running(client, dsn)
    client.post(f"/api/tasks/{tid}/cancel", headers=user_h())
    r = client.patch(f"/internal/tasks/{tid}/steps", json={"steps": ["x"], "sub_status": "success"}, headers=h)[1]
    assert r.status in (400, 403)  # 状态已非 RUNNING：fencing 或状态守卫拒绝
    d = db(dsn, tid)
    assert d["status"] == "cancelled" and json.loads(d["steps"]) == [], "stale 回调不得污染已取消的任务"


def test_step_message_guard_rejects_late_messages_from_the_previous_step(client, dsn, types):
    types(TaskTypeSpec(name="staged", stage=MULTI))
    tid = create_submit(client, task_type="staged")
    _, seq = claim(dsn, types=("staged",))
    h = wk(seq=seq)
    client.post(f"/internal/tasks/{tid}/ack", headers=h)
    client.put(f"/internal/tasks/{tid}/status", json={"status": "running"}, headers=h)
    r = client.patch(f"/internal/tasks/{tid}/steps", json={"step_message": "late"}, headers=h)[1]
    assert r.status == 400, "写 step_message 必须同时带 sub_step"
    r = client.patch(f"/internal/tasks/{tid}/steps", json={"sub_step": "wrong", "step_message": "late"}, headers=h)[1]
    assert r.status == 400 and db(dsn, tid, "step_message")["step_message"] == ""


# ==================== 字段 / 交付 ====================


def test_fields_overwrite_vs_merge_and_validation(client, dsn):
    tid, h = running(client, dsn)
    client.patch(f"/internal/tasks/{tid}/fields", json={"log_path": "plain-log"}, headers=h)
    assert db(dsn, tid)["log_path"] == "plain-log"
    client.patch(f"/internal/tasks/{tid}/fields?mode=merge", json={"log_path": {"a": 1}}, headers=h)
    assert json.loads(db(dsn, tid)["log_path"]) == {"default": "plain-log", "a": 1}, "非结构化旧值包成 default 再合并"
    assert client.patch(f"/internal/tasks/{tid}/fields?mode=bogus", json={"log_path": "x"}, headers=h)[1].status == 400
    empty = client.patch(f"/internal/tasks/{tid}/fields", json={}, headers=h)[1]
    assert empty.status == 200 and "无字段" in empty.json["message"]


def test_deliveries_skip_fencing_on_purpose(client, dsn):
    """上传进程是长驻的，手里永远没有 per-task 的租约令牌；多阶段推进后 lease_seq 早已变了，产物仍要交付。"""
    tid, h = running(client, dsn)
    client.post(f"/internal/tasks/{tid}/complete", headers=h)
    execute(dsn, "UPDATE tasks SET lease_seq = lease_seq + 9, worker_id='someone-else' WHERE id=$1", tid)

    r = client.post(f"/internal/tasks/{tid}/deliveries", json={"result_path": {"nvt_2": {"xtc_url": "/public/x.xtc"}}},
                    headers={"X-Internal-Token": TOKEN})[1]
    assert r.status == 200
    again = client.post(f"/internal/tasks/{tid}/deliveries", json={"result_path": {"nvt_2": {"size": 1}}},
                        headers={"X-Internal-Token": TOKEN})[1]
    assert json.loads(again.json["result_path"])["nvt_2"] == {"xtc_url": "/public/x.xtc", "size": 1}, "幂等 merge"


def test_deliveries_say_404_for_missing_or_deleted_tasks_so_the_uploader_can_drop_the_entry(client, dsn):
    tid, h = running(client, dsn)
    execute(dsn, "UPDATE tasks SET is_delete=true WHERE id=$1", tid)
    hdr = {"X-Internal-Token": TOKEN}
    assert client.post(f"/internal/tasks/{tid}/deliveries", json={"result_path": {"a": 1}}, headers=hdr)[1].status == 404
    assert client.post("/internal/tasks/nope/deliveries", json={"result_path": {"a": 1}}, headers=hdr)[1].status == 404


def test_deliveries_never_touch_log_path(client, dsn):
    tid, h = running(client, dsn)
    client.post(f"/internal/tasks/{tid}/deliveries", json={"result_path": {"a": 1}, "log_path": "evil"}, headers={"X-Internal-Token": TOKEN})
    assert db(dsn, tid)["log_path"] == "", "log_path 被上一轮覆盖正是要防的事，那条路走严格 fencing"


# ==================== 输入文件 ====================


def test_workers_download_the_task_input_files(client, dsn):
    _, r = client.post("/api/tasks", json={"task_type": "demo"}, headers=user_h())
    tid = r.json["data"]["id"]
    b = "----w"
    body = (f'--{b}\r\nContent-Disposition: form-data; name="file"; filename="in.bin"\r\n\r\n').encode() + b"payload" + f"\r\n--{b}--\r\n".encode()
    client.post(f"/api/tasks/{tid}/files", data=body, headers={**user_h(), "Content-Type": f"multipart/form-data; boundary={b}"})

    listing = client.get(f"/internal/tasks/{tid}/files", headers=wk())[1].json
    assert listing["files"] == [{"name": "in.bin", "size": 7}]
    got = client.get(f"/internal/tasks/{tid}/files/in.bin", headers=wk())[1]
    assert got.status == 200 and got.body == b"payload"
    assert client.get(f"/internal/tasks/{tid}/files/nope.bin", headers=wk())[1].status == 404
    assert client.get(f"/internal/tasks/{tid}/files/..%2F..%2Fetc%2Fpasswd", headers=wk())[1].status == 404, "路径穿越走存储层守卫"
