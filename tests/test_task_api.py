"""用户侧任务接口 ``/api/tasks``：端到端，真实 PostgreSQL。

提交会走 ``enter_pending_pool``（PG 专有 SQL），所以这组用例连真实 PG（夹具 ``use_pg``）。
断言既看 HTTP 响应，也直接看库里的字段——租约不变量、lease_seq 的 bump 这些，响应里是看不到的。
"""

import json

import pytest

from tests.pg_helpers import execute, fetch, orm
from webx.lib.task_types import TaskStageConfig, TaskTypeSpec, register_task_type, unregister_task_type

pytestmark = pytest.mark.usefixtures("use_pg")


def auth(uid="u1", **claims):
    from webx.lib.token import get_token

    payload = {"user_id": uid, "username": uid, "role": "user", **claims}
    return {"Authorization": f"Bearer {get_token().make_token(payload, id_='user')}"}


@pytest.fixture
def client(use_pg):
    return use_pg


@pytest.fixture
def types():
    added = []

    def add(spec):
        register_task_type(spec)
        added.append(spec.name)

    yield add
    for n in added:
        unregister_task_type(n)


def row(dsn_fixture_free, task_id, *cols):
    raise NotImplementedError


@pytest.fixture
def dsn(pg_dsn):
    return pg_dsn


def db(dsn, task_id, cols="status, retry, lease_seq, worker_id, sub_status, sub_step, steps, params_text"):
    return dict(fetch(dsn, f"SELECT {cols} FROM tasks WHERE id=$1", task_id)[0])


def create(client, uid="u1", **body):
    body.setdefault("task_type", "demo")
    _, r = client.post("/api/tasks", json=body, headers=auth(uid))
    assert r.status == 201, r.text
    return r.json["data"]


MULTI = TaskStageConfig(
    end_step="encode",
    step_transitions={"": {"prepare"}, "prepare": {"render", "encode"}, "render": {"render", "encode"}},
    max_steps=6,
)


# ==================== 认证 / 归属 ====================


def test_every_endpoint_requires_login(client):
    for method, path in [("get", "/api/tasks"), ("post", "/api/tasks"), ("get", "/api/tasks/x"),
                         ("delete", "/api/tasks/x"), ("post", "/api/tasks/x/cancel")]:
        assert getattr(client, method)(path)[1].status == 401, (method, path)


def test_users_cannot_see_or_touch_each_others_tasks(client, dsn):
    mine = create(client, "alice", name="mine")
    h = auth("bob")
    for method, path, body in [
        ("get", f"/api/tasks/{mine['id']}", None),
        ("put", f"/api/tasks/{mine['id']}/info", {"name": "x"}),
        ("patch", f"/api/tasks/{mine['id']}", {"params_text": {}}),
        ("post", f"/api/tasks/{mine['id']}/submit", {}),
        ("post", f"/api/tasks/{mine['id']}/cancel", None),
        ("delete", f"/api/tasks/{mine['id']}", None),
        ("get", f"/api/tasks/{mine['id']}/files", None),
    ]:
        kw = {"json": body} if body is not None else {}
        assert getattr(client, method)(path, headers=h, **kw)[1].status == 403, (method, path)
    assert db(dsn, mine["id"])["status"] == "none", "被拒的请求不得改库"
    assert client.get("/api/tasks/nope", headers=h)[1].status == 404


# ==================== 创建 ====================


def test_create_without_files_is_none_and_with_files_is_draft(client, dsn):
    a = create(client, name="a", params_text={"k": 1})
    b = create(client, name="b", files=[{"name": "in.txt"}])
    assert a["status"] == "none" and b["status"] == "draft"
    assert json.loads(a["params_text"]) == {"k": 1}, "dict 入参存成 JSON 文本，不是 Python repr"
    assert a["steps"] == [] and "worker_id" not in a and "user_id" not in a, "响应不含租约列与归属"


@pytest.mark.parametrize("body", [{"task_type": ""}, {"task_type": "x" * 65}, {"task_type": "demo", "name": "n" * 129},
                                  {"task_type": "demo", "params_text": 123}])
def test_create_rejects_malformed_bodies(client, body):
    assert client.post("/api/tasks", json=body, headers=auth())[1].status == 400


def test_create_and_submit_in_one_call(client, dsn):
    created = create(client, submit=True)
    assert created["status"] == "pending"
    r = db(dsn, created["id"])
    assert r["status"] == "pending" and r["lease_seq"] == 1, "进池 bump 了 lease_seq"


def test_type_params_validator_runs_on_create(client, types):
    from webx.lib.task_errors import ParamsError

    def check(params):
        if params.get("n", 1) < 1:
            raise ParamsError("n_too_small", min=1)

    types(TaskTypeSpec(name="validated", validate_params=check))
    ok = client.post("/api/tasks", json={"task_type": "validated", "params_text": {"n": 2}}, headers=auth())[1]
    bad = client.post("/api/tasks", json={"task_type": "validated", "params_text": {"n": 0}}, headers=auth())[1]
    assert ok.status == 201 and bad.status == 400 and bad.json["code"] == "n_too_small"


def test_strict_mode_rejects_unregistered_types(client, monkeypatch, types):
    from webx.config import get_settings

    monkeypatch.setattr(get_settings(), "TASK_REQUIRE_REGISTERED_TYPES", True)
    types(TaskTypeSpec(name="known"))
    assert client.post("/api/tasks", json={"task_type": "known"}, headers=auth())[1].status == 201
    r = client.post("/api/tasks", json={"task_type": "unknown"}, headers=auth())[1]
    assert r.status == 400 and r.json["code"] == "unknown_task_type"


def test_daily_create_quota(client, monkeypatch):
    from webx.services import quota

    class Tiny(quota.DefaultQuotaPolicy):
        default_daily_create = 2

    quota.set_quota_policy(Tiny())
    try:
        create(client, "q1"); create(client, "q1")
        r = client.post("/api/tasks", json={"task_type": "demo"}, headers=auth("q1"))[1]
        assert r.status == 403 and r.json["scope"] == "daily_create" and r.json["limit"] == 2
        assert client.post("/api/tasks", json={"task_type": "demo"}, headers=auth("q2"))[1].status == 201
    finally:
        quota.set_quota_policy(quota.DefaultQuotaPolicy())


# ==================== 提交 ====================


def test_submit_flips_to_pending_with_the_lease_invariants(client, dsn):
    t = create(client, params_text={"a": 1})
    _, r = client.post(f"/api/tasks/{t['id']}/submit", json={}, headers=auth())
    assert r.status == 200 and r.json["data"] == {"task_id": t["id"], "status": "pending", "submitted": True}
    d = db(dsn, t["id"], "status, worker_id, lease_until, lease_seq, level, submitted_at, rand_rank")
    assert (d["status"], d["worker_id"], d["lease_until"]) == ("pending", "", None)
    assert d["lease_seq"] == 1 and d["submitted_at"] is not None


def test_submit_rejects_a_second_submission_and_overrides_params_wholesale(client, dsn):
    t = create(client, params_text={"keep": 1})
    client.post(f"/api/tasks/{t['id']}/submit", json={"params_text": {"only": 2}}, headers=auth())
    assert json.loads(db(dsn, t["id"])["params_text"]) == {"only": 2}, "提交时的参数是整体覆盖，不是合并"
    r = client.post(f"/api/tasks/{t['id']}/submit", json={}, headers=auth())[1]
    assert r.status == 400, "已投递的不能再提交，避免重复投递"


def test_invalid_params_at_submit_are_not_persisted(client, dsn, types):
    from webx.lib.task_errors import ParamsError

    def check(p):
        if "bad" in p:
            raise ParamsError("bad_key")

    types(TaskTypeSpec(name="strictp", validate_params=check))
    t = create(client, task_type="strictp", params_text={"fine": 1})
    r = client.post(f"/api/tasks/{t['id']}/submit", json={"params_text": {"bad": 1}}, headers=auth())[1]
    assert r.status == 400
    d = db(dsn, t["id"])
    assert d["status"] == "none" and json.loads(d["params_text"]) == {"fine": 1}, "入口闸在落库之前"


def test_concurrency_quota_blocks_the_second_submission_without_leaving_orphans(client, dsn):
    a, b = create(client, "cq"), create(client, "cq")
    assert client.post(f"/api/tasks/{a['id']}/submit", json={}, headers=auth("cq"))[1].status == 200
    r = client.post(f"/api/tasks/{b['id']}/submit", json={}, headers=auth("cq"))[1]
    assert r.status == 403 and r.json["scope"] == "concurrency"
    assert db(dsn, b["id"])["status"] == "none", "闸在状态翻转之前：不留 PENDING 孤儿"


def test_precheck_hook_runs_before_any_write(client, dsn, types):
    from webx.lib.task_errors import QuotaExceededError

    async def gate(task):
        raise QuotaExceededError("custom_gate", limit=1, current=2)

    types(TaskTypeSpec(name="gated", precheck_submit=gate))
    t = create(client, task_type="gated")
    r = client.post(f"/api/tasks/{t['id']}/submit", json={}, headers=auth())[1]
    assert r.status == 403 and r.json["scope"] == "custom_gate"
    assert db(dsn, t["id"])["status"] == "none"


def test_submit_derives_the_timeout_for_the_step_being_submitted(client, dsn, types):
    cfg = TaskStageConfig(end_step="b", step_transitions={"": {"a"}, "a": {"b"}},
                          default_timeout=1000, timeout_by_step={"a": 77})
    types(TaskTypeSpec(name="timed", stage=cfg))
    t = create(client, task_type="timed")
    client.post(f"/api/tasks/{t['id']}/submit", json={}, headers=auth())
    d = db(dsn, t["id"], "sub_step, timeout_seconds")
    assert d["sub_step"] == "a", "多阶段首次提交按状态机推导起始步骤"
    assert d["timeout_seconds"] == 77, "超时是'本步上限'，在投递咽喉按当次步骤现算"


# ==================== 列表 / 详情 ====================


def test_list_is_scoped_to_the_user_top_level_only_and_paginated(client, dsn):
    ids = [create(client, "ls", name=f"t{i}")["id"] for i in range(5)]
    create(client, "someone-else")
    execute(dsn, "UPDATE tasks SET parent_id = $1 WHERE id = $2", ids[0], ids[1])  # 把 t1 变成 t0 的子任务

    _, r = client.get("/api/tasks?page=1&page_size=3", headers=auth("ls"))
    body = r.json
    assert body["total"] == 4 and len(body["items"]) == 3 and body["page_size"] == 3
    assert all(i["id"] != ids[1] for i in body["items"]), "子任务只在父任务详情里出现"
    page2 = client.get("/api/tasks?page=2&page_size=3", headers=auth("ls"))[1].json["items"]
    assert len(page2) == 1
    assert client.get("/api/tasks?page=x", headers=auth("ls"))[1].status == 400
    assert client.get("/api/tasks?page_size=100000", headers=auth("ls"))[1].json["page_size"] == 100


def test_list_filters_by_status_and_multiple_types(client):
    create(client, "f1", task_type="a"); create(client, "f1", task_type="b"); create(client, "f1", task_type="c")
    both = client.get("/api/tasks?task_type=a,b", headers=auth("f1"))[1].json
    assert both["total"] == 2, "逗号分隔多值一次 __in 查回，分页与 total 才对得上"
    assert client.get("/api/tasks?status=draft", headers=auth("f1"))[1].json["total"] == 0
    assert client.get("/api/tasks?status=none", headers=auth("f1"))[1].json["total"] == 3
    assert client.get("/api/tasks?status=bogus", headers=auth("f1"))[1].status == 400


def test_ids_query_is_a_lightweight_status_poll(client):
    a = create(client, "p1", name="A"); b = create(client, "p1", name="B")
    other = create(client, "p2")
    _, r = client.get(f"/api/tasks?ids={a['id']},{b['id']},{other['id']},nope,{a['id']}", headers=auth("p1"))
    got = {x["id"]: x for x in r.json["data"]}
    assert set(got) == {a["id"], b["id"]}, "非本人 / 不存在的静默跳过，重复去重"
    assert set(got[a["id"]]) == {"id", "status", "name", "steps", "sub_step", "sub_status", "step_message", "error_message"}
    too_many = ",".join(f"id{i}" for i in range(101))
    assert client.get(f"/api/tasks?ids={too_many}", headers=auth("p1"))[1].status == 400


def test_detail_inlines_children_and_children_endpoint_filters_by_step(client, dsn):
    parent = create(client, "dt")
    kids = [create(client, "dt", name=f"k{i}")["id"] for i in range(3)]
    execute(dsn, "UPDATE tasks SET parent_id=$1, parent_step='s1', tag='x' WHERE id=$2", parent["id"], kids[0])
    execute(dsn, "UPDATE tasks SET parent_id=$1, parent_step='s2', tag='y' WHERE id=$2", parent["id"], kids[1])
    execute(dsn, "UPDATE tasks SET parent_id=$1, parent_step='s2', tag='z', is_delete=true WHERE id=$2", parent["id"], kids[2])

    detail = client.get(f"/api/tasks/{parent['id']}", headers=auth("dt"))[1].json["data"]
    assert [c["id"] for c in detail["children"]] == kids[:2], "软删除的不出现"
    s2 = client.get(f"/api/tasks/{parent['id']}/children?parent_step=s2", headers=auth("dt"))[1].json["data"]
    assert [c["id"] for c in s2] == [kids[1]]
    child = client.get(f"/api/tasks/{kids[0]}", headers=auth("dt"))[1].json["data"]
    assert "children" not in child


# ==================== 编辑（PATCH）/ 辅助信息 ====================


def test_patch_draft_merges_params_and_can_save_without_submitting(client, dsn):
    t = create(client, params_text={"a": 1})
    _, r = client.patch(f"/api/tasks/{t['id']}", json={"params_text": {"b": 2}, "submit": False}, headers=auth())
    assert r.status == 200
    d = db(dsn, t["id"])
    assert d["status"] == "none" and json.loads(d["params_text"]) == {"a": 1, "b": 2}, "草稿模式合并而不投递"
    assert client.patch(f"/api/tasks/{t['id']}", json={"submit": False}, headers=auth())[1].status == 400, "空 PATCH 无意义"
    r = client.patch(f"/api/tasks/{t['id']}", json={}, headers=auth())[1]  # submit 默认 true
    assert r.status == 200 and db(dsn, t["id"])["status"] == "pending"


def test_patch_is_refused_once_the_task_is_in_flight(client, dsn):
    t = create(client, submit=True)
    r = client.patch(f"/api/tasks/{t['id']}", json={"params_text": {"x": 1}}, headers=auth())[1]
    assert r.status == 400 and "不可编辑" in r.json["error"]


def test_patch_validates_step_transitions(client, types):
    types(TaskTypeSpec(name="multi", stage=MULTI))
    t = create(client, task_type="multi")
    assert client.patch(f"/api/tasks/{t['id']}", json={"step": "render", "submit": False}, headers=auth())[1].status == 400
    r = client.patch(f"/api/tasks/{t['id']}", json={"step": "bogus", "submit": False}, headers=auth())[1]
    assert r.status == 400 and r.json["code"] == "invalid_step_name"
    ok = client.patch(f"/api/tasks/{t['id']}", json={"step": "prepare", "submit": False}, headers=auth())[1]
    assert ok.status == 200 and ok.json["data"]["sub_step"] == "prepare"


def test_info_update_works_in_any_state_and_requires_a_field(client):
    t = create(client, submit=True)
    r = client.put(f"/api/tasks/{t['id']}/info", json={"name": "renamed"}, headers=auth())[1]
    assert r.status == 200 and r.json["data"]["name"] == "renamed", "辅助信息不判断任务状态"
    assert client.put(f"/api/tasks/{t['id']}/info", json={}, headers=auth())[1].status == 400


def test_auto_post_tasks_are_off_limits_to_manual_edits(client, dsn):
    t = create(client)
    execute(dsn, "UPDATE tasks SET status='draft', auto_post=1 WHERE id=$1", t["id"])
    for call in (lambda: client.patch(f"/api/tasks/{t['id']}", json={"submit": False, "params_text": {}}, headers=auth()),
                 lambda: client.post(f"/api/tasks/{t['id']}/submit", json={}, headers=auth())):
        assert call()[1].status == 409, "等自动投递的任务由巡检择机处理，手动操作会绕过节流并遗留脏标记"


# ==================== 取消 / 重试 / 改写 / 重做 / 删除 ====================


def test_cancel_and_the_states_it_refuses(client, dsn):
    draft = create(client)
    assert client.post(f"/api/tasks/{draft['id']}/cancel", headers=auth())[1].status == 400
    t = create(client, "cc", submit=True)
    r = client.post(f"/api/tasks/{t['id']}/cancel", headers=auth("cc"))[1]
    assert r.status == 200 and r.json["data"]["status"] == "cancelled"
    d = db(dsn, t["id"])
    assert (d["status"], d["sub_status"]) == ("cancelled", "draft"), "CANCELLED 是'暂停态 + 编辑窗口'"
    assert client.post(f"/api/tasks/{t['id']}/cancel", headers=auth("cc"))[1].status == 400


def test_cancel_is_refused_when_nothing_is_running(client, dsn, types):
    types(TaskTypeSpec(name="multi", stage=MULTI))
    t = create(client, task_type="multi")
    execute(dsn, "UPDATE tasks SET status='running', sub_status='success', sub_step='' WHERE id=$1", t["id"])
    r = client.post(f"/api/tasks/{t['id']}/cancel", headers=auth())[1]
    assert r.status == 409 and "无需取消" in r.json["error"], "步骤刚完成、等用户追加——没有计算在进行"


def test_retry_resets_the_round_and_bumps_the_fencing_seq(client, dsn):
    t = create(client, "rt", submit=True)
    execute(dsn, "UPDATE tasks SET status='failed', sub_status='failed', error_message='boom', worker_id='w9', "
                 "lease_seq=5, requeues=3, step_message='old' WHERE id=$1", t["id"])
    r = client.post(f"/api/tasks/{t['id']}/retry", json={}, headers=auth("rt"))[1]
    assert r.status == 200
    d = db(dsn, t["id"], "status, retry, lease_seq, worker_id, requeues, error_message, step_message")
    assert d["status"] == "pending" and d["retry"] == 1
    assert d["worker_id"] == "" and d["requeues"] == 0 and d["error_message"] == "" and d["step_message"] == ""
    assert d["lease_seq"] >= 6, "上一轮 worker 的在途回调不能匹配到这一轮"


def test_retry_refuses_non_terminal_states_and_exhausted_budgets(client, dsn):
    t = create(client, "rb")
    assert client.post(f"/api/tasks/{t['id']}/retry", json={}, headers=auth("rb"))[1].status == 400
    execute(dsn, "UPDATE tasks SET status='failed', retry=allowed_retries WHERE id=$1", t["id"])
    r = client.post(f"/api/tasks/{t['id']}/retry", json={}, headers=auth("rb"))[1]
    assert r.status == 400 and "上限" in r.json["error"]


def test_failed_precheck_leaves_the_task_failed_and_the_retry_unspent(client, dsn, types):
    from webx.lib.task_errors import QuotaExceededError

    async def gate(task):
        raise QuotaExceededError("gate", 1, 2)

    types(TaskTypeSpec(name="gate2", precheck_submit=gate))
    t = create(client, task_type="gate2")
    execute(dsn, "UPDATE tasks SET status='failed' WHERE id=$1", t["id"])
    assert client.post(f"/api/tasks/{t['id']}/retry", json={}, headers=auth())[1].status == 403
    d = db(dsn, t["id"])
    assert (d["status"], d["retry"]) == ("failed", 0), "校验在消耗 retry / 翻 PENDING 之前"


def test_revise_returns_to_an_editable_draft(client, dsn):
    t = create(client, "rv", submit=True)
    execute(dsn, "UPDATE tasks SET status='failed', worker_id='w', lease_seq=2 WHERE id=$1", t["id"])
    r = client.post(f"/api/tasks/{t['id']}/revise", headers=auth("rv"))[1]
    d = db(dsn, t["id"])
    assert r.status == 200 and (d["status"], d["sub_status"], d["retry"], d["worker_id"]) == ("draft", "draft", 1, "")
    assert d["lease_seq"] == 3


def test_redo_truncates_the_trajectory_and_opens_the_edit_window(client, dsn, types):
    types(TaskTypeSpec(name="multi", stage=MULTI))
    t = create(client, "rd", task_type="multi")
    execute(dsn, "UPDATE tasks SET status='running', sub_status='success', sub_step='', steps=$2, "
                 "result_path=$3, worker_id='w', lease_seq=4 WHERE id=$1", t["id"],
            json.dumps(["prepare", "render"]),
            json.dumps({"prepare": {"url": "/public/tasks/x/results/p.zip"}, "render": {"url": "/public/tasks/x/results/r.zip"}}))
    r = client.post(f"/api/tasks/{t['id']}/redo", json={"step": "render"}, headers=auth("rd"))[1]
    assert r.status == 200
    d = db(dsn, t["id"], "status, sub_status, sub_step, steps, retry, lease_seq, worker_id, result_path")
    assert (d["status"], d["sub_status"], d["sub_step"], d["retry"]) == ("running", "draft", "render", 1)
    assert json.loads(d["steps"]) == ["prepare"], "截断到目标步之前"
    assert json.loads(d["result_path"]) == {"prepare": {"url": "/public/tasks/x/results/p.zip"}}, \
        "被丢弃步骤的产物地址一并作废：否则 worker 恢复时会把上一代产物当当代取回来"
    assert d["lease_seq"] == 5 and d["worker_id"] == ""
    assert client.post(f"/api/tasks/{t['id']}/redo", json={"step": "encode"}, headers=auth("rd"))[1].status == 400, "尚未执行的步骤不能重做"
    assert client.post(f"/api/tasks/{t['id']}/redo", json={"step": "bogus"}, headers=auth("rd"))[1].status == 400


def test_redo_is_refused_for_single_stage_tasks(client, dsn):
    t = create(client)
    execute(dsn, "UPDATE tasks SET status='failed' WHERE id=$1", t["id"])
    assert client.post(f"/api/tasks/{t['id']}/redo", json={"step": "a"}, headers=auth())[1].status == 400


def test_delete_rules(client, dsn):
    draft = create(client, "dl")
    assert client.delete(f"/api/tasks/{draft['id']}", headers=auth("dl"))[1].status == 200
    assert fetch(dsn, "SELECT count(*) AS n FROM tasks WHERE id=$1", draft["id"])[0]["n"] == 0

    active = create(client, "dl", submit=True)
    r = client.delete(f"/api/tasks/{active['id']}", headers=auth("dl"))[1]
    assert r.status == 400 and "不能删除" in r.json["error"], "在飞任务不可删，避免悬空回调"

    child = create(client, "dl")
    execute(dsn, "UPDATE tasks SET parent_id=$1 WHERE id=$2", active["id"], child["id"])
    assert client.delete(f"/api/tasks/{child['id']}", headers=auth("dl"))[1].status == 409, "子任务不开放直接删除"


def test_delete_cascades_to_children_and_idle_multistage_tasks_are_deletable(client, dsn, types):
    types(TaskTypeSpec(name="multi", stage=MULTI))
    parent = create(client, "dc", task_type="multi")
    kid = create(client, "dc")
    execute(dsn, "UPDATE tasks SET parent_id=$1 WHERE id=$2", parent["id"], kid["id"])
    execute(dsn, "UPDATE tasks SET status='running', sub_status='success', sub_step='' WHERE id=$1", parent["id"])
    assert client.delete(f"/api/tasks/{parent['id']}", headers=auth("dc"))[1].status == 200, \
        "停在 running 但空闲（等用户追加参数）的多阶段任务不占名额，可删"
    assert fetch(dsn, "SELECT count(*) AS n FROM tasks WHERE id = ANY($1)", [parent["id"], kid["id"]])[0]["n"] == 0


# ==================== 输入文件 ====================


def _mp(filename, content, extra=()):
    b = "----tf"
    body = b""
    for name, value in extra:
        body += f'--{b}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode()
    body += (f'--{b}\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'
             f"Content-Type: application/octet-stream\r\n\r\n").encode() + content + f"\r\n--{b}--\r\n".encode()
    return body, {"Content-Type": f"multipart/form-data; boundary={b}"}


def test_input_files_lifecycle(client, dsn):
    t = create(client, "fl")
    body, ct = _mp("in.txt", b"hello")
    r = client.post(f"/api/tasks/{t['id']}/files", data=body, headers={**auth("fl"), **ct})[1]
    assert r.status == 201 and r.json["data"] == {"name": "in.txt", "size": 5}
    assert db(dsn, t["id"])["status"] == "draft", "有输入文件的任务从 none 翻成 draft"

    assert client.get(f"/api/tasks/{t['id']}/files", headers=auth("fl"))[1].json["data"] == [{"name": "in.txt", "size": 5}]
    down = client.get(f"/api/tasks/{t['id']}/files/in.txt", headers=auth("fl"))[1]
    assert down.status == 200 and down.body == b"hello"

    body, ct = _mp("in.txt", b"new!!!")  # 同名覆盖
    client.post(f"/api/tasks/{t['id']}/files", data=body, headers={**auth("fl"), **ct})
    files = client.get(f"/api/tasks/{t['id']}/files", headers=auth("fl"))[1].json["data"]
    assert files == [{"name": "in.txt", "size": 6}]

    assert client.delete(f"/api/tasks/{t['id']}/files/in.txt", headers=auth("fl"))[1].status == 200
    assert client.get(f"/api/tasks/{t['id']}/files", headers=auth("fl"))[1].json["data"] == []
    assert client.delete(f"/api/tasks/{t['id']}/files/in.txt", headers=auth("fl"))[1].status == 404


def test_input_files_are_frozen_once_submitted_and_names_are_guarded(client):
    t = create(client, "fz")
    for bad in ("../../etc/passwd", ".hidden", "a/b"):
        body, ct = _mp(bad, b"x")
        r = client.post(f"/api/tasks/{t['id']}/files", data=body, headers={**auth("fz"), **ct})[1]
        assert r.status in (400, 404), bad
    client.post(f"/api/tasks/{t['id']}/submit", json={}, headers=auth("fz"))
    body, ct = _mp("late.txt", b"x")
    r = client.post(f"/api/tasks/{t['id']}/files", data=body, headers={**auth("fz"), **ct})[1]
    assert r.status == 409, "已投递的任务输入变了而 worker 已拿走旧的，会静默跑错"


# ==================== 子任务：准入与父任务提升（服务层，真实 PG）====================


def _admit(dsn, fn):
    from webx.services.task import task_service

    return orm(dsn, lambda: fn(task_service))


def test_children_are_admitted_within_the_per_user_child_quota(dsn):
    from webx.models import Task, TaskStatus

    async def setup(svc):
        parent = await svc.create_task("demo", user_id="ad", name="p")
        kids = [await svc.create_child(parent, "demo", tag=f"k{i}") for i in range(3)]
        return parent.id, [k.id for k in kids]

    pid, kids = _admit(dsn, setup)
    assert {r["auto_post"] for r in fetch(dsn, "SELECT auto_post FROM tasks WHERE id = ANY($1)", kids)} == {1}

    async def admit(svc):
        return await svc.admit_pending_children()

    assert _admit(dsn, admit) == 2, "level 0 的子任务额度是 顶层上限(1)+1 = 2：第三个留队"
    states = {r["id"]: (r["status"], r["auto_post"]) for r in fetch(dsn, "SELECT id, status, auto_post FROM tasks WHERE id = ANY($1)", kids)}
    assert sorted(v[0] for v in states.values()) == ["draft", "pending", "pending"]
    assert sum(1 for v in states.values() if v == ("draft", 1)) == 1, "留队的保持 DRAFT + auto_post=1"

    execute(dsn, "UPDATE tasks SET status='completed' WHERE id = (SELECT id FROM tasks WHERE parent_id=$1 AND status='pending' LIMIT 1)", pid)
    assert _admit(dsn, admit) == 1, "有名额释放后，留队的被投递"
    assert Task and TaskStatus  # 仅为让导入显式


def test_children_are_exempt_from_the_top_level_concurrency_gate(dsn):
    async def go(svc):
        parent = await svc.create_task("demo", user_id="ex")
        await svc.submit_task(parent)  # 占满顶层额度(1)
        kid = await svc.create_child(parent, "demo", tag="x")
        return await svc._try_admit_child(kid)

    assert _admit(dsn, go) == "admitted", "子任务有独立额度池，不被顶层任务占满的名额挡住"


def test_a_held_parent_is_promoted_only_after_all_children_finish(dsn):
    async def setup(svc):
        parent = await svc.create_task("demo", user_id="hp")
        kids = [await svc.create_child(parent, "demo", tag=f"k{i}") for i in range(2)]
        assert await svc.hold_parent(parent)
        return parent.id, [k.id for k in kids]

    pid, kids = _admit(dsn, setup)
    held = db(dsn, pid, "status, auto_post")
    assert (held["status"], held["auto_post"]) == ("draft", 1)

    async def promote(svc):
        return await svc.admit_waiting_parents()

    assert _admit(dsn, promote) == 0, "子任务还没算完：继续等"
    execute(dsn, "UPDATE tasks SET status='completed' WHERE id=$1", kids[0])
    assert _admit(dsn, promote) == 0
    execute(dsn, "UPDATE tasks SET status='failed' WHERE id=$1", kids[1])
    assert _admit(dsn, promote) == 1, "子任务全部终态（成功或失败）才提升"
    d = db(dsn, pid)
    assert d["status"] == "pending"
    assert db(dsn, pid, "auto_post")["auto_post"] == 0, "投递成功后才清 auto_post"


def test_a_waiting_parent_whose_children_vanished_is_submitted_instead_of_waiting_forever(dsn):
    async def setup(svc):
        parent = await svc.create_task("demo", user_id="vn")
        assert await svc.hold_parent(parent)
        return parent.id

    pid = _admit(dsn, setup)

    async def promote(svc):
        return await svc.admit_waiting_parents()

    assert _admit(dsn, promote) == 1
    assert db(dsn, pid)["status"] == "pending"


def test_the_waiting_parent_pile_up_gate(dsn, monkeypatch):
    from webx.config import get_settings
    from webx.lib.task_errors import QuotaExceededError

    monkeypatch.setattr(get_settings(), "TASK_WAITING_PARENT_MAX", 1)

    async def go(svc):
        a = await svc.create_task("demo", user_id="pg")
        b = await svc.create_task("demo", user_id="pg")
        assert await svc.hold_parent(a)
        try:
            await svc.hold_parent(b)
        except QuotaExceededError as e:
            return e.scope
        return None

    assert _admit(dsn, go) == "waiting_children"


def test_the_hold_submit_hook_takes_over_the_submission(dsn, types):
    async def hold(task):
        from webx.services.task import task_service

        await task_service.create_child(task, "demo", tag="auto")
        return await task_service.hold_parent(task)

    types(TaskTypeSpec(name="holder", hold_submit=hold))

    async def go(svc):
        parent = await svc.create_task("holder", user_id="hk")
        ok = await svc.submit_task(parent)
        return ok, parent.id

    ok, pid = _admit(dsn, go)
    d = db(dsn, pid, "status, auto_post")
    assert ok and (d["status"], d["auto_post"]) == ("draft", 1), "钩子接管后提交流程到此结束：任务没有入池"
    assert fetch(dsn, "SELECT count(*) AS n FROM tasks WHERE parent_id=$1", pid)[0]["n"] == 1


def test_aggregate_gate_blocks_until_children_of_that_step_complete(dsn, types):
    from webx.lib.task_errors import ChildrenNotReadyError

    cfg = TaskStageConfig(end_step="agg", step_transitions={"": {"agg"}}, aggregate_steps=frozenset({"agg"}))
    types(TaskTypeSpec(name="aggr", stage=cfg,
                       expected_child_tags=lambda task, step: ["m1", "m2"] if step == "agg" else []))

    async def go(svc):
        parent = await svc.create_task("aggr", user_id="ag")
        c1 = await svc.create_child(parent, "demo", tag="m1", parent_step="agg")
        try:
            await svc.precheck_submittable(parent, "agg")
        except ChildrenNotReadyError as e:
            first = (sorted(e.pending), e.failed, e.total)
        await Task_update(c1.id, "completed")
        try:
            await svc.precheck_submittable(parent, "agg")
        except ChildrenNotReadyError as e:
            second = (sorted(e.pending), e.failed)
        return first, second

    async def Task_update(tid, status):
        from webx.models import Task

        await Task.filter(id=tid).update(status=status)

    first, second = _admit(dsn, go)
    assert first == (["m1", "m2"], [], 2), "缺失的子任务也算未就绪（expected_child_tags）"
    assert second == (["m2"], [])


# ==================== 审查修复：授权 / 输入校验 ====================


def test_empty_owner_is_not_a_wildcard(client, dsn):
    """库里 user_id 为空的任务，不能被别的用户访问；令牌没有身份时 ids 轮询也不能带回别人的任务。"""
    t = create(client, "owner1")
    execute(dsn, "UPDATE tasks SET user_id='' WHERE id=$1", t["id"])
    assert client.get(f"/api/tasks/{t['id']}", headers=auth("mallory"))[1].status == 403
    assert client.put(f"/api/tasks/{t['id']}/info", json={"name": "x"}, headers=auth("mallory"))[1].status == 403
    assert client.delete(f"/api/tasks/{t['id']}", headers=auth("mallory"))[1].status == 403

    other = create(client, "owner2")
    _, r = client.get(f"/api/tasks?ids={other['id']}", headers=auth(""))
    assert r.json["data"] == [], "没有身份的令牌不能用 ids 带回别人的任务"


def test_children_cannot_be_submitted_revised_or_redone_by_users(client, dsn):
    """子任务由系统准入闸管理：用户逐个 revise + submit 会绕过子任务独立的额度池与重试预算。"""
    parent, child = create(client, "pc"), create(client, "pc")
    execute(dsn, "UPDATE tasks SET parent_id=$1, status='failed' WHERE id=$2", parent["id"], child["id"])
    h = auth("pc")
    for method, path, body in (
        ("post", f"/api/tasks/{child['id']}/revise", None),
        ("post", f"/api/tasks/{child['id']}/redo", {"step": "s"}),
        ("patch", f"/api/tasks/{child['id']}", {"params_text": {}}),
    ):
        kw = {"headers": h, **({"json": body} if body is not None else {})}
        assert getattr(client, method)(path, **kw)[1].status == 409, (method, path)
    execute(dsn, "UPDATE tasks SET status='draft' WHERE id=$1", child["id"])
    assert client.post(f"/api/tasks/{child['id']}/submit", json={}, headers=h)[1].status == 409


def test_nul_bytes_and_huge_pages_are_client_errors_not_500(client):
    h = auth("nul")
    assert client.get("/api/tasks/%00", headers=h)[1].status == 400
    assert client.get("/api/tasks?ids=%00", headers=h)[1].status == 400
    assert client.get("/api/tasks?task_type=%00", headers=h)[1].status == 400
    assert client.post("/api/tasks", json={"task_type": "demo", "name": "a\u0000b"}, headers=h)[1].status == 400
    assert client.get("/api/tasks?page=99999999999999999999", headers=h)[1].status == 200, "page 被钳到上限，不是 500"


def test_params_text_string_must_be_a_json_object(client):
    """"not json" 以前会被存进库发给 worker，而 parse_params 静默当成 {}——用户的参数就这样丢了。"""
    h = auth("pj")
    for bad in ("not json", "123", '"x"'):
        _, r = client.post("/api/tasks", json={"task_type": "demo", "params_text": bad}, headers=h)
        assert r.status == 400, bad
    _, r = client.post("/api/tasks", json={"task_type": "demo", "params_text": '{"a": 1}'}, headers=h)
    assert r.status == 201


def test_create_with_submit_that_is_rejected_leaves_no_orphan(client, dsn, types):
    """前置闸拒绝时，刚创建的任务不能留成占每日额度、又无从找回 id 的孤儿。"""
    from webx.lib.task_errors import ParamsError

    def deny(params):
        raise ParamsError("nope")

    types(TaskTypeSpec(name="denies", validate_params=deny))
    before = fetch(dsn, "SELECT count(*) AS n FROM tasks WHERE user_id='orph' AND is_delete=false")[0]["n"]
    _, r = client.post("/api/tasks", json={"task_type": "denies", "submit": True}, headers=auth("orph"))
    assert r.status >= 400
    after = fetch(dsn, "SELECT count(*) AS n FROM tasks WHERE user_id='orph' AND is_delete=false")[0]["n"]
    assert after == before


def test_input_file_upload_during_submit_does_not_pull_the_task_back_to_draft(client, dsn):
    """上传期间任务被提交：结束时不能用旧状态把它写回 DRAFT（任务会从池里消失）。直接对行锁更新函数做断言。"""
    import asyncio

    from webx.controller.api.task import _update_input_files

    t = create(client, "up")
    execute(dsn, "UPDATE tasks SET status='pending' WHERE id=$1", t["id"])

    async def go():
        return await _update_input_files(t["id"], lambda fs: fs + [{"name": "x"}], to_draft=True)

    assert orm(dsn, go) is False
    row = fetch(dsn, "SELECT status, files_json FROM tasks WHERE id=$1", t["id"])[0]
    assert row["status"] == "pending" and "x" not in (row["files_json"] or "")
