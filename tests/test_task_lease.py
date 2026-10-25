"""认领 / 租约 / fencing 的 SQL 语义——全部在**真实 PostgreSQL** 上跑。

互斥性来自一条带条件的 UPDATE，租约的时间运算全在 SQL 里用数据库 now()。sqlite 里这些都跑不起来，
而它们恰恰是全系统正确性的来源——所以这里不 mock 数据库。
"""

import asyncio
import json

import pytest

from webx.models import SubStatus, Task, TaskStatus, User
from webx.services import claim, lease

pytestmark = pytest.mark.usefixtures("pgdb")


async def db_ms(conn) -> int:
    rows = await conn.execute_query_dict("SELECT (EXTRACT(EPOCH FROM now()) * 1000)::bigint AS ms")
    return rows[0]["ms"]


async def mk(task_type="demo", user_id="u1", status=TaskStatus.PENDING, **kw) -> Task:
    return await Task.create(task_type=task_type, user_id=user_id, status=status, **kw)


async def fresh(task: Task) -> Task:
    return await Task.get(id=task.id)


# ==================== 认领：互斥 ====================


async def test_claim_is_exclusive_under_concurrency():
    """40 个并发认领抢 20 个任务：每个任务恰好被一个认领方拿到，不重不漏。"""
    ids = {(await mk()).id for _ in range(20)}

    async def worker(n):
        got = []
        for _ in range(5):
            row = await claim.claim_one(f"w{n}", ["demo"])
            if row:
                got.append(row)
        return got

    results = await asyncio.gather(*(worker(n) for n in range(8)))
    claimed = [row for batch in results for row in batch]
    claimed_ids = [r["id"] for r in claimed]

    assert len(claimed_ids) == len(set(claimed_ids)) == 20, "没有任务被重复认领"
    assert set(claimed_ids) == ids
    for r in claimed:  # 响应里写的与库里存的一致
        t = await Task.get(id=r["id"])
        assert (t.status, t.worker_id, t.lease_seq) == (TaskStatus.QUEUED, r["worker_id"], 1)
    assert await claim.claim_one("late", ["demo"]) is None, "池空了就是空"


async def test_claim_only_takes_pending_live_tasks_of_the_requested_types():
    await mk(status=TaskStatus.DRAFT)
    await mk(status=TaskStatus.QUEUED)
    await mk(status=TaskStatus.RUNNING)
    await mk(is_delete=True)
    await mk(task_type="other")
    assert await claim.claim_one("w", ["demo"]) is None
    assert await claim.claim_one("w", []) is None, "空类型列表不发 SQL"
    got = await claim.claim_one("w", ["nope", "other"])
    assert got and got["task_type"] == "other"


async def test_claim_order_level_then_points_then_rand_rank_then_submitted_at():
    from datetime import datetime, timedelta, timezone

    t0 = datetime.now(timezone.utc)
    low = await mk(level=0, rand_rank=1, submitted_at=t0)
    high = await mk(level=2, rand_rank=9, submitted_at=t0)
    paid = await mk(level=0, compute_points=50, rand_rank=9, submitted_at=t0)
    early = await mk(level=0, rand_rank=1, submitted_at=t0 - timedelta(hours=1))
    order = [(await claim.claim_one("w", ["demo"]))["id"] for _ in range(4)]
    # level 高者先；同档位 compute_points 高者先；再 rand_rank 小者先；rand_rank 相同看更早提交
    assert order == [high.id, paid.id, early.id, low.id]


async def test_claim_filters_are_upper_bounds_and_only_narrow_the_pool():
    free = await mk(level=0, compute_points=0)
    mid = await mk(level=1, compute_points=0)
    paid = await mk(level=0, compute_points=100)

    got = await claim.claim_one("w", ["demo"], max_level=0, max_compute_points=0)
    assert got["id"] == free.id, "max_level=0 且 max_points=0 只服务免费档"
    assert await claim.claim_one("w", ["demo"], max_level=0, max_compute_points=0) is None
    got = await claim.claim_one("w", ["demo"], max_level=1)
    assert got["id"] in {mid.id, paid.id}
    # 不带筛选的认领方仍能拿到剩下的——筛选只缩小候选集，不改互斥语义
    assert (await claim.claim_one("w2", ["demo"]))["id"] in {mid.id, paid.id}


async def test_claim_for_slots_one_task_per_lane_in_priority_order():
    a = await mk(task_type="heavy")
    b = await mk(task_type="light")
    got = await claim.claim_for_slots("w", [["heavy"], ["light"]])
    assert [r["id"] for r in got] == [a.id, b.id]

    c = await mk(task_type="light")
    got = await claim.claim_for_slots("w", [["heavy"], ["light"]])
    assert [r["id"] for r in got] == [c.id], "只领到一个也照常返回，不等凑齐"
    assert await claim.claim_for_slots("w", [["heavy"]]) == []


async def test_claim_response_columns_are_fixed_and_json_serializable(pgdb):
    await mk(user_id="secret-user", org_id="secret-org", files_json='[{"name":"a"}]', params_text='{"k":1}')
    row = await claim.claim_one("w", ["demo"])
    assert set(row) == {c.strip() for c in claim.RETURN_COLS.split(",")}
    assert "user_id" not in row and "org_id" not in row and "error_message" not in row
    json.dumps(row)  # 不含 datetime 列，端点返回时不会炸
    assert row["status"] == "queued" and row["lease_seq"] == 1 and row["worker_id"] == "w"

    t = await Task.get(id=row["id"])
    ms = await db_ms(pgdb)
    assert 50_000 < t.lease_until - ms <= 60_000, "认领写的是 LEASE_CLAIM_SECONDS(60s) 的短租约，且按数据库时钟"
    assert abs(t.last_heartbeat - ms) < 5_000


async def test_claim_sql_shape_depends_only_on_the_filter_set():
    assert "level <=" not in claim.build_claim_sql(False, False)
    assert "level <= $4" in claim.build_claim_sql(True, False)
    assert "compute_points <= $4" in claim.build_claim_sql(False, True)
    both = claim.build_claim_sql(True, True)
    assert "level <= $4" in both and "compute_points <= $5" in both
    assert "SKIP LOCKED" in both and "LIMIT 1" in both


# ==================== 进池 ====================


async def test_enter_pool_restores_the_invariants_and_bumps_the_fencing_seq(pgdb):
    t = await mk(status=TaskStatus.RUNNING, worker_id="w1", lease_seq=4, lease_until=999, last_heartbeat=5, level=0)
    assert await lease.enter_pending_pool(t.id, ["running", "queued"], level=2) is True
    t = await fresh(t)
    assert (t.status, t.worker_id, t.lease_until, t.last_heartbeat) == (TaskStatus.PENDING, "", None, 0)
    assert t.lease_seq == 5, "任何路径 → PENDING 都要 lease_seq+1"
    assert t.level == 2 and t.submitted_at is not None


async def test_enter_pool_is_a_compare_and_swap_on_status():
    t = await mk(status=TaskStatus.COMPLETED)
    assert await lease.enter_pending_pool(t.id, ["running", "queued"]) is False
    assert (await fresh(t)).status == TaskStatus.COMPLETED, "已被并发改走就不动"
    assert await lease.enter_pending_pool("no-such-task", ["pending"]) is False


async def test_enter_pool_keeps_or_resets_submitted_at():
    from datetime import datetime, timedelta, timezone

    old = datetime.now(timezone.utc) - timedelta(days=3)
    keep = await mk(status=TaskStatus.RUNNING, submitted_at=old)
    reset = await mk(status=TaskStatus.RUNNING, submitted_at=old)
    await lease.enter_pending_pool(keep.id, ["running"])
    await lease.enter_pending_pool(reset.id, ["running"], reset_submitted_at=True)
    assert abs(((await fresh(keep)).submitted_at - old).total_seconds()) < 1, "重投保留原提交时刻：等得最久的该排队首"
    assert (await fresh(reset)).submitted_at > old + timedelta(days=2)


async def test_enter_pool_snapshots_level_from_the_user_resolver():
    await User.create(id="vip", username="vip", level=3)
    t = await mk(user_id="vip", status=TaskStatus.DRAFT)
    await lease.enter_pending_pool(t.id, ["draft"])
    assert (await fresh(t)).level == 3, "level 经 UserResolver 快照到任务行，认领路径不 JOIN"


async def test_fair_share_rank_is_dominated_by_the_users_in_flight_count():
    """rand_rank = 在飞任务数 × 10000 + 抖动：已占 3 个位置的用户落在桶 3，空手用户落在桶 0。"""
    for _ in range(3):
        await mk(user_id="hog", status=TaskStatus.RUNNING)
    hog = await mk(user_id="hog", status=TaskStatus.DRAFT)
    newbie = await mk(user_id="newbie", status=TaskStatus.DRAFT)
    await lease.enter_pending_pool(hog.id, ["draft"])
    await lease.enter_pending_pool(newbie.id, ["draft"])
    assert 30_000 <= (await fresh(hog)).rand_rank < 40_000
    assert 0 <= (await fresh(newbie)).rand_rank < 10_000


async def test_fair_share_ignores_soft_deleted_and_waiting_for_user_tasks():
    await mk(user_id="u9", status=TaskStatus.RUNNING, is_delete=True)  # 软删不算
    await mk(user_id="u9", status=TaskStatus.RUNNING, end_step="last", sub_status=SubStatus.SUCCESS)  # 等用户不算
    await mk(user_id="u9", status=TaskStatus.RUNNING)  # 算
    t = await mk(user_id="u9", status=TaskStatus.DRAFT)
    await lease.enter_pending_pool(t.id, ["draft"])
    assert 10_000 <= (await fresh(t)).rand_rank < 20_000


async def test_claim_auto_post_child_is_a_three_predicate_cas():
    ok = await mk(status=TaskStatus.DRAFT, auto_post=1, parent_id="p")
    assert await lease.claim_auto_post_child(ok.id) is True
    t = await fresh(ok)
    assert t.status == TaskStatus.PENDING and t.auto_post == 1, "auto_post 刻意保留为 1，由调用方在提交成功后清"
    assert await lease.claim_auto_post_child(ok.id) is False, "已被认领就不再命中"

    for bad in (
        await mk(status=TaskStatus.DRAFT, auto_post=0, parent_id="p"),
        await mk(status=TaskStatus.DRAFT, auto_post=1, parent_id="p", is_delete=True),
        await mk(status=TaskStatus.CANCELLED, auto_post=1, parent_id="p"),
    ):
        assert await lease.claim_auto_post_child(bad.id) is False


# ==================== 续租 ====================


async def lessee(pgdb, **kw) -> Task:
    ms = await db_ms(pgdb)
    return await mk(status=kw.pop("status", TaskStatus.RUNNING), worker_id="w1", lease_seq=3,
                    lease_until=ms + 1000, **kw)


async def test_renew_requires_the_full_fencing_token(pgdb):
    t = await lessee(pgdb)
    assert await lease.renew_lease(t.id, "w1", 2) is None, "seq 不对"
    assert await lease.renew_lease(t.id, "w2", 3) is None, "worker 不对"
    row = await lease.renew_lease(t.id, "w1", 3)
    assert row and row["status"] == "running"
    pending = await mk(status=TaskStatus.PENDING, worker_id="w1", lease_seq=3)
    assert await lease.renew_lease(pending.id, "w1", 3) is None, "PENDING 不可续租"


async def test_renew_grace_floors_at_min_when_start_time_is_missing(pgdb):
    """已认领、已 ack、还没上报 running（start_time 为空）也要有下限保护，而不是 0。"""
    from webx.config import settings

    t = await lessee(pgdb, status=TaskStatus.QUEUED)
    ms = await db_ms(pgdb)
    row = await lease.renew_lease(t.id, "w1", 3)
    expected = (settings.LEASE_SECONDS + settings.LEASE_GRACE_MIN_SECONDS) * 1000
    assert abs((row["lease_until"] - ms) - expected) < 3_000


@pytest.mark.parametrize(
    "elapsed_s, grace_s",
    [
        (60, 180),         # α×60=7.2s < MIN → 取下限 180（网络抖动维度，该固定）
        (10_000, 1_200),   # α×10000 = 1200s，居中：跑得越久越有耐心
        (500_000, 18_000), # 超大 → 被 MAX 截断（防无限增长）
    ],
)
async def test_renew_grace_is_alpha_times_elapsed_clamped(pgdb, elapsed_s, grace_s):
    from webx.config import settings

    t = await lessee(pgdb)
    await pgdb.execute_query(
        "UPDATE tasks SET start_time = now() - ($1 * interval '1 second') WHERE id = $2", [elapsed_s, t.id]
    )
    ms = await db_ms(pgdb)
    row = await lease.renew_lease(t.id, "w1", 3)
    assert abs((row["lease_until"] - ms) - (settings.LEASE_SECONDS + grace_s) * 1000) < 3_000
    assert abs((await fresh(t)).last_heartbeat - ms) < 3_000


# ==================== 回收 ====================


async def expired(pgdb, **kw) -> Task:
    ms = await db_ms(pgdb)
    return await mk(worker_id="w1", lease_seq=2, lease_until=ms - 5_000, **kw)


async def test_reclaim_only_running_counts_toward_requeues(pgdb):
    """从 QUEUED 回收意味着任务根本没跑起来（多半是 Web 不可达），不能计入"连续无进展"。"""
    running = await expired(pgdb, status=TaskStatus.RUNNING, requeues=1)
    queued = await expired(pgdb, status=TaskStatus.QUEUED, requeues=1)
    rows = {r["id"]: r for r in await lease.reclaim_expired()}
    assert set(rows) == {running.id, queued.id}
    assert rows[running.id]["old_status"] == "running" and rows[queued.id]["old_status"] == "queued"

    r, q = await fresh(running), await fresh(queued)
    for t in (r, q):
        assert (t.status, t.worker_id, t.lease_until, t.last_heartbeat) == (TaskStatus.PENDING, "", None, 0)
        assert t.lease_seq == 3, "回收也要 bump，旧租户的在途回调才会被 fencing 挡住"
    assert (r.requeues, q.requeues) == (2, 1)


async def test_reclaim_leaves_live_and_leaseless_tasks_alone(pgdb):
    ms = await db_ms(pgdb)
    live = await mk(status=TaskStatus.RUNNING, worker_id="w1", lease_until=ms + 60_000)
    leaseless = await mk(status=TaskStatus.RUNNING, worker_id="w1", lease_until=None)
    pending = await mk(status=TaskStatus.PENDING)
    assert await lease.reclaim_expired() == []
    for t in (live, leaseless, pending):
        assert (await fresh(t)).lease_seq == t.lease_seq


async def test_fail_over_limit_runs_before_reclaim(pgdb):
    from webx.config import settings

    poison = await expired(pgdb, status=TaskStatus.RUNNING, requeues=settings.LEASE_REQUEUE_LIMIT)
    normal = await expired(pgdb, status=TaskStatus.RUNNING, requeues=settings.LEASE_REQUEUE_LIMIT - 1)

    failed = await lease.fail_over_limit()
    assert [r["id"] for r in failed] == [poison.id]
    p = await fresh(poison)
    assert (p.status, p.sub_status, p.worker_id) == (TaskStatus.FAILED, SubStatus.FAILED, "")
    assert f"连续 {settings.LEASE_REQUEUE_LIMIT} 次" in p.error_message and "w1" in p.error_message

    # 顺序：先失败后回收——否则刚被 +1 的任务在同一轮里又被判超限，阈值凭空少一次
    reclaimed = await lease.reclaim_expired()
    assert [r["id"] for r in reclaimed] == [normal.id]
    assert (await fresh(normal)).requeues == settings.LEASE_REQUEUE_LIMIT
    assert await lease.fail_over_limit() == [], "同一轮不会立刻把它判死"


async def test_release_is_fenced_and_does_not_charge_requeues(pgdb):
    t = await lessee(pgdb, requeues=4)
    assert await lease.release_lease_fenced(t.id, "w1", 2) is False, "旧令牌不能退别人的租约"
    assert await lease.release_lease_fenced(t.id, "w2", 3) is False
    assert await lease.release_lease_fenced(t.id, "w1", 3) is True
    r = await fresh(t)
    assert (r.status, r.worker_id, r.lease_until, r.lease_seq, r.requeues) == (TaskStatus.PENDING, "", None, 4, 4)
    assert await lease.release_lease_fenced(t.id, "w1", 3) is False, "二次归还不命中"


# ==================== fencing ====================


async def test_check_fence_distinguishes_the_three_rejections(pgdb):
    cur = await lessee(pgdb)
    await lease.check_fence(cur.id, "w1", 3)  # 当前租户通过

    for worker, seq, reason in [
        ("w1", 2, lease.REASON_NOT_LESSEE),     # 旧一代
        ("w2", 3, lease.REASON_NOT_LESSEE),     # 别的机器
        ("w1", None, lease.REASON_BAD_SEQ),     # 带了 worker 却没 seq：响亮拒绝
        ("", None, lease.REASON_NOT_LESSEE),    # 无令牌：不再有"遗留任务放行"
    ]:
        with pytest.raises(lease.FenceReject) as ei:
            await lease.check_fence(cur.id, worker, seq)
        assert ei.value.reason == reason, (worker, seq)

    with pytest.raises(lease.FenceReject) as ei:
        await lease.check_fence("nope", "w1", 3)
    assert ei.value.reason == lease.REASON_GONE
    deleted = await lessee(pgdb, is_delete=True)
    with pytest.raises(lease.FenceReject) as ei:
        await lease.check_fence(deleted.id, "w1", 3)
    assert ei.value.reason == lease.REASON_GONE


async def test_returned_to_pool_task_rejects_the_previous_lessees_callbacks(pgdb):
    """踩过的坑：任务回池后 worker_id 被清空，曾经"行上无租户就放行"，上一轮的在途回调会写进下一轮。"""
    t = await lessee(pgdb)
    await lease.release_lease_fenced(t.id, "w1", 3)
    with pytest.raises(lease.FenceReject) as ei:
        await lease.check_fence(t.id, "w1", 3)
    assert ei.value.reason == lease.REASON_NOT_LESSEE


async def test_same_worker_on_a_finished_task_is_an_idempotent_success(pgdb):
    done = await lessee(pgdb, status=TaskStatus.COMPLETED)
    await Task.filter(id=done.id).update(lease_seq=9)  # 令牌已不是最新，但同一台机器、已达终态
    with pytest.raises(lease.FenceReject) as ei:
        await lease.check_fence(done.id, "w1", 3)
    assert ei.value.reason == lease.REASON_ALREADY_FINAL


class FakeRequest:
    def __init__(self, headers=None, body=None, form=None, json_raises=False):
        self.headers = headers or {}
        self._body, self._form, self._raises = body, form, json_raises

    @property
    def json(self):
        if self._raises:
            raise ValueError("not json")
        return self._body

    @property
    def form(self):
        return self._form


class FormDict(dict):
    def get(self, k, default=None):  # Sanic 的 form 是 multi-dict：取值拿首项
        v = super().get(k, default)
        return v[0] if isinstance(v, list) else v


def test_extract_fencing_channel_priority_and_robustness():
    ex = lease.extract_fencing
    assert ex(FakeRequest({"X-Worker-Id": " w1 ", "X-Lease-Seq": "7"})) == ("w1", 7)
    assert ex(FakeRequest(body={"worker_id": "w2", "lease_seq": 8})) == ("w2", 8), "第二通道：body"
    form = FormDict(worker_id=["w3"], lease_seq=["9"])
    assert ex(FakeRequest(form=form)) == ("w3", 9), "第三通道：form"
    # header 与 body 都带且不一致：header 赢（要有确定的答案）
    assert ex(FakeRequest({"X-Worker-Id": "h", "X-Lease-Seq": "1"}, body={"worker_id": "b", "lease_seq": 2})) == ("h", 1)
    # header 只带一半时，缺的那半从 body 补
    assert ex(FakeRequest({"X-Worker-Id": "h"}, body={"lease_seq": 5})) == ("h", 5)
    # 畸形 = 没有令牌；解析永不抛异常
    assert ex(FakeRequest({"X-Worker-Id": "w", "X-Lease-Seq": "abc"})) == ("w", None)
    assert ex(FakeRequest(json_raises=True)) == ("", None)
    assert ex(FakeRequest(body=["not", "a", "dict"])) == ("", None)


# ==================== 认领热点索引 ====================


async def test_ensure_claim_index_is_idempotent_and_repairs_a_dropped_index(pgdb):
    assert await lease.ensure_claim_index() == "已存在"
    await pgdb.execute_query(f'DROP INDEX "{Task.CLAIM_INDEX_NAME}"')
    assert await lease.ensure_claim_index() == "已创建", "dbsync --force 把它当野索引清掉后，启动时要能补回来"
    assert await lease.ensure_claim_index() == "已存在"
    cols = await pgdb.execute_query_dict(
        "SELECT indexdef FROM pg_indexes WHERE indexname = $1", [Task.CLAIM_INDEX_NAME]
    )
    assert "level DESC" in cols[0]["indexdef"] and "compute_points DESC" in cols[0]["indexdef"]
