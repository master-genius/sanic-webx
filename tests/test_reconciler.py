"""补偿器：租约回收、慢扫描（第二道网）、空任务清理、滞留告警、可注册的巡检钩子。

数据库行为在真实 PG 上跑（回收走的是 PG 专有 SQL）；poke 与存储客户端用记录器替换，
断言"该通知的路径确实通知了"。
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import pytest
from tortoise import Tortoise

from webx.models import SubStatus, Task, TaskStatus
from webx.services import reconciler as rec_mod
from webx.services.reconciler import Reconciler

pytestmark = pytest.mark.usefixtures("pgdb")

HOUR = 3600 * 1000
NOW = lambda: int(time.time() * 1000)  # noqa: E731


@pytest.fixture
def pokes(monkeypatch):
    calls = []

    async def fake_poke():
        calls.append(1)
        return True

    monkeypatch.setattr(rec_mod, "poke_task_server", fake_poke)
    return calls


@pytest.fixture
def purged(monkeypatch):
    ids = []
    monkeypatch.setattr(rec_mod.storage_client, "delete_task_background", lambda tid: ids.append(tid))
    return ids


@pytest.fixture(autouse=True)
def clean_hooks():
    saved = (list(rec_mod._SWEEPS), list(rec_mod._FAILED_HOOKS))
    yield
    rec_mod._SWEEPS[:], rec_mod._FAILED_HOOKS[:] = saved


def make() -> Reconciler:
    return Reconciler(stale_threshold_minutes=60, queued_timeout_minutes=60,
                      abandon_threshold_hours=168, none_task_timeout_hours=3)


async def mk(status=TaskStatus.RUNNING, **kw) -> Task:
    return await Task.create(task_type="demo", user_id="u1", status=status, **kw)


async def fresh(t: Task) -> Task:
    return await Task.get(id=t.id)


# ==================== 租约回收（秒级循环）====================


async def test_reclaim_once_returns_expired_leases_and_pokes(pgdb, pokes):
    ms = (await pgdb.execute_query_dict("SELECT (EXTRACT(EPOCH FROM now())*1000)::bigint AS m"))[0]["m"]
    t = await mk(worker_id="w1", lease_seq=1, lease_until=ms - 1000)
    await make()._reclaim_once()
    r = await fresh(t)
    assert (r.status, r.worker_id, r.lease_seq) == (TaskStatus.PENDING, "", 2)
    assert pokes, "回收也是'让任务进入 PENDING'的路径，规则要求 poke"


async def test_reclaim_once_with_nothing_to_do_does_not_poke(pokes):
    await make()._reclaim_once()
    assert not pokes


async def test_reclaim_once_fails_poison_tasks_and_notifies(pgdb, pokes):
    from webx.config import settings

    ms = (await pgdb.execute_query_dict("SELECT (EXTRACT(EPOCH FROM now())*1000)::bigint AS m"))[0]["m"]
    poison = await mk(worker_id="w1", lease_until=ms - 1000, requeues=settings.LEASE_REQUEUE_LIMIT)
    seen = []

    async def hook(ids, reason):
        seen.append((ids, reason))

    rec_mod.register_failed_hook(hook)
    await make()._reclaim_once()
    assert (await fresh(poison)).status == TaskStatus.FAILED
    assert seen and seen[0][0] == [poison.id] and "异常任务" in seen[0][1]


# ==================== 慢扫描 ====================


async def test_stale_running_task_goes_back_to_the_pool_without_charging_retry(pokes):
    t = await mk(last_heartbeat=NOW() - 2 * HOUR, worker_id="w1", lease_seq=4, retry=2)
    await make().scan_and_recover()
    r = await fresh(t)
    assert (r.status, r.worker_id, r.lease_seq) == (TaskStatus.PENDING, "", 5)
    assert r.retry == 2, "被动重投不蚕食用户的重试配额"
    assert pokes


async def test_fresh_heartbeat_is_left_alone(pokes):
    t = await mk(last_heartbeat=NOW() - 1000)
    await make().scan_and_recover()
    assert (await fresh(t)).status == TaskStatus.RUNNING and not pokes


@pytest.mark.parametrize(
    "kw",
    [
        dict(sub_status=SubStatus.SUCCESS, sub_step="", end_step="last"),  # 等用户追加参数——不是卡死
        dict(sub_status=SubStatus.DRAFT),
        dict(sub_status=SubStatus.FAILED),
    ],
    ids=["waiting-for-user", "draft", "failed-substep"],
)
async def test_tasks_that_are_legitimately_idle_are_not_reclaimed(kw):
    t = await mk(last_heartbeat=NOW() - 2 * HOUR, **kw)
    await make().scan_and_recover()
    assert (await fresh(t)).status == TaskStatus.RUNNING, "回收判据必须排除这些，否则会从健康 worker 手里抢任务"


async def test_abandoned_tasks_fail_instead_of_looping_forever(pokes):
    seen = []

    async def hook(ids, reason):
        seen.append(ids)

    rec_mod.register_failed_hook(hook)
    t = await mk(last_heartbeat=NOW() - 200 * HOUR)  # > 168h
    await make().scan_and_recover()
    r = await fresh(t)
    assert (r.status, r.sub_status) == (TaskStatus.FAILED, SubStatus.FAILED)
    assert "长时间无进展" in r.error_message and seen == [[t.id]]
    assert not pokes, "判失败的任务不回池，不需要 poke"


async def test_dirty_zero_heartbeat_is_backfilled_not_reclaimed():
    t = await mk(status=TaskStatus.RUNNING, last_heartbeat=0)
    q = await mk(status=TaskStatus.QUEUED, last_heartbeat=0)
    await make().scan_and_recover()
    for x in (t, q):
        r = await fresh(x)
        assert r.status == x.status and abs(r.last_heartbeat - NOW()) < 10_000


async def test_stale_queued_task_is_recovered_too(pokes):
    """QUEUED = 领了但没起跑：worker 崩了，或领取响应在途丢失。"""
    t = await mk(status=TaskStatus.QUEUED, last_heartbeat=NOW() - 2 * HOUR, worker_id="w1")
    await make().scan_and_recover()
    assert (await fresh(t)).status == TaskStatus.PENDING


async def test_a_task_changed_concurrently_is_skipped(pokes):
    """乐观锁：扫描读到后、回池前状态被改走（比如刚好完成了），就不动它。"""
    t = await mk(last_heartbeat=NOW() - 2 * HOUR)
    await Task.filter(id=t.id).update(status=TaskStatus.COMPLETED)
    assert await make()._recover_task(t) is False
    assert (await fresh(t)).status == TaskStatus.COMPLETED


# ==================== 空任务清理 / 滞留告警 ====================


async def test_stale_none_tasks_are_purged_but_drafts_never(purged):
    old_none = await mk(status=TaskStatus.NONE)
    new_none = await mk(status=TaskStatus.NONE)
    draft = await mk(status=TaskStatus.DRAFT)
    long_ago = "now() - interval '5 hours'"
    await Tortoise.get_connection("default").execute_query(
        f"UPDATE tasks SET updated_at = {long_ago} WHERE id IN ($1, $2)", [old_none.id, draft.id]
    )
    await make()._clean_stale_none_tasks()
    assert await Task.filter(id=old_none.id).count() == 0
    assert await Task.filter(id=new_none.id).count() == 1, "未到期的保留"
    assert await Task.filter(id=draft.id).count() == 1, "草稿永不自动清理"
    assert purged == [old_none.id], "远端产物也要删（本地与远端各删各的）"


async def test_stuck_pending_pool_is_reported_by_type(caplog):
    await Task.create(task_type="lonely", status=TaskStatus.PENDING, user_id="u")
    await Task.filter(task_type="lonely").update(submitted_at=None)
    await Tortoise.get_connection("default").execute_query(
        "UPDATE tasks SET submitted_at = now() - interval '3 hours' WHERE task_type = 'lonely'"
    )
    with caplog.at_level(logging.ERROR):
        await make()._warn_stuck_pending()
    assert "滞留" in caplog.text and "lonely" in caplog.text, "没有 worker 在线时必须响亮告警，而不是自动判失败"


async def test_young_pending_tasks_do_not_trigger_the_warning(caplog):
    t = await Task.create(task_type="demo", status=TaskStatus.PENDING, user_id="u")
    await Task.filter(id=t.id).update(submitted_at=datetime.now(timezone.utc))
    with caplog.at_level(logging.ERROR):
        await make()._warn_stuck_pending()
    assert "滞留" not in caplog.text


# ==================== 钩子 / 生命周期 / 配置 ====================


async def test_sweeps_are_isolated_from_each_other():
    ran = []

    async def boom():
        raise RuntimeError("one sweep dies")

    async def fine():
        ran.append("fine")

    rec_mod.register_sweep("a-boom", boom)
    rec_mod.register_sweep("b-fine", fine)
    await make()._run_sweeps()
    assert ran == ["fine"], "一侧异常不该让另一侧整轮不跑"


def test_registering_a_sweep_twice_replaces_it():
    async def v1(): ...
    async def v2(): ...

    rec_mod.register_sweep("same", v1)
    rec_mod.register_sweep("same", v2)
    assert [f for n, f in rec_mod._SWEEPS if n == "same"] == [v2]
    rec_mod.unregister_sweep("same")
    assert "same" not in [n for n, _ in rec_mod._SWEEPS]


async def test_lifecycle_runs_all_three_loops_and_stops_cleanly(pokes):
    ticks = []

    async def sweep():
        ticks.append(1)

    rec_mod.register_sweep("tick", sweep)
    r = Reconciler(scan_interval=1, admit_interval=1, lease_interval=1)
    await r.start(startup_delay=0)
    assert r.is_running
    await r.start(startup_delay=0)  # 重复启动被忽略，不会叠出第二套循环
    await asyncio.sleep(0.4)
    assert ticks, "准入循环在跑"
    await r.stop()
    assert not r.is_running
    n = len(ticks)
    await asyncio.sleep(0.3)
    assert len(ticks) == n, "停止后不再有任何循环在跑"


async def test_loop_survives_exceptions_in_its_body():
    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("first round blows up")

    r = Reconciler()
    r._running = True
    task = asyncio.create_task(r._loop(flaky, 0.05, 0))
    await asyncio.sleep(0.3)
    r._running = False
    task.cancel()
    assert len(calls) >= 2, "单轮异常不能让补偿器停摆"


def test_from_settings_precedence_overrides_over_config_over_default(monkeypatch):
    from webx.config import get_settings

    cfg = get_settings()
    monkeypatch.setattr(cfg, "RECONCILER_SCAN_INTERVAL", 321)
    monkeypatch.setattr(cfg, "RECONCILER_LEASE_INTERVAL", 7)
    r = Reconciler.from_settings(cfg, lease_interval=2, scan_interval=None)
    assert r.scan_interval == 321, "None 表示不覆盖，回退到配置"
    assert r.lease_interval == 2, "显式参数（命令行）优先于配置"
    assert Reconciler().abandon_threshold_ms == 168 * HOUR
