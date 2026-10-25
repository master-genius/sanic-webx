"""任务核心的数据层：类型注册表 / 阶段状态机 / 超时派生 / 模型 / 配额 / 点数接缝。

不需要 PostgreSQL：这里没有任何 PG 专有 SQL（认领与租约的测试在 test_task_lease*.py，用真实 PG）。
"""

import pytest

from webx.lib import task_pricing
from webx.lib.task_errors import (
    ParamsError,
    QuotaExceededError,
    UnknownTaskTypeError,
)
from webx.lib.task_pricing import CostEstimate, DefaultPricingPolicy
from webx.lib.task_types import (
    MAX_STEP_TIMEOUT,
    StepValidationError,
    TaskStageConfig,
    TaskTypeSpec,
    check_task_type,
    get_stage_config,
    is_registered,
    register_task_type,
    registered_task_types,
    requires_children_complete,
    resolve_timeout,
    unregister_task_type,
    validate_step_transition,
    validate_task_params,
)
from webx.models import SubStatus, Task, TaskStatus, User


@pytest.fixture
def types():
    """隔离的注册表：用例里登记的类型在结束时全部注销。"""
    added: list[str] = []

    def add(spec: TaskTypeSpec, **kw):
        register_task_type(spec, **kw)
        added.append(spec.name)
        return spec

    yield add
    for name in added:
        unregister_task_type(name)


MULTI = TaskStageConfig(
    end_step="encode",
    step_transitions={"": {"prepare"}, "prepare": {"render", "encode"}, "render": {"render", "encode"}},
    max_steps=4,
    timeout_by_step={"render": 7200},
    aggregate_steps=frozenset({"encode"}),
)


# ==================== 注册表 ====================


def test_registry_basics(types):
    types(TaskTypeSpec(name="t_basic"))
    assert is_registered("t_basic") and "t_basic" in registered_task_types()
    with pytest.raises(ValueError, match="已登记"):
        register_task_type(TaskTypeSpec(name="t_basic"))
    register_task_type(TaskTypeSpec(name="t_basic", description="v2"), replace=True)
    assert not is_registered("t_never")


@pytest.mark.parametrize("bad", ["", "   ", "x" * 65])
def test_registry_rejects_bad_names(bad):
    with pytest.raises(ValueError):
        register_task_type(TaskTypeSpec(name=bad))


def test_unregistered_type_falls_back_to_single_stage():
    cfg = get_stage_config("never_registered")
    assert not cfg.is_multi_stage() and cfg.default_timeout == 172800


def test_strict_mode_rejects_unregistered_types(types, monkeypatch):
    from webx.config import get_settings

    check_task_type("whatever")  # 宽松（默认）：放行
    monkeypatch.setattr(get_settings(), "TASK_REQUIRE_REGISTERED_TYPES", True)
    types(TaskTypeSpec(name="t_known"))
    check_task_type("t_known")
    with pytest.raises(UnknownTaskTypeError) as ei:
        check_task_type("t_unknown")
    assert ei.value.status == 400
    for bad in ("", "  ", "x" * 65):  # 名字本身非法恒拒绝，与模式无关
        with pytest.raises(ParamsError):
            check_task_type(bad)


def test_validate_params_hook(types):
    def check(params):
        if params.get("n", 0) < 1:
            raise ParamsError("n_too_small", min=1)

    types(TaskTypeSpec(name="t_val", validate_params=check))
    validate_task_params("t_val", {"n": 2})
    with pytest.raises(ParamsError) as ei:
        validate_task_params("t_val", {"n": 0})
    assert ei.value.code == "n_too_small" and ei.value.status == 400
    validate_task_params("never_registered", {"anything": 1})  # 没登记就放行


# ==================== 阶段配置 / 步骤状态机 ====================


def test_end_step_must_not_have_outgoing_edges():
    with pytest.raises(ValueError, match="不得有出边"):
        TaskStageConfig(end_step="done", step_transitions={"": {"done"}, "done": {"again"}})


def test_step_names_are_the_union_of_keys_and_targets():
    assert MULTI.step_names == {"prepare", "render", "encode"}
    assert MULTI.is_valid_step("render") and not MULTI.is_valid_step("")


def test_single_stage_skips_step_validation(types):
    types(TaskTypeSpec(name="t_single"))
    validate_step_transition("t_single", ["x"] * 99, "anything")


def test_multi_stage_transitions(types):
    types(TaskTypeSpec(name="t_multi", stage=MULTI))
    validate_step_transition("t_multi", [], "prepare")
    validate_step_transition("t_multi", ["prepare"], "render")
    validate_step_transition("t_multi", ["prepare", "render"], "render")  # 允许的循环步

    with pytest.raises(StepValidationError) as e1:
        validate_step_transition("t_multi", [], "render")  # 初始只能 prepare
    assert e1.value.code == "step_not_allowed"
    with pytest.raises(StepValidationError) as e2:
        validate_step_transition("t_multi", ["prepare"], "bogus")
    assert e2.value.code == "invalid_step_name"
    with pytest.raises(StepValidationError) as e3:  # 轨迹已达 max_steps=4
        validate_step_transition("t_multi", ["prepare", "render", "render", "render"], "render")
    assert e3.value.code == "max_steps_reached"


def test_aggregate_steps_are_config_driven(types):
    types(TaskTypeSpec(name="t_multi", stage=MULTI))
    assert requires_children_complete("t_multi", "encode") is True
    assert requires_children_complete("t_multi", "render") is False
    assert requires_children_complete("never_registered", "encode") is False


# ==================== 超时派生 ====================


def test_timeout_default_and_step_tier(types):
    types(TaskTypeSpec(name="t_multi", stage=MULTI))
    assert resolve_timeout("t_multi") == 172800
    assert resolve_timeout("t_multi", sub_step="render") == 7200
    assert resolve_timeout("t_multi", sub_step="prepare") == 172800, "未命中的步骤回落默认"
    assert resolve_timeout("never_registered", sub_step="render") == 172800


def test_timeout_unit_amplification_adds_not_multiplies_and_caps(types):
    stage = TaskStageConfig(default_timeout=1000, timeout_per_unit=100, unit_timeout_cap=1500)
    types(TaskTypeSpec(name="t_unit", stage=stage, unit_count=lambda params, files: params["n"]))
    assert resolve_timeout("t_unit", params={"n": 1}) == 1000, "首个单元不放大"
    assert resolve_timeout("t_unit", params={"n": 4}) == 1300, "每多一个单元加 100，相加不是相乘"
    assert resolve_timeout("t_unit", params={"n": 50}) == 1500, "被类型自己的天花板截断"


def test_timeout_is_clamped_and_survives_hostile_input(types):
    stage = TaskStageConfig(default_timeout=10**12, timeout_per_unit=10**9)
    types(TaskTypeSpec(name="t_huge", stage=stage, unit_count=lambda p, f: p["n"]))
    assert resolve_timeout("t_huge", params={"n": 10**9}) == MAX_STEP_TIMEOUT

    # 参数用户可控：unit_count 钩子抛任何数值类异常都不能让超时派生 500
    types(TaskTypeSpec(name="t_bad", stage=TaskStageConfig(timeout_per_unit=5),
                       unit_count=lambda p, f: p["missing"] if False else float("inf")))
    assert resolve_timeout("t_bad", params={}) > 0


def test_timeout_hook_takes_over_completely(types):
    types(TaskTypeSpec(name="t_hook", resolve_timeout=lambda cfg, step, params, files: params["seconds"]))
    assert resolve_timeout("t_hook", params={"seconds": 42}) == 42
    assert resolve_timeout("t_hook", params={"seconds": 10**12}) == MAX_STEP_TIMEOUT
    assert resolve_timeout("t_hook", params={"seconds": "oops"}) == 172800, "钩子给出垃圾就回落默认档"


# ==================== 模型 / 状态机 ====================


async def test_task_defaults_and_transitions(db):
    t = await Task.create(task_type="demo")
    assert t.status == TaskStatus.NONE and t.lease_seq == 0 and t.worker_id == "" and t.lease_until is None

    await t.transition_to(TaskStatus.DRAFT)
    await t.transition_to(TaskStatus.PENDING)
    await t.transition_to(TaskStatus.QUEUED, worker_id="w1")
    await t.transition_to(TaskStatus.RUNNING)
    await t.transition_to(TaskStatus.COMPLETED, result_path="{}")
    fresh = await Task.get(id=t.id)
    assert fresh.status == TaskStatus.COMPLETED and fresh.worker_id == "w1" and fresh.result_path == "{}"

    for target in TaskStatus:  # 终态无出边
        assert not fresh.can_transition_to(target)
    with pytest.raises(ValueError, match="非法状态迁移"):
        await fresh.transition_to(TaskStatus.RUNNING)


@pytest.mark.parametrize(
    "origin, illegal",
    [
        (TaskStatus.NONE, TaskStatus.RUNNING),
        (TaskStatus.DRAFT, TaskStatus.RUNNING),
        (TaskStatus.PENDING, TaskStatus.COMPLETED),
        (TaskStatus.QUEUED, TaskStatus.COMPLETED),
    ],
)
async def test_illegal_transitions_are_rejected(db, origin, illegal):
    t = await Task.create(task_type="demo", status=origin)
    with pytest.raises(ValueError):
        await t.transition_to(illegal)
    assert (await Task.get(id=t.id)).status == origin, "拒绝后不得改库"


def test_running_to_queued_exists_only_for_the_reconciler():
    """running → queued 在迁移表里，但只供补偿器经 filter().update() 使用。"""
    assert Task.VALID_TRANSITIONS[TaskStatus.RUNNING] >= {TaskStatus.QUEUED}


def test_claim_index_ddl_is_explicit_and_mixed_direction():
    ddl = Task.CLAIM_INDEX_DDL
    assert Task.CLAIM_INDEX_NAME == "idx_tasks_claim" and not Task.CLAIM_INDEX_NAME.endswith("_idx")
    assert '"level" DESC' in ddl and '"compute_points" DESC' in ddl and '"rand_rank",' in ddl


# ==================== 配额 ====================


async def _task(user_id, *, status=TaskStatus.PENDING, end_step="", sub_status=SubStatus.PENDING,
                parent_id="", **kw):
    return await Task.create(task_type="demo", user_id=user_id, status=status, end_step=end_step,
                             sub_status=sub_status, parent_id=parent_id, **kw)


async def test_active_filter_and_python_predicate_agree_on_every_combination(db):
    """DB 侧过滤与内存谓词必须逐种组合同口径——它们是同一规则的两种写法。"""
    from webx.services.quota import count_active_tasks, is_task_active

    expected = 0
    for status in TaskStatus:
        for end_step in ("", "last"):
            for sub in SubStatus:
                t = await _task("u1", status=status, end_step=end_step, sub_status=sub)
                expected += is_task_active(t)
    assert await count_active_tasks("u1") == expected
    assert expected > 0


async def test_draft_with_default_pending_substatus_does_not_occupy_quota(db):
    """踩过的坑：草稿的 sub_status 默认就是 pending，不叠加 status 闸会被误计入。"""
    from webx.services.quota import count_active_tasks

    await _task("u2", status=TaskStatus.DRAFT, end_step="last", sub_status=SubStatus.PENDING)
    await _task("u2", status=TaskStatus.RUNNING, end_step="last", sub_status=SubStatus.SUCCESS)  # 等用户
    assert await count_active_tasks("u2") == 0


async def test_children_have_their_own_pool(db):
    from webx.services.quota import count_active_children, count_active_tasks

    parent = await _task("u3")
    await _task("u3", parent_id=parent.id)
    await _task("u3", parent_id=parent.id, is_delete=True)  # 软删不计
    assert await count_active_tasks("u3") == 1, "顶层计数排除子任务"
    assert await count_active_children("u3") == 1


async def test_concurrency_gate_by_level_and_exclude_self(db):
    from webx.services.quota import ensure_within_concurrency

    await User.create(id="lv0", username="lv0", level=0)
    await User.create(id="lv1", username="lv1", level=1)

    first = await _task("lv0")
    with pytest.raises(QuotaExceededError) as ei:  # level 0 上限 1
        await ensure_within_concurrency("lv0")
    assert (ei.value.scope, ei.value.limit, ei.value.current) == ("concurrency", 1, 1)
    await ensure_within_concurrency("lv0", exclude_task_id=first.id)  # 推进自身不被自身误伤

    for _ in range(2):
        await _task("lv1")
    await ensure_within_concurrency("lv1")  # level 1 上限 3，现有 2
    await _task("lv1")
    with pytest.raises(QuotaExceededError):
        await ensure_within_concurrency("lv1")

    await ensure_within_concurrency("")  # 匿名 / 内部放行
    await ensure_within_concurrency("ghost-user")  # 用户不存在按 0 档，且没有在飞任务


async def test_daily_create_gate(db):
    from webx.services import quota

    class Tiny(quota.DefaultQuotaPolicy):
        default_daily_create = 2

    quota.set_quota_policy(Tiny())
    try:
        await quota.ensure_within_daily_create("d1")
        await _task("d1", status=TaskStatus.COMPLETED)
        await _task("d1", status=TaskStatus.FAILED)  # 任意状态都计入创建数
        with pytest.raises(QuotaExceededError) as ei:
            await quota.ensure_within_daily_create("d1")
        assert ei.value.scope == "daily_create" and ei.value.limit == 2
        await quota.ensure_within_daily_create("someone-else")
    finally:
        quota.set_quota_policy(quota.DefaultQuotaPolicy())


async def test_waiting_parents_gate_and_clamping(db, monkeypatch, caplog):
    from webx.config import get_settings
    from webx.services import quota

    monkeypatch.setattr(get_settings(), "TASK_WAITING_PARENT_MAX", 2)
    for _ in range(2):
        await _task("w1", status=TaskStatus.DRAFT, auto_post=1)
    await _task("w1", status=TaskStatus.DRAFT, auto_post=0)           # 普通草稿不算
    await _task("w1", status=TaskStatus.DRAFT, auto_post=1, is_delete=True)  # 软删不算

    with pytest.raises(QuotaExceededError) as ei:
        await quota.ensure_within_waiting_parents("w1")
    assert ei.value.scope == "waiting_children" and ei.value.current == 2
    await quota.ensure_within_waiting_parents("someone-else")

    mine = await Task.filter(user_id="w1", auto_post=1, is_delete=False).first()
    await quota.ensure_within_waiting_parents("w1", exclude_task_id=mine.id)  # 排除自身后 1 < 2

    monkeypatch.setattr(get_settings(), "TASK_WAITING_PARENT_MAX", 0)
    await quota.ensure_within_waiting_parents("w1")  # 0 = 不限

    monkeypatch.setattr(get_settings(), "TASK_WAITING_PARENT_MAX", 10**6)
    assert quota.waiting_parent_limit() == 100 and "超出有效区间" in caplog.text


async def test_user_resolver_is_replaceable(db):
    from webx.services import quota

    class Fixed:
        async def level(self, user_id): return 2
        async def is_privileged(self, user_id): return True
        async def balance(self, user_id): return 77

    quota.set_user_resolver(Fixed())
    try:
        for _ in range(4):
            await _task("any-user")
        await quota.ensure_within_concurrency("any-user")  # level 2 → 上限 5，现有 4
    finally:
        quota.set_user_resolver(quota.DefaultUserResolver())


async def test_default_resolver_reads_the_user_table(db):
    from webx.services.quota import DefaultUserResolver

    await User.create(id="r1", username="r1", level=3, role=" Inner ")
    await User.create(id="r2", username="r2", level=0, role="user")
    r = DefaultUserResolver()
    assert await r.level("r1") == 3 and await r.is_privileged("r1") is True
    assert await r.level("r2") == 0 and await r.is_privileged("r2") is False
    assert await r.level("") == 0 and await r.is_privileged("") is False
    assert await r.balance("r1") is None, "webx 的 User 没有算力点字段，默认不校验充足性"


# ==================== 点数接缝 ====================


async def test_default_pricing_is_free(db):
    from webx.services.compute_points import ensure_affordable, estimate_compute_points

    t = await Task.create(task_type="demo", user_id="p1")
    est = await estimate_compute_points(t)
    assert est == CostEstimate(affordable=True, consumed_points=0, user_remaining=None)
    assert (await ensure_affordable(t, "p1")).affordable


async def test_per_type_estimator_and_the_gate(db, types):
    from webx.lib.task_types import CostContext
    from webx.services.compute_points import ensure_affordable, estimate_compute_points

    def estimator(ctx: CostContext) -> CostEstimate:
        n = len(ctx.task.name)
        if n > 5:
            return CostEstimate(affordable=False, scope="name_length", limit=5, current=n)
        return CostEstimate(consumed_points=n, user_remaining=ctx.balance)

    types(TaskTypeSpec(name="t_cost", estimate_cost=estimator))
    ok = await Task.create(task_type="t_cost", name="abc", user_id="p2")
    bad = await Task.create(task_type="t_cost", name="abcdefgh", user_id="p2")

    assert (await estimate_compute_points(ok)).consumed_points == 3
    assert (await estimate_compute_points(ok.id)).consumed_points == 3, "传 id 则自动查库"
    with pytest.raises(ValueError, match="任务不存在"):
        await estimate_compute_points("nope")

    await ensure_affordable(ok, "p2")
    with pytest.raises(QuotaExceededError) as ei:
        await ensure_affordable(bad, "p2")
    assert (ei.value.scope, ei.value.limit, ei.value.current) == ("name_length", 5, 8)
    assert ei.value.status == 403


async def test_privileged_users_skip_the_estimate_gate(db, types):
    from webx.services import quota
    from webx.services.compute_points import ensure_affordable

    types(TaskTypeSpec(name="t_deny", estimate_cost=lambda ctx: CostEstimate(affordable=False, scope="always")))
    t = await Task.create(task_type="t_deny", user_id="boss")
    await User.create(id="boss", username="boss", role="inner")
    await ensure_affordable(t, "boss")  # inner 豁免
    await User.filter(id="boss").update(role="user")
    with pytest.raises(QuotaExceededError):
        await ensure_affordable(t, "boss")
    assert quota.PRIVILEGED_ROLE == "inner"


async def test_whole_pricing_policy_can_be_replaced(db):
    from webx.services.compute_points import estimate_compute_points

    class Flat:
        def estimate(self, ctx):
            return CostEstimate(consumed_points=9, user_remaining=ctx.balance)

    task_pricing.set_pricing_policy(Flat())
    try:
        t = await Task.create(task_type="demo")
        assert (await estimate_compute_points(t)).consumed_points == 9
    finally:
        task_pricing.reset_pricing_policy()
    assert isinstance(task_pricing.get_pricing_policy(), DefaultPricingPolicy)


# ==================== 错误 → 响应 ====================


def test_quota_error_renders_localized_message_and_carries_machine_readable_fields():
    err = QuotaExceededError(scope="concurrency", limit=3, current=3)
    zh = err.to_dict("zh")
    assert zh["code"] == "quota_exceeded" and zh["scope"] == "concurrency"
    assert zh["limit"] == 3 and zh["current"] == 3 and "上限 3" in zh["error"]
    assert "limit 3" in err.to_dict("en")["error"]
    assert err.status == 403
