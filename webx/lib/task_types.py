"""任务类型注册表与阶段配置。

任务模块本身不知道"有哪些任务"。每种任务类型通过 ``TaskTypeSpec`` **显式登记**自己的：
阶段配置（单阶段 / 多阶段状态机）、参数校验、点数估算、超时派生、提交前置闸……
框架里所有"按任务类型分支"的地方一律查这张表，**没有任何类型名被写死**。

    from webx.lib.task_types import TaskStageConfig, TaskTypeSpec, register_task_type

    register_task_type(TaskTypeSpec(
        name="render",
        stage=TaskStageConfig(
            end_step="encode",
            step_transitions={"": {"prepare"}, "prepare": {"encode"}, "encode": set()},
        ),
        validate_params=check_render_params,
    ))

**显式登记，不用装饰器自注册**：装饰器靠 import 副作用生效，漏 import 一个模块就悄悄少一种类型，
而且谁先谁后取决于 import 顺序。显式的 ``register_task_type(...)`` 调用写在业务入口里一目了然。

未登记的类型按**默认单阶段**处理（宽松，便于起步）；要求"必须先登记才能建任务"时设
``TASK_REQUIRE_REGISTERED_TYPES=true``。

步骤推进采用"状态机"语义而非严格线性序列：每个步骤声明其允许的下一步集合（``step_transitions``），
覆盖跨阶段跳跃、重做等场景。**通用不变量**：``end_step`` 不得有出边——最终步骤在一条轨迹里只能出现一次。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

# steps（已完成步骤轨迹）最大长度的默认值。真正生效的是每个任务类型在
# TaskStageConfig.max_steps 上的声明——合理步数是任务类型的属性，不该全局统一。
DEFAULT_MAX_STEPS = 12

#: 任何派生出来的超时都不得超过它（30 天）。既防手滑算出荒谬值撑爆
#: timeout_seconds 的 int4 列，也给卡死任务兜一个最坏占用时长。
MAX_STEP_TIMEOUT = 30 * 24 * 3600

#: 单元放大的计数上限——防御性上限。超时派生不能假设参数闸一定跑过（手工投递、历史数据）。
MAX_UNIT_COUNT = 5000


@dataclass(frozen=True)
class TaskStageConfig:
    """一种任务类型的阶段配置。"""

    #: 最终步骤名，""=单阶段任务（completed 即终态）；非空=多阶段，仅 end_step 完成时 completed 才是终态
    end_step: str = ""
    #: 默认超时（秒），48h。超时是"本步上限"、防卡死兜底：宁可宽松——误杀正常任务的代价远高于让卡死任务多占一会
    default_timeout: int = 172800
    #: 已完成步骤轨迹的最大长度，防止状态机允许的循环步让单个 Task 体积失控
    max_steps: int = DEFAULT_MAX_STEPS
    #: 状态机：当前最后一步 → 允许的下一步集合。key "" 表示初始（steps == []）。空 dict = 单阶段
    step_transitions: dict[str, frozenset[str]] = field(default_factory=dict)
    #: 需"子任务聚合"的步骤：进入这些步骤前，父任务的全部子任务必须已完成。配置驱动，不写死步骤名
    aggregate_steps: frozenset[str] = field(default_factory=frozenset)
    #: 按步骤差异化的超时档（秒）。未命中的步骤回落 default_timeout，只需列耗时显著偏离的几步
    timeout_by_step: dict[str, int] = field(default_factory=dict)
    #: 一次任务内"串行重复执行"的单元数放大超时：每多一个单元追加的秒数（相加不是相乘，首个单元的固定开销只付一次）。0=不放大
    timeout_per_unit: int = 0
    #: 单元放大后的超时天花板（秒）；0=只受全局 MAX_STEP_TIMEOUT 约束
    unit_timeout_cap: int = 0

    def is_multi_stage(self) -> bool:
        return self.end_step != ""

    def __post_init__(self) -> None:
        """配置自检：**end_step 不得有出边**。

        终点可再走一步，会同时打破三件事：① "任务是否结束"无从判定；② 该步用裸名 key，
        第二次出现会撞同一个参数块 / result key / 日志 key，后者静默覆盖前者；
        ③ 重置类入口按 key 裁剪时无法区分两次出现，只能连坐。
        所以这是配置错误而非运行期分支——在登记期直接炸，别等到线上。
        要"再跑一段"请用 redo 截断轨迹重跑该步，而不是追加第二个终点。
        """
        if self.end_step and self.step_transitions.get(self.end_step):
            raise ValueError(
                f"配置错误：end_step '{self.end_step}' 不得有出边，"
                f"实际声明了 {sorted(self.step_transitions[self.end_step])}。"
                f"最终步骤在一条轨迹里只能出现一次。"
            )

    @property
    def step_names(self) -> frozenset[str]:
        """所有合法步骤名（key 与 value 的并集，扣掉初始 key ""）。"""
        names: set[str] = set()
        for src, targets in self.step_transitions.items():
            if src:
                names.add(src)
            names.update(targets)
        return frozenset(names)

    def is_valid_step(self, step: str) -> bool:
        return step in self.step_names


@dataclass
class CostContext:
    """传给点数估算器的上下文：任务当前状态 + 用户档位 / 余额（均已解析好）。"""

    task: Any  # 任务模型实例；这里不 import 模型，保持本模块零 ORM
    level: int = 0
    balance: Optional[int] = None  # None = 不校验充足性


@dataclass
class TaskTypeSpec:
    """一种任务类型的全部扩展点。字段全部可选——只填需要的。"""

    name: str
    #: 阶段配置；默认单阶段
    stage: TaskStageConfig = field(default_factory=TaskStageConfig)
    #: 参数校验：入参为解析后的 params dict，不合法时抛 ``ParamsError``（见 webx.lib.params_gate）
    validate_params: Optional[Callable[[dict], None]] = None
    #: 点数估算：``(CostContext) -> CostEstimate``。不登记 = 免费（估算为 0）
    estimate_cost: Optional[Callable[[CostContext], Any]] = None
    #: 本次任务内串行重复的单元数，用于超时放大：``(params: dict, files: list) -> int``
    unit_count: Optional[Callable[[dict, list], int]] = None
    #: 完全接管超时派生：``(stage_cfg, sub_step, params, files) -> int``。不登记走通用派生
    resolve_timeout: Optional[Callable[..., int]] = None
    #: 提交前置闸（异步，超限就抛业务异常）：``(task) -> None``。在状态翻转之前调用，失败不留 PENDING 孤儿
    precheck_submit: Optional[Callable[[Any], Awaitable[None]]] = None
    #: 提交时挂起钩子（异步）：``(task) -> bool``。返回 True = 钩子已接管（典型：先建一批子任务，
    #: 把本任务挂起为 DRAFT + auto_post=1，等子任务算完再由补偿器的准入巡检投递），提交流程到此结束
    hold_submit: Optional[Callable[[Any], Awaitable[bool]]] = None
    #: 进入聚合步骤前**应当存在**的子任务 tag 列表：``(task, step) -> list[str]``。
    #: 不登记 = 只检查已存在的子任务是否都完成；登记后，缺失的子任务也算"未就绪"
    expected_child_tags: Optional[Callable[[Any, str], list[str]]] = None
    #: 解析 retry / redo 传入的步骤令牌：``(steps_done, token) -> (裸步骤名, 截断索引|None)``。
    #: 不登记 = 令牌就是步骤名，截断点取该步骤在轨迹中**最近一次**出现的位置。
    #: 步骤可重复出现且需要消歧（如带序号的 "nvt_2"）的类型自己登记
    resolve_step_token: Optional[Callable[[list[str], str], tuple[str, Optional[int]]]] = None
    #: 把调用方传入的某一步参数归一化后并入已有参数：``(steps_done, step, incoming) -> dict``。不登记 = 原样并入
    normalize_params: Optional[Callable[[list[str], str, dict], dict]] = None
    #: 重置 / 重试后裁剪 result_path：``(result_path, kept_steps) -> (新值, 孤儿 URL 列表)``。
    #: 不登记走通用裁剪（见 webx.lib.task_results.prune_result_path）
    prune_result_path: Optional[Callable[[str, list[str]], tuple[str, list[str]]]] = None
    description: str = ""


# ==================== 注册表 ====================

_REGISTRY: dict[str, TaskTypeSpec] = {}
_DEFAULT_SPEC = TaskTypeSpec(name="")


def register_task_type(spec: TaskTypeSpec, *, replace: bool = False) -> TaskTypeSpec:
    """登记一种任务类型。同名重复登记默认报错（两处各登记一份必然有一份是过期的）。"""
    if not spec.name or not spec.name.strip():
        raise ValueError("任务类型名不能为空")
    if len(spec.name) > 64:
        raise ValueError(f"任务类型名过长（>64）: {spec.name!r}")
    if spec.name in _REGISTRY and not replace:
        raise ValueError(f"任务类型 {spec.name!r} 已登记；确需覆盖请传 replace=True")
    _REGISTRY[spec.name] = spec
    return spec


def unregister_task_type(name: str) -> None:
    """注销（测试用，或运行时下线某类型）。"""
    _REGISTRY.pop(name, None)


def registered_task_types() -> list[str]:
    return sorted(_REGISTRY)


def is_registered(name: str) -> bool:
    return name in _REGISTRY


def get_task_type(name: str) -> TaskTypeSpec:
    """取登记的类型；未登记返回**默认单阶段**的空 spec（不抛异常）。"""
    return _REGISTRY.get(name, _DEFAULT_SPEC)


def get_stage_config(task_type: str) -> TaskStageConfig:
    return get_task_type(task_type).stage


def requires_children_complete(task_type: str, step: str) -> bool:
    """该类型在进入指定 step 前，是否要求全部子任务先完成（配置驱动）。"""
    return step in get_stage_config(task_type).aggregate_steps


# ==================== 创建 / 参数入口校验 ====================


def check_task_type(name: str) -> TaskTypeSpec:
    """创建任务前校验类型名。

    严格模式（``TASK_REQUIRE_REGISTERED_TYPES=true``）下未登记 → ``UnknownTaskTypeError``(400)；
    宽松模式（默认）下未登记按默认单阶段放行。类型名为空 / 过长恒拒绝。
    """
    from webx.config import settings
    from webx.lib.task_errors import ParamsError, UnknownTaskTypeError

    if not name or not name.strip() or len(name) > 64:
        raise ParamsError("invalid_params", field="task_type")
    if settings.TASK_REQUIRE_REGISTERED_TYPES and name not in _REGISTRY:
        raise UnknownTaskTypeError(name)
    return get_task_type(name)


def validate_task_params(task_type: str, params: dict) -> None:
    """跑该类型登记的参数校验；没登记就放行。校验失败由校验函数抛 ``ParamsError``。"""
    validator = get_task_type(task_type).validate_params
    if validator is not None:
        validator(params)


# ==================== 步骤校验 ====================


class StepValidationError(ValueError):
    """步骤校验错误，携带 i18n 所需的 key 与上下文。

    code 取值：
      - ``invalid_step_name``  步骤名不在该任务类型的合法集合内
      - ``step_not_allowed``   步骤名合法，但从当前状态不允许迁移到它
      - ``max_steps_reached``  已完成轨迹长度已达上限，不允许再追加
    """

    def __init__(self, code: str, **context):
        self.code = code
        self.context = context
        super().__init__(f"{code}: {context}")


def validate_step_transition(task_type: str, steps_done: list[str], requested_step: str) -> None:
    """统一的"写入 sub_step"前置校验。

    所有把调用方传入的 step 写入 task.sub_step 的入口（PATCH / retry 等）都应通过它，
    避免硬编码任务专属步骤名。redo 不走转换表校验（它截断轨迹重跑，有独立校验）。

    规则：单阶段（step_transitions 为空）直接放行；多阶段须 ① 轨迹未达 max_steps，
    ② 步骤名合法，③ 当前最后一步允许迁移到它。
    """
    config = get_stage_config(task_type)
    if not config.is_multi_stage():
        return

    if len(steps_done) >= config.max_steps:
        raise StepValidationError("max_steps_reached", max=config.max_steps, current=len(steps_done))

    if not config.is_valid_step(requested_step):
        raise StepValidationError(
            "invalid_step_name",
            step=requested_step,
            task_type=task_type,
            allowed=", ".join(sorted(config.step_names)),
        )

    current = steps_done[-1] if steps_done else ""
    allowed = config.step_transitions.get(current, frozenset())
    if requested_step not in allowed:
        raise StepValidationError(
            "step_not_allowed",
            current=current or "(initial)",
            requested=requested_step,
            allowed=", ".join(sorted(allowed)) if allowed else "(none)",
        )


# ==================== 超时派生 ====================


def resolve_timeout(
    task_type: str,
    *,
    sub_step: str = "",
    params: Optional[dict] = None,
    files: Optional[list] = None,
) -> int:
    """按任务类型 + 当前步骤派生子进程超时（秒）。

    ⚠️ 超时的真实语义是**本次步骤的上限**而非整任务：worker 每推进一步即一次全新投递、一个全新子进程，
    每次从零独立计时。所以本函数须在**投递时**按当次 sub_step 现算，而非创建时定死。

    优先级：类型自带的 ``resolve_timeout`` 钩子（完全接管）→ 步骤档 → ``default_timeout``，
    再叠加串行单元放大。恒返回 ``(0, MAX_STEP_TIMEOUT]`` 内的合法值，对**任何**入参都不抛异常
    （参数是用户可控的，一个 JSON 里的 ``1e400`` 就能读成 inf）。
    """
    spec = get_task_type(task_type)
    config = spec.stage
    params = params or {}
    files = files or []

    if spec.resolve_timeout is not None:
        try:
            custom = int(spec.resolve_timeout(config, sub_step, params, files))
        except (TypeError, ValueError, OverflowError):
            custom = config.default_timeout
        return max(1, min(custom, MAX_STEP_TIMEOUT))

    base = config.default_timeout
    if sub_step:
        base = config.timeout_by_step.get(sub_step, base)

    if config.timeout_per_unit and spec.unit_count is not None:
        try:
            units = int(spec.unit_count(params, files))
        except (TypeError, ValueError, OverflowError):
            units = 1
        if units > 1:
            base += config.timeout_per_unit * (min(units, MAX_UNIT_COUNT) - 1)
            base = min(base, config.unit_timeout_cap or MAX_STEP_TIMEOUT)

    if not math.isfinite(base):
        base = config.default_timeout
    return max(1, min(int(base), MAX_STEP_TIMEOUT))
