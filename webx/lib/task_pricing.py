"""点数（compute points）估算的结构与接缝。

**现状：这里只有"估算 + 可负担闸门"，没有账务。** 余额、预扣、结算、退款这套状态流
在原项目里并不存在（``Task.compute_points`` 恒为 0，只当认领排序键；流水表没有写入者），
所以这里**不凭空发明**——只留好接口，把计价这件事留给将来的策略：

  - ``CostEstimate``：估算结果的统一结构，"提交前预估"与"可负担闸门"共用
  - ``PricingPolicy``：整个计价策略的替换点（要上线真实计费，实现它并 ``set_pricing_policy``）
  - 每种任务类型可在 ``TaskTypeSpec.estimate_cost`` 里登记自己的估算器；不登记 = 免费

默认策略 ``DefaultPricingPolicy`` 只做一件事：把估算委托给任务类型自己的估算器，没有就返回"免费可执行"。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Protocol

from webx.lib.task_types import CostContext, get_task_type


@dataclass(frozen=True)
class CostEstimate:
    """一次估算的结果。

    Attributes:
        affordable:      是否可执行（闸门据此放行 / 拒绝）
        consumed_points: 本次预估消耗的点数（免费档占位 0）
        user_remaining:  用户剩余点数；未传用户 / 不校验充足性 → None
        scope:           不可执行的原因归类（i18n 与统计口径用；affordable=False 时有效）
        limit:           该 scope 的上限（单位随 scope 而定）
        current:         本次估算值（单位随 scope 而定）
        detail:          估算明细，前端展示 / 调试用
    """

    affordable: bool = True
    consumed_points: int = 0
    user_remaining: Optional[int] = None
    scope: str = ""
    limit: float = 0.0
    current: float = 0.0
    detail: dict = field(default_factory=dict)


class PricingPolicy(Protocol):
    """计价策略。实现它并 ``set_pricing_policy`` 即可接管全部估算。"""

    def estimate(self, ctx: CostContext) -> CostEstimate: ...


class DefaultPricingPolicy:
    """默认策略：交给任务类型自己的估算器；没登记就是免费。"""

    def estimate(self, ctx: CostContext) -> CostEstimate:
        spec = get_task_type(ctx.task.task_type)
        if spec.estimate_cost is None:
            return CostEstimate(user_remaining=ctx.balance)
        return spec.estimate_cost(ctx)


_policy: PricingPolicy = DefaultPricingPolicy()


def set_pricing_policy(policy: PricingPolicy) -> None:
    """替换全局计价策略（进程级；在业务入口调用一次）。"""
    global _policy
    _policy = policy


def get_pricing_policy() -> PricingPolicy:
    return _policy


def reset_pricing_policy() -> None:
    """还原为默认策略（测试用）。"""
    set_pricing_policy(DefaultPricingPolicy())
