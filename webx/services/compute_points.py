"""点数估算与可负担闸门。

本模块是"点数"域在服务层的唯一入口，只做两件事：

  1. 估算：``estimate_compute_points`` 返回统一的 ``CostEstimate``。具体怎么估由
     ``PricingPolicy`` / 各任务类型的 ``estimate_cost`` 决定（见 ``webx.lib.task_pricing``），
     本模块不含任何公式。
  2. 闸门：提交链路调用估算，不可执行时抛 ``QuotaExceededError``（复用配额错误通道 → HTTP 403），
     不另造异常类型。

**不含账务。** 余额、预扣、结算、退款不在这里——原项目也没有，凭空发明不如留白。
用户参数可选：传 user → 取其档位 / 余额；不传 → 仅估算消耗、不校验充足性（用于"提交前预估"这类只读场景）。
"""

from __future__ import annotations

from typing import Optional, Union

from webx.lib.task_errors import QuotaExceededError
from webx.lib.task_pricing import CostEstimate, get_pricing_policy
from webx.lib.task_types import CostContext
from webx.models.task import Task
from webx.services.quota import get_user_resolver

__all__ = ["CostEstimate", "estimate_compute_points", "ensure_affordable"]


async def estimate_compute_points(
    task: Union[Task, str], user_id: Optional[str] = None
) -> CostEstimate:
    """估算某任务**当前步骤**的点数消耗。

    Args:
        task: 任务对象或任务 id（传 id 则查库，不存在抛 ``ValueError``）。
        user_id: 传 → 取该用户的档位与余额；不传 → 按默认档估算且不校验充足性。
    """
    if isinstance(task, str):
        fetched = await Task.get_or_none(id=task)
        if fetched is None:
            raise ValueError(f"任务不存在: {task}")
        task = fetched

    level, balance = 0, None
    if user_id:
        resolver = get_user_resolver()
        level = await resolver.level(user_id)
        balance = await resolver.balance(user_id)

    return get_pricing_policy().estimate(CostContext(task=task, level=level, balance=balance))


async def ensure_affordable(task: Task, user_id: str) -> CostEstimate:
    """提交前置闸：估算为不可执行 → 抛 ``QuotaExceededError``（403）。返回估算结果供调用方放进响应。

    特权账号（``UserResolver.is_privileged``）整体豁免。应在状态翻转**之前**调用，
    超限不写库、不留 PENDING 孤儿。
    """
    if user_id and await get_user_resolver().is_privileged(user_id):
        return CostEstimate()
    est = await estimate_compute_points(task, user_id or None)
    if not est.affordable:
        raise QuotaExceededError(scope=est.scope, limit=est.limit, current=est.current)
    return est
