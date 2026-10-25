"""任务模块的业务异常。

都是 ``WebxError`` 子类：自带 HTTP 状态码与 i18n key，抛出即可，由 ``core.errors`` 里的
统一处理器翻成响应，不需要在应用入口为每种异常各挂一个处理器。
"""

from __future__ import annotations

from webx.lib.errors import WebxError


class QuotaExceededError(WebxError):
    """配额超限（并发名额 / 每日创建 / 估算闸共用）→ 403。

    ``scope`` 区分超限类型，供前端分支与统计：
      - ``concurrency``       并发名额超限
      - ``daily_create``      每日创建超限
      - ``waiting_children``  等待子任务的顶层任务数超限（**不是并发**：等待态不占 lane，这道闸管的是堆积）
    以及估算器 / 业务自定义的 scope。携带 ``limit`` / ``current`` 供生成用户可读提示。
    """

    status = 403
    code = "quota_exceeded"

    def __init__(self, scope: str, limit: float = 0, current: float = 0):
        self.scope = scope
        self.limit = limit
        self.current = current
        super().__init__(scope=scope, limit=limit, current=current)


class TaskNotFoundError(WebxError):
    status = 404
    code = "task_not_found"


class TaskStateConflictError(WebxError):
    """提交时任务状态已被并发请求抢先改变（乐观锁抢占失败）→ 400。

    触发点：提交的原子占位（``filter(id, status=origin).update(...)``）rowcount=0，
    说明在"读入 → 翻转"窗口内任务状态已被另一并发请求（典型：用户双击提交）改走，本次为重复投递。
    """

    status = 400
    code = "task_state_conflict"

    def __init__(self, task_id: str, origin: str):
        self.task_id = task_id
        self.origin = origin
        super().__init__(task_id=task_id, origin=origin)


class ChildrenNotReadyError(WebxError):
    """聚合步骤投递前仍有子任务未完成 → 409。携带未就绪 / 失败的子任务 tag 列表供前端提示。"""

    status = 409
    code = "children_not_ready"

    def __init__(self, step: str, pending: list[str], failed: list[str], total: int):
        self.step = step
        self.pending = pending
        self.failed = failed
        self.total = total
        super().__init__(step=step, pending=pending, failed=failed, total=total)


class ParamsError(WebxError):
    """任务参数不合法 → 400。``code`` 即 i18n key，``context`` 渲染文案占位符。

    任务类型的 ``validate_params`` 抛它::

        raise ParamsError("steps_out_of_range", min=1, max=100, got=n)
    """

    status = 400
    code = "invalid_params"

    def __init__(self, code: str = "invalid_params", **context):
        super().__init__(code=code, **context)


class UnknownTaskTypeError(WebxError):
    """任务类型未登记且配置要求必须登记（``TASK_REQUIRE_REGISTERED_TYPES=true``）→ 400。"""

    status = 400
    code = "unknown_task_type"

    def __init__(self, task_type: str):
        self.task_type = task_type
        super().__init__(task_type=task_type)
