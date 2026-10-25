"""任务事件：服务层在关键迁移处发出，订阅方（通知、审计、AI 编排…）自行处理。

框架不认识任何订阅方。事件**尽力而为**：处理器抛的异常只记日志，绝不能反过来影响任务状态迁移——
迁移已经落库，事件只是"告诉别人发生了什么"。
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)

EVENT_COMPLETED = "completed"   # 任务完成（终态）
EVENT_FAILED = "failed"         # 任务失败（终态）
EVENT_CANCELLED = "cancelled"   # 任务被取消
EVENT_STEP_DONE = "step_done"   # 多阶段任务某一步完成，等待用户追加参数

Handler = Callable[[str, str, dict], Awaitable[None]]  # (task_id, event, payload)

_HANDLERS: list[Handler] = []


def on_task_event(fn: Handler) -> Handler:
    """订阅所有任务事件（可当装饰器用）。同一函数重复订阅只生效一次。"""
    if fn not in _HANDLERS:
        _HANDLERS.append(fn)
    return fn


def off_task_event(fn: Handler) -> None:
    if fn in _HANDLERS:
        _HANDLERS.remove(fn)


async def emit_task_event(task_id: str, event: str, **payload) -> None:
    for handler in list(_HANDLERS):
        try:
            await handler(task_id, event, payload)
        except Exception as e:  # noqa: BLE001 —— 订阅方的故障不得影响任务迁移
            logger.error("任务事件处理器异常 event=%s task=%s: %s", event, task_id, e, exc_info=True)
