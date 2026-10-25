"""
进度回报薄封装 — 供任务执行脚本在子进程中调用。

设计原则：
  - 只做一件事：把 (task_id, sub_step, message) 写入云端 task.step_message
  - 所有异常静默吞掉（网络故障 / 服务端 400 守门拒绝 / 任务已迁出 RUNNING）
    都不影响主流程；最多 logger.debug 留痕
  - 服务端做 sub_step 一致性守门：sub_step 与 task.sub_step 不匹配会被拒绝
"""

from __future__ import annotations

import logging
from typing import Optional

from webx_worker.sdk.result_client import (
    _PROGRESS_MAX_ATTEMPTS,
    _PROGRESS_TIMEOUT_SEC,
    update_step_status,
)

logger = logging.getLogger(__name__)


def report_step_message(
    task_id: Optional[str],
    sub_step: Optional[str],
    message: str,
) -> None:
    """上报当前步骤的动态进度消息（最佳努力，永不抛出）。

    Args:
        task_id: 任务 ID（缺失则直接返回，不构成致命错误）
        sub_step: 当前子步骤名，必须与服务端 task.sub_step 一致。
            **空串是合法值，不是"没有"**：单阶段任务的 task.sub_step 恒为空串，
            服务端守门比的是"相等"，空串对空串正好通过。若在这里用 ``not sub_step``
            早退，单阶段任务的进度会**全部丢在客户端**。
            真正的"没有"用 None 表示，那才该早退。
        message: 显示给前端的进度文本（**英文**，见下）

    文案契约（前端直接展示，worker 不做 i18n）：
      - **一律英文**：前端有语言切换，worker 侧混中文会导致显示不一致；而让 worker 承担
        中英文切换是错误的职责划分（它不知道用户语言）。故统一英文短句。
      - **不出现具体软件名**：底层实现会换，进度文案是**给用户看"在做什么"**，
        不是给运维看"用了什么"。写 "Packing items"，不写 "Running <某程序>"。

    重试语义（**同步阻塞调用，但绝不为一条提示拖住计算**）：
      短超时 + 2 次即弃，且**不入 IPC 队列**——进度消息过期即无价值，被父进程异步补发只会
      覆盖掉更新的进度。最坏阻塞约 16s（旧实现是 5×30s + 退避 ≈ 153s）。
    """
    if not task_id or sub_step is None:
        return
    try:
        update_step_status(
            task_id=task_id,
            sub_step=sub_step,
            step_message=message,
            max_attempts=_PROGRESS_MAX_ATTEMPTS,
            timeout=_PROGRESS_TIMEOUT_SEC,
            enqueue_on_failure=False,
        )
    except Exception as e:
        # 网络异常 / 服务端守门拒绝 / 任务已离开 RUNNING 都不影响主流程
        logger.debug(
            f"step_message 上报失败（已忽略）: task_id={task_id}, "
            f"sub_step={sub_step}, error={e}"
        )
