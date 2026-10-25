"""poke 客户端：通知领取服务"有新任务进入 PENDING"。

## 规则，不是三个补丁

**任何使任务进入 PENDING 的路径都要 poke**：提交、补偿器的租约回收、补偿器的子任务准入、
主动归还租约……写成规则而非给几处各打一个补丁，否则将来加第四个生产者必然漏。

## best-effort：失败只损失延迟，不损失正确性

任务已经持久在 PENDING，领取服务的兜底轮询也会发现它。poke 只是把发现延迟从"一个兜底间隔"
压到"近乎立刻"。分发的权威只有数据库，所以不存在"投递失败"这种故障，也就没有需要回滚的投递动作。

## 为什么不重试

真丢了有兜底轮询兜着，而重试只会在领取服务挂掉时把调用方的请求线程拖住。
**超时取得很短、异常一律吞掉**是刻意的。
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from webx.config import settings

logger = logging.getLogger(__name__)

# 很短：poke 是"说一声就走"，绝不能因领取服务无响应而拖慢用户的提交请求
_POKE_TIMEOUT_SEC = 2.0

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=_POKE_TIMEOUT_SEC)
    return _client


async def poke_task_server() -> bool:
    """通知领取服务有新任务。返回是否送达（调用方**不必**关心）。

    空 body：poke 不携带任何任务内容。领取服务醒来后一律回数据库认领——
    任务归属的权威在数据库，谁都不该在旁边留一份会过期的镜像。
    """
    url = f"{settings.TASK_SERVER_URL.rstrip('/')}/task/pokes"
    try:
        resp = await _get_client().post(url, json={})
        if resp.status_code == 200:
            return True
        logger.warning("[poke] 领取服务返回 %s（不影响正确性）", resp.status_code)
    except Exception as e:  # noqa: BLE001 —— 吞掉：兜底轮询会发现任务
        logger.warning("[poke] 送达失败（不影响正确性，兜底轮询兜住）：%s", e)
    return False


def poke_background() -> None:
    """即发即忘版本——连 2 秒都不想等的调用点用它（如批量 fan-out）。"""
    try:
        task = asyncio.create_task(poke_task_server())
        # 挂一个空回调，避免 "Task exception was never retrieved" 告警
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
    except RuntimeError:
        pass  # 无运行中的事件循环（同步上下文）：静默跳过，兜底轮询兜住


async def close_poke_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None
