"""独立存储服务的最小客户端（任务模块清理产物用）。

只做"删除任务产物"一件事——读写产物是 worker 与存储服务之间的事，不经过这里。

- 未配置 ``STORAGE_INTERNAL_URL`` → **静默跳过**远端删除（单机部署时本地目录就是同一棵树）。
- 失败重试一次；404 视为已定论（目标本来就不存在）。
- 删除接口是幂等的，所以"重复删"没有副作用，失败也只是残留，由存储侧的保留期清理兜底。
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from webx.config import settings

logger = logging.getLogger(__name__)

_TIMEOUT = 10.0


async def delete_task(task_id: str) -> bool:
    """删除某任务在存储服务上的全部产物（``DELETE /internal/tasks/<id>``）。

    Returns:
        True=已确认（含目标本就不存在）；False=未配置或失败（调用方不必处理）。
    """
    base = settings.STORAGE_INTERNAL_URL.strip().rstrip("/")
    if not base:
        return False
    url = f"{base}/internal/tasks/{task_id}"
    headers = {"X-Internal-Token": settings.STORAGE_INTERNAL_TOKEN}
    for attempt in (1, 2):
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.delete(url, headers=headers)
            if resp.status_code in (200, 404):
                return True
            logger.warning("[storage] 删除任务产物被拒 task_id=%s status=%s", task_id, resp.status_code)
            if resp.status_code in (401, 403):
                return False  # 令牌问题，重试无益
        except httpx.HTTPError as e:
            logger.warning("[storage] 删除任务产物失败（第 %d 次）task_id=%s: %s", attempt, task_id, e)
        if attempt == 1:
            await asyncio.sleep(0.5)
    return False


def delete_task_background(task_id: str) -> None:
    """即发即忘版本：用户的删除请求不该等存储服务。"""
    try:
        task = asyncio.create_task(delete_task(task_id))
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
    except RuntimeError:
        pass  # 无运行中的事件循环：存储侧的保留期清理会兜底


async def delete_results(task_id: str, urls: list[str]) -> bool:
    """按下载 URL 批量删除某任务在存储服务上的结果文件
    （``POST /internal/tasks/<id>/results/deletions``，幂等、逐条隔离）。

    调用方是"重试 / 重做后裁剪了 result_path"——被丢弃步骤的产物成了孤儿。URL 的反解由存储服务做
    （布局是它的私有知识），这里只说"这些条目我不要了"。未配置存储服务 → 跳过（返回 False）。
    """
    base = settings.STORAGE_INTERNAL_URL.strip().rstrip("/")
    if not base or not urls:
        return False
    headers = {"X-Internal-Token": settings.STORAGE_INTERNAL_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(f"{base}/internal/tasks/{task_id}/results/deletions",
                                     json={"urls": urls}, headers=headers)
        return resp.status_code == 200
    except httpx.HTTPError as e:
        logger.warning("[storage] 批量删除产物失败 task_id=%s: %s", task_id, e)
        return False
