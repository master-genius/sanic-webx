"""Worker 分发入口 —— 主动领取版。

架构：

  • LeaseDispatcher：有空闲 Lane 就去领取服务领任务，没空槽就不发请求
    （「没空槽就不问」本身即背压，不需要任何绕行或丢弃逻辑）
  • Lane = 槽位：一条 Lane 同时最多跑一个任务，跑完才领下一个
  • 后台清理：与分发无关，周期性起子进程清理过期任务目录

没有本地队列、没有预取、没有排队任务需要续命，也没有需要保活的长寿连接：
每次领取都是独立的 HTTP 请求各带超时，因此不存在「socket 还是 ESTAB 但已经死了、
静默停收任务、必须人工重启」那类故障。

步进式轮询（TICK_SECONDS = 2s）：清理循环基于 2 秒 tick + 计数器控制实际频率。
"""

import asyncio
import json
import logging
import sys

from ._paths import with_package_on_pythonpath
from .callback import callback_client
from .config import worker_settings

logger = logging.getLogger(__name__)

# ─── 常量 ────────────────────────────────────────────────────────────────────

# 步进式轮询基础 tick（秒）—— cleanup 用
TICK_SECONDS = 2

# 任务目录清理 tick 数：每 N 个 TICK_SECONDS 触发一次清理子进程
# 设计动机见 cleanup 模块 —— 清理走子进程，避免 rmtree IO 阻塞主 event loop，
# 也方便与 CLI 入口共享同一份模块逻辑（python -m webx_worker cleanup）
CLEANUP_TICKS = 300  # 300 × 2s = 600s ≈ 10 分钟


async def run_worker(handlers: dict, lane_manager, executor):
    """Worker 主循环：领取分发 + 后台清理。

    worker 拿着自己的空闲槽位去领取服务领任务：
      · **本地队列**：Lane 是「槽位」，跑完才领下一个。
      · 没空槽就不发请求，任务安静待在 DB 池里。

    Args:
        handlers:     {channel_name: handler} 路由表（保留：executor 据此路由）
        lane_manager: LaneManager 实例（提供 task_type → lane 映射，即能力声明）
        executor:     GenericTaskExecutor 实例
    """
    from .lease import LeaseDispatcher, lease_client

    dispatcher = LeaseDispatcher(lane_manager, executor)
    tasks = [
        asyncio.create_task(dispatcher.loop(), name="lease-dispatcher"),
        asyncio.create_task(_cleanup_loop(), name="cleanup"),
    ]
    logger.info("[Worker] 已启动：主动领取分发 + 后台清理")

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        dispatcher.stop()
        # 这里**不等**在跑的任务：`tasks` 只装了分发循环与清理循环，真正跑任务的
        # `_run_one` 是 lease.py 里 create_task 出来并登记在 _running_tasks 的，
        # 停机时由 app.async_main 的收尾三步取消（见那里）。
        logger.info("Worker 被中断：停止领新任务，归还在手租约后退出（不等在跑的任务）")
    finally:
        # 归还在手租约：让任务立刻回池，而不是等租约自然到期。
        # 放在这里而非 except 里，是为了正常退出与异常退出都覆盖到。
        try:
            await dispatcher.release_all()
        except Exception as e:
            logger.warning("[lease] 归还在手租约时出错（将由租约到期兜底）：%s", e)

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await lease_client.close()
        await callback_client.close()
        print("[✓] Worker 已关闭")


# ─── 内部协程 ─────────────────────────────────────────────────────────────────


async def _cleanup_loop():
    """步进式任务目录清理循环：每 CLEANUP_TICKS 个 tick 启动一次清理子进程。

    设计原则：
      - 起子进程而非线程：避免 shutil.rmtree 的 IO 阻塞 worker 主 event loop，
        同时进程级隔离让清理异常不会影响主进程
      - 与 CLI 入口共用同一份逻辑：调用 `python -m webx_worker cleanup` 而非内嵌实现
      - 没有任务投递时清理无意义，但成本极低（扫一遍目录就返回），可接受
    """
    tick_count = 0
    interval_s = CLEANUP_TICKS * TICK_SECONDS
    logger.info(
        f"[Cleanup] 启动，每 {interval_s}s（{CLEANUP_TICKS} ticks）触发一次清理子进程"
    )

    while True:
        await asyncio.sleep(TICK_SECONDS)
        tick_count += 1
        if tick_count < CLEANUP_TICKS:
            continue
        tick_count = 0

        try:
            await _launch_cleanup_subprocess()
        except Exception as e:
            logger.error(f"[Cleanup] 子进程启动异常: {e}", exc_info=True)


async def _launch_cleanup_subprocess():
    """启动清理子进程，使用默认策略（failed=2d, completed=7d）。

    子进程 stdout 是 JSON 结果摘要，截断后写入主进程日志；非零退出码记 warning。
    """
    import os

    cmd = [
        sys.executable, "-m", "webx_worker", "cleanup",
        "--work-dir", worker_settings.WORK_DIR,
        "--quiet",   # 抑制子进程的 INFO 日志，只保留最终 JSON 摘要
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # 清理子进程要能 import 本包（包可能没装进解释器，只是靠 sys.path 找到的）
        env=with_package_on_pythonpath(dict(os.environ)),
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        logger.warning(
            f"[Cleanup] 子进程退出码 {proc.returncode}, "
            f"stderr: {stderr.decode(errors='ignore').strip()[:500]}"
        )
        return

    summary = stdout.decode(errors="ignore").strip()
    # 解析 JSON 摘要做更精细的日志，解析失败回退到原文截断
    try:
        data = json.loads(summary)
        logger.info(
            f"[Cleanup] 子进程完成 scanned={data.get('scanned', 0)} "
            f"purged={data.get('purged', 0)} skipped={data.get('skipped', {})} "
            f"errors={len(data.get('errors', []))}"
        )
    except json.JSONDecodeError:
        logger.info(f"[Cleanup] 子进程完成: {summary[:500]}")
