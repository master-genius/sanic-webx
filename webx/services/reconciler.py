"""任务补偿器（Reconciler）。

三个**独立节拍**的后台循环，互不阻塞：

  1. 租约回收（秒级）：按 ``lease_until`` 的走索引点查，回收到期租约。**故障发现速度由它决定**。
  2. 慢扫描（分钟级）：心跳陈旧的第二道网、放弃判定、空任务清理、待领取池滞留告警。
  3. 准入（秒级）：把"等着被自动投递"的任务择机投递——**具体的巡检动作是注册进来的钩子**
     （见 ``register_sweep``），补偿器本身不认识任何任务类型。

## 为什么回收循环不能与慢扫描合并

节拍必须跟随"查询代价 × 时效需求"：回收是走索引的时间戳点查、决定故障发现速度 → 秒级；慢扫描是多分支重扫
（含 sub_status 判定、abandon、NONE 清理），代价高、时效要求低 → 分钟级。把两者合并（即调小 scan_interval）
会拖慢重扫，且解决不了问题——真正的提速来自**判据换了**：从"心跳静默了多久"这种需要重扫的模糊统计，
换成一次走索引的时间戳比较。

## 判据说明（慢扫描）

只看 ``last_heartbeat``，不看 ``updated_at``：``updated_at`` 是 auto_now，任何无关字段写入都会污染它，
做"活体检测"语义不严谨；``last_heartbeat`` 由认领时写入并由 worker 心跳 / 状态回调持续刷新，与"任务还活着"严格对应。
``last_heartbeat = 0`` 仅出现在 NONE / DRAFT / PENDING 这些尚未投递的状态，补偿器不该接管它们；
若在 RUNNING / QUEUED 看到 0，视为脏数据，由扫描入口的 backfill 一次性补成当前时间。

## 使用

独立进程（见 ``tools/reconciler.py``）自行初始化数据库连接后 ``await reconciler.start()``。
也可以挂在应用的启动 / 停止钩子里，但**多 worker 部署时必须独立成一份进程**，否则每个 worker 各跑一套。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from tortoise.expressions import Q

from webx.config import BaseSettings, settings
from webx.lib.task_poke import poke_task_server
from webx.models.task import SubStatus, Task, TaskStatus
from webx.services import lease as lease_svc
from webx.services import storage_client
from webx.services.storage_service import storage_service

logger = logging.getLogger(__name__)

Sweep = Callable[[], Awaitable[None]]
FailedHook = Callable[[list[str], str], Awaitable[None]]

# 模块级钩子注册表，与补偿器实例无关。
#
# 为什么不挂在实例上：拥有任务知识的模块（如任务服务）在 **import 期**就要注册钩子，而那时业务配置往往还没
# configure_settings——若此时创建补偿器实例，它会读到过期的默认配置。注册表放模块级，实例在运行时才读取。
_SWEEPS: list[tuple[str, Sweep]] = []
_FAILED_HOOKS: list[FailedHook] = []


def register_sweep(name: str, fn: Sweep) -> None:
    """注册一个准入 / 巡检钩子（每个节拍各自 try 执行，一个异常不影响其余）。

    补偿器不认识任务类型。"把等待中的子任务投递出去"、"收拾某类业务的死 job"这类动作，
    由拥有这些知识的模块注册进来。同名重复注册会替换（便于测试与热替换）。
    """
    global _SWEEPS
    _SWEEPS = [(n, f) for n, f in _SWEEPS if n != name] + [(name, fn)]


def unregister_sweep(name: str) -> None:
    global _SWEEPS
    _SWEEPS = [(n, f) for n, f in _SWEEPS if n != name]


def register_failed_hook(fn: FailedHook) -> None:
    """注册"任务被补偿器批量判失败"的回调：``(task_ids, reason)``。用于通知用户 / 告警。"""
    if fn not in _FAILED_HOOKS:
        _FAILED_HOOKS.append(fn)


class Reconciler:
    """任务补偿器。

    Args:
        scan_interval:            慢扫描间隔（秒），默认 600
        stale_threshold_minutes:  RUNNING 心跳超时阈值（分钟），默认 60
        queued_timeout_minutes:   QUEUED 超时阈值（分钟），默认 60——领了但没起跑
        abandon_threshold_hours:  放弃阈值（小时），默认 168（7 天）；last_heartbeat 超过它未刷新则标记 failed
        none_task_timeout_hours:  空任务（none 状态）超时时间（小时），默认 3；draft 永不自动清理
        admit_interval:           准入循环间隔（秒），默认 30
        lease_interval:           租约回收循环间隔（秒），默认 5；**独立于 scan_interval**
    """

    def __init__(
        self,
        scan_interval: int = 600,
        stale_threshold_minutes: int = 60,
        queued_timeout_minutes: int = 60,
        abandon_threshold_hours: int = 168,
        none_task_timeout_hours: int = 3,
        admit_interval: int = 30,
        lease_interval: int = 5,
    ) -> None:
        self.scan_interval = scan_interval
        # 阈值转为毫秒，与 last_heartbeat（毫秒级 Unix 时间戳）配合使用
        self.stale_threshold_ms = stale_threshold_minutes * 60 * 1000
        self.queued_timeout_ms = queued_timeout_minutes * 60 * 1000
        self.abandon_threshold_ms = abandon_threshold_hours * 3600 * 1000
        self.none_task_timeout_ms = none_task_timeout_hours * 3600 * 1000
        self.admit_interval = admit_interval
        self.lease_interval = lease_interval

        self._tasks: list[asyncio.Task] = []
        self._running = False

    # ------------------------------------------------------------------
    # 配置装配
    # ------------------------------------------------------------------

    @classmethod
    def from_settings(cls, s: BaseSettings, **overrides: int | None) -> "Reconciler":
        """优先级：显式关键字参数（命令行） > 配置（.env） > 构造函数默认值。值为 None 的项表示"不覆盖"。"""

        def pick(key: str, conf: int) -> int:
            val = overrides.get(key)
            return val if val is not None else conf

        return cls(
            scan_interval=pick("scan_interval", s.RECONCILER_SCAN_INTERVAL),
            stale_threshold_minutes=pick("stale_threshold_minutes", s.RECONCILER_STALE_THRESHOLD_MINUTES),
            queued_timeout_minutes=pick("queued_timeout_minutes", s.RECONCILER_QUEUED_TIMEOUT_MINUTES),
            abandon_threshold_hours=pick("abandon_threshold_hours", s.RECONCILER_ABANDON_THRESHOLD_HOURS),
            none_task_timeout_hours=pick("none_task_timeout_hours", s.RECONCILER_NONE_TASK_TIMEOUT_HOURS),
            admit_interval=pick("admit_interval", s.RECONCILER_ADMIT_INTERVAL),
            lease_interval=pick("lease_interval", s.RECONCILER_LEASE_INTERVAL),
        )

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def start(self, *, startup_delay: float = 10.0) -> None:
        """启动三个后台循环。``startup_delay`` 给数据库连接就绪留时间（测试里传 0）。"""
        if self._running:
            logger.warning("Reconciler 已在运行，忽略重复启动")
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._loop(self._loop_scan, self.scan_interval, startup_delay), name="reconciler-scan"),
            asyncio.create_task(self._loop(self._run_sweeps, self.admit_interval, startup_delay), name="reconciler-admit"),
            asyncio.create_task(self._loop(self._reclaim_once, self.lease_interval, startup_delay), name="reconciler-lease"),
        ]
        logger.info(
            "Reconciler 已启动 (scan=%ss stale=%smin queued_timeout=%smin abandon=%sh admit=%ss lease=%ss sweeps=%s)",
            self.scan_interval, self.stale_threshold_ms // 60000, self.queued_timeout_ms // 60000,
            self.abandon_threshold_ms // 3600000, self.admit_interval, self.lease_interval,
            [n for n, _ in _SWEEPS],
        )

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks = []
        logger.info("Reconciler 已停止")

    @property
    def is_running(self) -> bool:
        return self._running and any(not t.done() for t in self._tasks)

    async def _loop(self, body: Callable[[], Awaitable[None]], interval: float, startup_delay: float) -> None:
        """通用循环骨架：异常兜底（单轮失败不能让补偿器停摆），取消即退出。"""
        if startup_delay:
            await asyncio.sleep(startup_delay)
        while self._running:
            try:
                await body()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.error("Reconciler 循环异常 [%s]: %s", getattr(body, "__name__", body), e, exc_info=True)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

    # ------------------------------------------------------------------
    # 准入 / 巡检
    # ------------------------------------------------------------------

    async def _run_sweeps(self) -> None:
        """依次执行注册的巡检钩子，**各自 try**——一侧异常不该让另一侧整轮不跑。"""
        for name, fn in list(_SWEEPS):
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.error("[Sweep:%s] 异常: %s", name, e, exc_info=True)

    # ------------------------------------------------------------------
    # 租约回收
    # ------------------------------------------------------------------

    async def _reclaim_once(self) -> None:
        """一轮回收：先判超限失败，再回收其余。

        顺序不可颠倒——若先回收，requeues 刚被 +1 的任务会在同一轮里又被判超限，等于阈值凭空少一次。
        """
        failed = await lease_svc.fail_over_limit()
        for row in failed:
            logger.error(
                "[Lease] 任务 %s 连续 %s 次被回收且未推进，判定异常并标记 failed（最后持有者 %s）",
                row.get("id"), row.get("requeues"), row.get("worker_id"),
            )
        if failed:
            await self._notify_failed([r["id"] for r in failed], "连续多次被回收且未推进，系统判定为异常任务")

        reclaimed = await lease_svc.reclaim_expired()
        if not reclaimed:
            return
        for row in reclaimed:
            # 区分两种回收：RUNNING 回收才计入"连续无进展"。一批回收里清一色是 queued，读作
            # "worker 领得到但 ack 不了"——十有八九是 Web 不可达，而不是任务有问题。
            # 这个区分只在日志里，但它决定了运维第一眼往哪儿看。
            old = row.get("old_status")
            logger.warning(
                "[Lease] 回收到期租约 task_id=%s type=%s 回收前=%s requeues=%s%s → 回 PENDING 池",
                row.get("id"), row.get("task_type"), old, row.get("requeues"),
                "" if old == "running" else "（未起跑，不计入无进展）",
            )
        # 回收也是"让任务进入 PENDING"的一条路径——规则要求 poke
        await poke_task_server()

    # ------------------------------------------------------------------
    # 慢扫描
    # ------------------------------------------------------------------

    async def _loop_scan(self) -> None:
        await self.scan_and_recover()

    async def scan_and_recover(self) -> None:
        """扫描并恢复超时任务。

          0. 入口 backfill：RUNNING / QUEUED 但 last_heartbeat=0 的脏数据补成当前时间戳
          1. 心跳超时的 RUNNING 任务，排除"等待用户"的（sub_status=success 且 sub_step=""，多阶段任务
             已完成某步、worker 等用户追加参数）、草稿、已失败的
          2. 对每个超时任务（仅按 last_heartbeat）：未超过 abandon_threshold → 重投（不写 retry）；否则标记 failed
          3. 超时的 QUEUED 任务（第二道网：租约字段本身出脏数据时才轮到它）
          4. 清理超时的 none 空任务
          5. 待领取池滞留告警
        """
        now_ms = int(time.time() * 1000)
        stale_ms = now_ms - self.stale_threshold_ms

        backfilled = await Task.filter(
            status__in=[TaskStatus.RUNNING, TaskStatus.QUEUED], last_heartbeat=0,
        ).update(last_heartbeat=now_ms)
        if backfilled:
            logger.warning("[Reconciler] backfill：%d 条 RUNNING/QUEUED 任务 last_heartbeat=0，已补为当前时间戳", backfilled)

        stale_tasks = await Task.filter(
            status=TaskStatus.RUNNING, last_heartbeat__gt=0, last_heartbeat__lt=stale_ms,
        ).filter(
            ~Q(sub_status=SubStatus.SUCCESS, sub_step=""),
            ~Q(sub_status=SubStatus.DRAFT),
            # 子步骤已失败：不变量保证此时 status 必然 FAILED，不会被 status=RUNNING 命中；
            # 显式排除是防御性补丁，避免未来某条路径破坏不变量
            ~Q(sub_status=SubStatus.FAILED),
        ).all()

        waiting = await Task.filter(status=TaskStatus.RUNNING, sub_status=SubStatus.SUCCESS, sub_step="").count()
        if waiting:
            logger.info("Reconciler: %d 个多阶段任务处于「等待用户」状态，跳过补偿", waiting)

        if stale_tasks:
            logger.warning("Reconciler 发现 %d 个心跳超时任务，开始处理", len(stale_tasks))
            await self._handle_stale(stale_tasks)
        else:
            logger.debug("Reconciler 扫描完毕，无超时任务")

        await self._recover_queued_tasks()
        await self._clean_stale_none_tasks()
        await self._warn_stuck_pending()

    async def _handle_stale(self, tasks: list[Task]) -> None:
        """分组：可重试 vs 应放弃（基于 last_heartbeat，而非 retry 计数 / updated_at）。"""
        abandon_before_ms = int(time.time() * 1000) - self.abandon_threshold_ms
        retry_tasks = [t for t in tasks if t.last_heartbeat >= abandon_before_ms]
        fail_tasks = [t for t in tasks if t.last_heartbeat < abandon_before_ms]

        if fail_tasks:
            await self._batch_fail_tasks([t.id for t in fail_tasks])

        recovered = 0
        for task in retry_tasks:
            try:
                if await self._recover_task(task):
                    recovered += 1
            except Exception as e:  # noqa: BLE001
                logger.error("Reconciler 处理任务异常 task_id=%s error=%s", task.id, e, exc_info=True)
        logger.warning("Reconciler 本轮处理完毕: 重新投递=%d, 批量标记失败=%d", recovered, len(fail_tasks))

    async def _recover_queued_tasks(self) -> None:
        """超时的 QUEUED 任务：已被 worker 认领但尚未上报 running。

        last_heartbeat 超过 queued_timeout 未刷新，说明 worker 崩溃，或领取响应在途丢失（认领已落库但 worker 没收到）。
        处理策略与 RUNNING 超时一致。注：正常情况下这由认领短租约在秒级兜住，轮不到本函数。
        """
        stale_ms = int(time.time() * 1000) - self.queued_timeout_ms
        # 同 RUNNING 分支：显式排除 sub_status=FAILED 作为防御性兜底
        stale_tasks = await Task.filter(
            status=TaskStatus.QUEUED, last_heartbeat__gt=0, last_heartbeat__lt=stale_ms,
        ).filter(~Q(sub_status=SubStatus.FAILED)).all()
        if not stale_tasks:
            logger.debug("Reconciler 无超时的 queued 任务")
            return
        logger.warning("Reconciler 发现 %d 个超时的 queued 任务，开始重新投递", len(stale_tasks))
        await self._handle_stale(stale_tasks)

    async def _warn_stuck_pending(self) -> None:
        """PENDING 停留过久 → 告警。

        这是全系统唯一**没有任何计时器会响**的状态：任务在 PENDING 池里，``lease_until IS NULL`` 所以租约回收
        循环跳过它，``last_heartbeat`` 从未推进所以心跳陈旧的判据也认不出它。它可以无声地躺到天荒地老。
        最典型的成因是"没有能跑这个 task_type 的 worker 在线"。

        为什么是告警而不是自动失败：没有 worker 是**运维问题**，不是任务问题。把用户的任务标成 failed 既解决不了问题，
        还要用户重新提交一遍。让它继续等，运维把机器拉起来即可。
        """
        stuck_before = datetime.now(timezone.utc) - timedelta(seconds=self.stale_threshold_ms // 1000)
        rows = await Task.filter(
            status=TaskStatus.PENDING, is_delete=False, submitted_at__lt=stuck_before,
        ).values("task_type")
        if not rows:
            return
        by_type = Counter(r["task_type"] for r in rows)
        logger.error(
            "[Reconciler] ⚠️ %d 个任务在待领取池滞留超过 %d 分钟，按类型分布 %s —— 请确认是否有能跑这些类型的 worker 在线",
            len(rows), self.stale_threshold_ms // 60000, dict(by_type),
        )

    async def _clean_stale_none_tasks(self) -> None:
        """清理超时未提交的空任务（none 状态）。草稿（draft）永不自动清理。"""
        stale_before = datetime.now(tz=timezone.utc) - timedelta(milliseconds=self.none_task_timeout_ms)
        stale_tasks = await Task.filter(status=TaskStatus.NONE, updated_at__lt=stale_before).all()
        if not stale_tasks:
            return
        logger.info("Reconciler 发现 %d 个超时的 none 任务，开始清理", len(stale_tasks))
        cleaned = 0
        for task in stale_tasks:
            try:
                # 本地输入文件与远端产物**各删各的，都必须执行**——
                # 同机部署时恰好是同一棵目录树，分离后是两台机器上的两棵
                storage_service.store.delete_prefix(f"tasks/{task.id}")
                await task.delete()
                storage_client.delete_task_background(task.id)
                cleaned += 1
            except Exception as e:  # noqa: BLE001
                logger.error("[Reconciler] 清理超时空任务失败 task_id=%s error=%s", task.id, e, exc_info=True)
        logger.info("Reconciler 清理超时的 none 任务: %d", cleaned)

    # ------------------------------------------------------------------
    # 单任务操作
    # ------------------------------------------------------------------

    async def _recover_task(self, task: Task) -> bool:
        """将心跳陈旧的任务放回待领取池（第二道网）。返回是否真的回池。

        ⚠️ 不写 retry：retry 是"用户主动重试 / 重做"消费 allowed_retries 的配额计数，补偿器的被动重投
        不应蚕食用户配额，否则环境抖动会让用户的额度被悄悄耗光。

        回收 = **只把任务放回待领取池**，没有任何投递动作。本函数如今是第二道网：正常情况下
        ``_reclaim_once`` 早在秒级就按 lease_until 回收了任务，轮不到这里；只有租约字段本身出脏数据
        （如被写成很远的未来）才会落到"心跳陈旧"这条判据上。
        """
        ok = await lease_svc.enter_pending_pool(task.id, [TaskStatus.RUNNING.value, TaskStatus.QUEUED.value])
        if not ok:
            logger.info("[Reconciler] 任务状态已变更，跳过回池 task_id=%s", task.id)
            return False
        logger.warning("[Reconciler] 心跳陈旧兜底：任务 %s（type=%s）回 PENDING 池", task.id, task.task_type)
        await poke_task_server()
        return True

    async def _batch_fail_tasks(self, task_ids: list[str]) -> None:
        """批量将长时间无进展的任务标记为 failed（单次 SQL UPDATE，带状态条件，并发完成的不受影响）。"""
        reason = "[Reconciler] 长时间无进展，自动标记失败"
        logger.error("[Reconciler] 批量标记 %d 个任务为失败（last_heartbeat 超过 abandon_threshold）: %s", len(task_ids), task_ids)
        await Task.filter(
            id__in=task_ids, status__in=[TaskStatus.RUNNING, TaskStatus.QUEUED],
        ).update(status=TaskStatus.FAILED, sub_status=SubStatus.FAILED, error_message=reason)
        # 只通知这次真被置为失败的：update 带状态条件，并发完成的任务不在其中
        failed = await Task.filter(id__in=task_ids, status=TaskStatus.FAILED, error_message=reason).values_list("id", flat=True)
        await self._notify_failed(list(failed), "长时间无进展，系统自动标记为失败")

    async def _notify_failed(self, task_ids: list[str], reason: str) -> None:
        if not task_ids:
            return
        for hook in list(_FAILED_HOOKS):
            try:
                await hook(task_ids, reason)
            except Exception as e:  # noqa: BLE001 —— 通知失败不能影响补偿本身
                logger.error("[Reconciler] 失败通知钩子异常: %s", e, exc_info=True)


def build_reconciler(**overrides: int | None) -> Reconciler:
    """构造补偿器：命令行 overrides > 配置 > 默认值。**在配置注册之后**调用（不要在 import 期）。"""
    return Reconciler.from_settings(settings, **overrides)
