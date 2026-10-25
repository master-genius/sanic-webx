"""任务配额服务。

集中管理面向用户的几类配额策略与计数逻辑，均为纯领域逻辑（不依赖 HTTP / 分发，便于单测与复用）：

  1. 并发名额（``ensure_within_concurrency``）：触发点是提交咽喉，任何投递入口都自动受控。
     规则：用户"同时占用名额"的任务数 ≤ 档位对应上限。补偿器的租约回收只把任务放回 PENDING 池、
     不经此校验——**被动重投不蚕食用户名额**。
  2. 每日创建（``ensure_within_daily_create``）：触发点是创建咽喉。规则：用户滚动 24 小时内
     创建的任务数 ≤ 档位上限（统计已存在任务的 created_at，删除会释放额度）。
  3. 等待子任务的顶层任务数（``ensure_within_waiting_parents``）：这是**堆积闸**，不是并发。

**两个接缝**，让配额策略不绑死某个用户体系或某套数字：

  - ``QuotaPolicy``：档位 → 各上限的映射（默认值见 ``DefaultQuotaPolicy``）
  - ``UserResolver``：怎么拿用户的档位 / 是否内部不限额账号 / 点数余额
    （默认读 webx 的 ``User`` 表；你的用户体系不同就实现它并 ``set_user_resolver``）

"占用名额"判定（与状态机语义对齐）：
  - 两个分支都先过 status 闸：status ∈ {pending, queued, running}
    —— draft / none 未提交草稿、completed / failed / cancelled 终态一律不占名额。
  - 单阶段任务（end_step=""）→ 仅看 status。
  - 多阶段任务（end_step!=""）→ status 闸之外还要 sub_status ∈ {pending, running}
    —— 在 running 内部停在 success（等用户追加参数）的多阶段任务不占名额。
  - 关键坑：sub_status 默认值即 PENDING，首次创建后草稿态是 status=draft + sub_status=pending，
    若不叠加 status 闸，这类草稿会被误计入名额（档位 0 时一个草稿即卡死提交）。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol

from tortoise.expressions import Q

from webx.config import settings
from webx.lib.task_errors import QuotaExceededError
from webx.models.task import ACTIVE_STATUSES, SubStatus, Task, TaskStatus
from webx.models.user import User

logger = logging.getLogger(__name__)

__all__ = [
    "QuotaExceededError", "QuotaPolicy", "DefaultQuotaPolicy", "UserResolver", "DefaultUserResolver",
    "set_quota_policy", "get_quota_policy", "set_user_resolver", "get_user_resolver",
    "is_task_active", "count_active_tasks", "count_active_children", "count_waiting_parents",
    "count_tasks_created_recently", "ensure_within_concurrency", "ensure_within_daily_create",
    "ensure_within_waiting_parents", "waiting_parent_filter", "PRIVILEGED_ROLE",
]

DAILY_CREATE_WINDOW_HOURS = 24

#: 内部 / 测试账号角色：直接跳过估算闸。仅软限制对其豁免——并发名额等"对别人的影响"不豁免
PRIVILEGED_ROLE = "inner"


# ==================== 接缝一：配额策略 ====================


class QuotaPolicy(Protocol):
    """档位 → 各项上限。"""

    def concurrency_limit(self, level: int) -> int: ...
    def child_concurrency_limit(self, level: int) -> int: ...
    def daily_create_limit(self, level: int) -> int: ...


class DefaultQuotaPolicy:
    """默认档位表（可子类化覆盖，或整体换一个策略）。"""

    #: 并发上限：level<=0 → 1；1 → 3；2 → 5；>2 → 10
    default_concurrency = 1
    concurrency_by_level: dict[int, int] = {1: 3, 2: 5}
    high_level_concurrency = 10
    #: 每日创建上限：level<=0 → 50；1 → 150；2 及以上 → 180
    default_daily_create = 50
    daily_create_by_level: dict[int, int] = {1: 150, 2: 180}
    high_level_daily_create = 180

    def concurrency_limit(self, level: int) -> int:
        if level <= 0:
            return self.default_concurrency
        return self.concurrency_by_level.get(level, self.high_level_concurrency)

    def child_concurrency_limit(self, level: int) -> int:
        """子任务（fan-out 展开）的并发额度 = 顶层额度 + 1。

        子任务有独立于顶层任务的额度池：比顶层宽 1，给"父任务的内部并行展开"留余量；
        且不挤占用户提交顶层任务的名额（顶层计数已排除子任务），否则"自己任务的子任务把自己下一个提交卡 403"反直觉。
        """
        return self.concurrency_limit(level) + 1

    def daily_create_limit(self, level: int) -> int:
        if level <= 0:
            return self.default_daily_create
        return self.daily_create_by_level.get(level, self.high_level_daily_create)


_quota_policy: QuotaPolicy = DefaultQuotaPolicy()


def set_quota_policy(policy: QuotaPolicy) -> None:
    global _quota_policy
    _quota_policy = policy


def get_quota_policy() -> QuotaPolicy:
    return _quota_policy


# ==================== 接缝二：用户解析 ====================


class UserResolver(Protocol):
    """怎么从 user_id 拿到配额与计价需要的三样东西。"""

    async def level(self, user_id: str) -> int: ...
    async def is_privileged(self, user_id: str) -> bool: ...
    async def balance(self, user_id: str) -> Optional[int]: ...


class DefaultUserResolver:
    """默认：读 webx 的 ``User`` 表。

    - 档位取 ``User.level``（user_id 为空或用户不存在 → 0）。
      预留接缝：level>0 时应校验 ``level_start`` / ``level_end`` 时间窗，窗口外视为过期降级为 0；当前未启用。
    - 特权判定：``User.role == "inner"``（大小写 / 空白不敏感）。
    - 余额：webx 的 ``User`` 没有"算力点"字段，默认返回 ``None``（= 不校验充足性）。
      你的用户体系有余额（例如把 ``User.points`` 当余额）就覆盖 ``balance``。
    """

    async def _user(self, user_id: str) -> Optional[User]:
        return await User.filter(id=user_id).first() if user_id else None

    async def level(self, user_id: str) -> int:
        user = await self._user(user_id)
        return (user.level or 0) if user else 0

    async def is_privileged(self, user_id: str) -> bool:
        user = await self._user(user_id)
        return bool(user) and (user.role or "").strip().lower() == PRIVILEGED_ROLE

    async def balance(self, user_id: str) -> Optional[int]:
        return None


_user_resolver: UserResolver = DefaultUserResolver()


def set_user_resolver(resolver: UserResolver) -> None:
    global _user_resolver
    _user_resolver = resolver


def get_user_resolver() -> UserResolver:
    return _user_resolver


# ==================== "占用名额"判定 ====================


def _active_task_filter() -> Q:
    """构造"占用名额"任务的查询条件（不含 user_id 维度）。

    仅统计**顶层任务**（parent_id=""）——子任务有独立额度池，不挤占用户提交顶层任务的名额。
    """
    active_status = Q(status__in=list(ACTIVE_STATUSES))
    single_phase = Q(end_step="") & active_status
    multi_phase = (~Q(end_step="")) & active_status & Q(
        sub_status__in=[SubStatus.PENDING, SubStatus.RUNNING]
    )
    return Q(parent_id="") & (single_phase | multi_phase)


def is_task_active(task: Task) -> bool:
    """单个任务是否"在飞"（占用 worker 名额）的纯 Python 谓词。

    与 ``_active_task_filter()``（DB 侧）严格同口径、互为单一真源，供只持有内存 Task 实例的调用点
    复用（如删除闸门）。不含 parent_id 维度。"非在飞"即等待用户追加参数、草稿态、终态等——
    这些均无 worker 在执行，可安全删除。
    """
    if task.status not in ACTIVE_STATUSES:
        return False
    if task.end_step == "":
        return True
    return task.sub_status in (SubStatus.PENDING, SubStatus.RUNNING)


async def count_active_tasks(user_id: str, exclude_task_id: Optional[str] = None) -> int:
    """统计用户当前占用名额的任务数。

    Args:
        exclude_task_id: 排除的任务 ID（正在投递的任务自身）。多阶段任务"推进下一步"时，
            控制器在调 submit 之前已把它置为 pending；不排除自身，续跑会被自身计数误伤。
    """
    query = Task.filter(Q(user_id=user_id) & _active_task_filter())
    if exclude_task_id:
        query = query.filter(~Q(id=exclude_task_id))
    return await query.count()


async def count_active_children(user_id: str) -> int:
    """统计用户当前在飞的子任务数（独立额度池）。排除软删除。"""
    return await Task.filter(
        user_id=user_id, is_delete=False, status__in=list(ACTIVE_STATUSES),
    ).filter(~Q(parent_id="")).count()


async def ensure_within_concurrency(user_id: str, exclude_task_id: Optional[str] = None) -> None:
    """投递前并发校验，超限抛 ``QuotaExceededError(scope="concurrency")``。无 user_id（匿名 / 内部）放行。"""
    if not user_id:
        return
    level = await _user_resolver.level(user_id)
    limit = _quota_policy.concurrency_limit(level)
    current = await count_active_tasks(user_id, exclude_task_id=exclude_task_id)
    if current >= limit:
        logger.info("并发名额超限：user_id=%s level=%s current=%s limit=%s", user_id, level, current, limit)
        raise QuotaExceededError(scope="concurrency", limit=limit, current=current)


# ==================== 每日创建 ====================


async def count_tasks_created_recently(user_id: str) -> int:
    """统计用户在滚动 24h 窗口内创建的任务数（含任意状态）。"""
    since = datetime.now(timezone.utc) - timedelta(hours=DAILY_CREATE_WINDOW_HOURS)
    return await Task.filter(user_id=user_id, created_at__gte=since).count()


async def ensure_within_daily_create(user_id: str) -> None:
    """创建前每日限额校验。应在写入新任务**之前**调用，避免本次创建被自身计入。"""
    if not user_id:
        return
    level = await _user_resolver.level(user_id)
    limit = _quota_policy.daily_create_limit(level)
    current = await count_tasks_created_recently(user_id)
    if current >= limit:
        logger.info("每日创建超限：user_id=%s level=%s current=%s limit=%s", user_id, level, current, limit)
        raise QuotaExceededError(scope="daily_create", limit=limit, current=current)


# ==================== 等待子任务的顶层任务（堆积闸）====================


def waiting_parent_filter() -> Q:
    """"已提交、但在等子任务算完"的顶层任务条件（不含 user_id 维度）。

    与"子任务待自动准入"是**同一对字段、不同的 parent_id 侧**：parent_id="" 是顶层，
    parent_id!="" 是子任务，两个集合不相交。各自把 parent_id 写进 WHERE，是它们能共用 auto_post 标记的全部前提。
    """
    return Q(parent_id="") & Q(status=TaskStatus.DRAFT) & Q(auto_post=1) & Q(is_delete=False)


async def count_waiting_parents(user_id: str, exclude_task_id: Optional[str] = None) -> int:
    query = Task.filter(Q(user_id=user_id) & waiting_parent_filter())
    if exclude_task_id:
        query = query.filter(~Q(id=exclude_task_id))
    return await query.count()


def waiting_parent_limit() -> int:
    """每用户等待中父任务上限。0=不限制；配置越界按 [0, 100] 钳。

    钳而不是启动时报错：这是一道堆积闸，配错的代价是限得过松或过紧，远低于"配错一个数字整个服务起不来"。
    钳的时候记日志，不静默。
    """
    raw = int(settings.TASK_WAITING_PARENT_MAX)
    clamped = max(0, min(100, raw))
    if clamped != raw:
        logger.warning("TASK_WAITING_PARENT_MAX=%s 超出有效区间 [0, 100]，按 %s 生效", raw, clamped)
    return clamped


async def ensure_within_waiting_parents(user_id: str, exclude_task_id: Optional[str] = None) -> None:
    """等待中父任务数闸门，超限抛 ``QuotaExceededError(scope="waiting_children")``。

    **与并发名额是两道独立的闸**，因为管的不是同一种资源：并发名额管**机时**（lane 被占住），
    而等待中的任务不占 lane、不耗机时——把它计进并发，等于让一个 48h 的子任务把用户的全部名额锁死一整天。
    但等待态也不能完全不设限：它没有成本，于是一个脚本可以堆出任意多个等待中的父任务，连带堆出同样多的子任务。
    """
    if not user_id:
        return
    limit = waiting_parent_limit()
    if limit <= 0:
        return
    current = await count_waiting_parents(user_id, exclude_task_id=exclude_task_id)
    if current >= limit:
        logger.info("等待中父任务数超限: user=%s current=%s limit=%s", user_id, current, limit)
        raise QuotaExceededError(scope="waiting_children", limit=limit, current=current)
