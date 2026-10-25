"""任务服务：任务生命周期的全部业务逻辑。

任务模块是 webx 核心的一部分。本服务**不认识任何具体任务类型**：参数校验、点数估算、超时、提交前置闸、
挂起子任务……一律经 ``webx.lib.task_types`` 的注册表（``TaskTypeSpec``）取。

## 几条贯穿全文件的铁律

1. **任何让任务进入 PENDING 的路径都走 ``lease.enter_pending_pool``**：它保证 ``worker_id=''``、
   ``lease_until=NULL``、``lease_seq += 1``，并且之后必须 poke 领取服务。
2. **用户主动的状态重置（retry / revise / redo / 推进下一步）必须让任务重回"无租户"并 bump ``lease_seq``**：
   上一轮 worker 可能还有在途回调（失败上报队列里的、产物交付里的），不 bump 的话它们的
   ``(worker_id, lease_seq)`` 仍然匹配，会写进用户刚刚重置的这一轮——从用户 API 就能触达，不需要网络分区。
3. **前置校验一律在破坏性写入之前**（翻 PENDING / 消耗 retry）：校验失败时任务原样停在原状态，
   不产生 PENDING 孤儿，也不白耗用户的重试额度。
4. **状态写入用带状态条件的 ``filter(...).update()``**，不用 stale 对象 ``save()``：
   并发的取消 / 完成 / 失败之间靠 WHERE 互斥，0 行即作废。

## 父子任务与自动准入

子任务（``parent_id`` 非空）是父任务的内部展开，有**独立额度池**，不蚕食用户提交顶层任务的名额。
两类"等着被自动投递"的任务共用 ``DRAFT + auto_post=1`` 这对字段，靠 ``parent_id`` 严格分开、集合不相交：

  - 子任务：等 per-user 子任务额度 → ``admit_pending_children``
  - 顶层任务：等自己的子任务全部终态 → ``admit_waiting_parents``

两者都注册为补偿器的巡检钩子（``register_sweep``），本模块 import 时完成注册。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from tortoise.expressions import F, Q

from webx.lib.task_errors import (
    ChildrenNotReadyError,
    ParamsError,
    QuotaExceededError,
    TaskStateConflictError,
)
from webx.lib.task_events import (
    EVENT_CANCELLED,
    EVENT_COMPLETED,
    EVENT_FAILED,
    EVENT_STEP_DONE,
    emit_task_event,
)
from webx.lib.task_poke import poke_task_server
from webx.lib.task_results import (
    merge_field_path,
    prune_log_path,
    prune_result_path,
    resolve_step_token,
)
from webx.lib.task_types import (
    check_task_type,
    get_stage_config,
    get_task_type,
    requires_children_complete,
    resolve_timeout,
    validate_step_transition,
    validate_task_params,
)
from webx.lib.genid import gen_id
from webx.models.task import ACTIVE_STATUSES, SubStatus, Task, TaskStatus
from webx.services import storage_client
from webx.services.compute_points import ensure_affordable
from webx.services.lease import claim_auto_post_child, enter_pending_pool
from webx.services.quota import (
    count_active_children,
    ensure_within_concurrency,
    ensure_within_daily_create,
    ensure_within_waiting_parents,
    get_quota_policy,
    get_user_resolver,
    waiting_parent_filter,
)
from webx.services.reconciler import register_failed_hook, register_sweep
from webx.services.storage_service import storage_service

logger = logging.getLogger(__name__)

_CHILD_UNFINISHED = (
    TaskStatus.NONE, TaskStatus.DRAFT, TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _loads(raw: Optional[str], default: Any) -> Any:
    try:
        return json.loads(raw) if raw else default
    except (json.JSONDecodeError, TypeError):
        return default


def parse_params(raw: Optional[str]) -> dict:
    """解析 params_text 为 dict；不是 JSON 对象（历史数据 / 脏值）按空处理。"""
    value = _loads(raw, {})
    return value if isinstance(value, dict) else {}


def parse_files(raw: Optional[str]) -> list[dict]:
    value = _loads(raw, [])
    return [f for f in value if isinstance(f, dict)] if isinstance(value, list) else []


class TaskService:
    """任务服务（全部是类方法，无实例状态）。"""

    # ==================== 创建 / 查询 ====================

    @classmethod
    async def create_task(
        cls,
        task_type: str,
        files: Optional[list[dict]] = None,
        params_text: Optional[str] = None,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        name: str = "",
        parent_id: str = "",
        tag: str = "",
        parent_step: str = "",
        auto_post: int = 0,
    ) -> Task:
        """创建任务。有输入文件 → DRAFT，否则 NONE（刚创建、尚未成型）。

        每日创建限额在写入新任务**之前**校验，避免本次创建被自身计入。
        子任务是父任务的内部展开，豁免——不蚕食用户每日创建额度。
        """
        spec = check_task_type(task_type)
        if params_text:
            validate_task_params(task_type, parse_params(params_text))

        if not parent_id:
            await ensure_within_daily_create(user_id or "")

        files = files or []
        config = spec.stage
        timeout = resolve_timeout(task_type, params=parse_params(params_text), files=files)
        task = await Task.create(
            id=gen_id(),
            task_type=task_type,
            name=name or "",
            status=TaskStatus.DRAFT if files else TaskStatus.NONE,
            user_id=user_id or "",
            org_id=org_id or "",
            parent_id=parent_id or "",
            tag=tag or "",
            parent_step=parent_step or "",
            auto_post=auto_post,
            files_json=json.dumps(files, ensure_ascii=False) if files else "[]",
            params_text=params_text or "{}",
            end_step=config.end_step,
            timeout_seconds=timeout,
        )
        storage_service.store.root.mkdir(parents=True, exist_ok=True)
        return task

    @classmethod
    async def get_task(cls, task_id: str) -> Optional[Task]:
        return await Task.filter(id=task_id).first()

    @classmethod
    async def get_tasks_by_ids(cls, task_ids: list[str], user_id: Optional[str] = None) -> list[Task]:
        """按 id 批量取（轻量轮询用）。传 user_id 则只返回该用户的，非本人 / 不存在的静默跳过。"""
        q = Task.filter(id__in=task_ids, is_delete=False)
        if user_id:
            q = q.filter(user_id=user_id)
        return await q

    @classmethod
    async def get_children(
        cls, parent_id: str, *, parent_step: Optional[str] = None, include_deleted: bool = False
    ) -> list[Task]:
        """某父任务的子任务，按创建时间正序。默认排除软删除的。"""
        if not parent_id:
            return []
        q = Task.filter(parent_id=parent_id)
        if parent_step is not None:
            q = q.filter(parent_step=parent_step)
        if not include_deleted:
            q = q.filter(is_delete=False)
        return await q.order_by("created_at")

    # ==================== 提交 ====================

    @classmethod
    async def precheck_submittable(
        cls, task: Task, target_sub_step: str, *, params_text: Optional[str] = None
    ) -> None:
        """提交前置校验（纯校验、不写库）。

        在任何破坏性状态变更（翻 PENDING / 消耗 retry）之前调用，保证校验失败时任务停在原状态、
        不产生 PENDING 孤儿。闸门与 ``submit_task`` 内部同口径，仅去掉写操作；
        按"目标步"而非 ``task.sub_step`` 判定（预翻前 sub_step 仍是旧值）。

        ``params_text`` 是本次提交**将要生效**的参数（PATCH 合并后的 prospective 值）。缺省用库里的。
        改参数的路径必须显式传，否则闸门判的是旧参数：旧参数合规 → 放行 → 翻 PENDING →
        提交咽喉用新参数拒 → 任务卡成 PENDING 孤儿。

        子任务豁免并发与聚合闸门。
        """
        spec = get_task_type(task.task_type)
        effective = task.params_text if params_text is None else params_text
        validate_task_params(task.task_type, parse_params(effective))
        if spec.precheck_submit is not None:
            probe = task if params_text is None else _with(task, params_text=params_text)
            await spec.precheck_submit(probe)
        await ensure_affordable(
            task if params_text is None else _with(task, params_text=params_text), task.user_id
        )

        if not task.parent_id:
            await ensure_within_concurrency(task.user_id, exclude_task_id=task.id)
            if requires_children_complete(task.task_type, target_sub_step):
                await cls._ensure_children_complete(task, step=target_sub_step)

    @classmethod
    async def submit_task(cls, task: Task) -> bool:
        """提交任务：入待领取池（NONE / DRAFT / PENDING → PENDING）。

        **不存在"投递失败"这种故障**：任务翻到 PENDING 就算提交完成，安静待在池里等 worker 来领。
        poke 只是把发现延迟压到近乎立刻，失败也只损失延迟。

        Returns:
            是否成功进入待领取池。False 表示状态不允许 / DB 故障，任务状态不变。
            校验不通过则抛对应的业务异常（400 / 403 / 409）。
        """
        if task.status not in (TaskStatus.NONE, TaskStatus.DRAFT, TaskStatus.PENDING):
            logger.error("任务 %s 状态 %s 不允许提交", task.id, task.status)
            return False

        spec = check_task_type(task.task_type)
        validate_task_params(task.task_type, parse_params(task.params_text))
        if spec.precheck_submit is not None:
            await spec.precheck_submit(task)
        await ensure_affordable(task, task.user_id)

        # 挂起钩子：类型可以先建一批子任务、把本任务挂起为 DRAFT + auto_post=1，等子任务算完再投递。
        # 位置在并发闸**之前**：等待态不占 lane、不耗机时，把它计进并发等于让一个长时间的子任务
        # 把用户的全部名额锁死（堆积另有闸，见 hold_parent）。
        if spec.hold_submit is not None and await spec.hold_submit(task):
            return True

        # 并发名额（投递咽喉统一收口）。排除任务自身，避免多阶段任务推进下一步时被自身计数误伤；
        # 补偿器的回收只回池、不经此处。子任务豁免——否则 fan-out 会瞬间撑爆用户名额上限。
        if not task.parent_id:
            await ensure_within_concurrency(task.user_id, exclude_task_id=task.id)

        # 多阶段任务首次提交：sub_step 为空时按状态机推导起始步骤（起点必须唯一，否则交由调用方显式传入）
        if task.end_step and not task.sub_step:
            if not _loads(task.steps, []):
                initial = get_stage_config(task.task_type).step_transitions.get("", frozenset())
                if len(initial) == 1:
                    task.sub_step = next(iter(initial))
                    await task.save(update_fields=["sub_step", "updated_at"])

        # 聚合闸门（配置驱动）：进入需聚合子任务的步骤前，子任务必须都完成。子任务自身不参与
        if not task.parent_id and requires_children_complete(task.task_type, task.sub_step):
            await cls._ensure_children_complete(task)

        # 按"本次投递的步骤"派生超时：每步是一次全新投递、全新子进程、从零计时，所以超时恒是"本步上限"。
        # 在投递咽喉现算而非存字段——所有推进路径（首投 / 推进 / retry / redo / 补偿器重投）都必经此处，
        # 没有"哪条路径忘了同步更新"的不变量负担
        timeout = resolve_timeout(
            task.task_type, sub_step=task.sub_step or "",
            params=parse_params(task.params_text), files=parse_files(task.files_json),
        )
        if timeout != task.timeout_seconds:
            task.timeout_seconds = timeout
            await task.save(update_fields=["timeout_seconds", "updated_at"])

        try:
            # 原子占位（乐观锁）：以读入时的 origin 状态作为翻转 UPDATE 的 WHERE 前置条件。并发重复提交时
            # 只有一个请求能 origin→PENDING（rowcount=1），其余因 status 已被抢先改变而 rowcount=0，
            # 判为重复提交抛 TaskStateConflictError（400），且没有任何副作用。
            # 本守卫只拦 origin≠PENDING 的入口（DRAFT 双击提交、PATCH 草稿提交、创建即提交）；
            # retry / 推进路径是上游以 id-only update 预翻 PENDING 再进入本函数（need_save=False），
            # 其并发去重在各自的预翻点加 status 守卫。
            need_save = task.status != TaskStatus.PENDING or task.sub_status == SubStatus.DRAFT
            if need_save:
                origin = task.status
                updates: dict[str, Any] = {"updated_at": _now()}
                if task.status != TaskStatus.PENDING:
                    updates["status"] = TaskStatus.PENDING
                # sub_status=DRAFT 表示"用户未确认执行"，与"已进入投递流"互斥；这里同步重置，
                # 保证后续 worker 上报 sub_status=running 时的迁移检查（DRAFT → RUNNING 非法）不会拒绝
                if task.sub_status == SubStatus.DRAFT:
                    updates["sub_status"] = SubStatus.PENDING
                if not await Task.filter(id=task.id, status=origin).update(**updates):
                    logger.info("任务 %s 已被并发提交抢占（origin=%s），本次跳过", task.id, origin)
                    raise TaskStateConflictError(task.id, str(origin))
                if "status" in updates:
                    task.status = TaskStatus.PENDING
                if "sub_status" in updates:
                    task.sub_status = SubStatus.PENDING

            await enter_pending_pool(task.id, [TaskStatus.PENDING.value], reset_submitted_at=True)
            await poke_task_server()  # best-effort：失败只损失延迟
            logger.info("任务 %s 已入待领取池（type=%s）", task.id, task.task_type)
            return True
        except TaskStateConflictError:
            raise
        except Exception as e:  # noqa: BLE001
            # 到这里只可能是入池的 UPDATE 本身失败（DB 故障）。不回滚：没有"送达"这个动作，
            # 入池失败 = 状态没变 = 任务仍在原状态，用户重试提交即可
            logger.error("任务 %s 入待领取池失败: %s", task.id, e, exc_info=True)
            return False

    @classmethod
    async def _ensure_children_complete(cls, task: Task, step: Optional[str] = None) -> None:
        """聚合闸门：进入聚合步骤前，该步骤的每个子任务都须 COMPLETED。

        ``step`` 缺省取已落库的 ``task.sub_step``；预翻状态前的前置校验须显式传"目标步"
        （此时 sub_step 仍是旧值）。按 ``parent_step`` 限定到"服务当前步骤"的子任务，
        避免其它步骤 fan-out 的同 tag 子任务被错误计入本步就绪判定。

        类型登记了 ``expected_child_tags`` 时，缺失的子任务也算未就绪；否则只检查已存在的。
        """
        step = step if step is not None else task.sub_step
        spec = get_task_type(task.task_type)
        expected = list(spec.expected_child_tags(task, step)) if spec.expected_child_tags else []
        children = await cls.get_children(task.id, parent_step=step)
        status_by_tag = {c.tag: c.status for c in children}  # 同名取最后创建的（覆盖重建场景）

        pending: list[str] = []
        failed: list[str] = []
        tags = list(dict.fromkeys(expected + [c.tag for c in children]))
        for tag in tags:
            st = status_by_tag.get(tag)
            if st == TaskStatus.COMPLETED:
                continue
            if st in (TaskStatus.FAILED, TaskStatus.CANCELLED):
                failed.append(tag)
            else:
                pending.append(tag)
        if pending or failed:
            raise ChildrenNotReadyError(step=step, pending=pending, failed=failed, total=len(tags))

    # ==================== 子任务：创建 / 挂起 / 准入 ====================

    @classmethod
    async def create_child(
        cls, parent: Task, task_type: str, *, tag: str = "", parent_step: str = "",
        files: Optional[list[dict]] = None, params_text: Optional[str] = None, name: str = "",
    ) -> Task:
        """创建一个待自动准入的子任务（``DRAFT + auto_post=1``）。

        子任务继承父任务的用户与组织；不直接提交——由补偿器的准入巡检按 per-user 子任务额度择机投递
        （直发 ``submit_task`` 会豁免并发闸、击穿额度）。
        """
        return await cls.create_task(
            task_type, files=files or [{"name": tag or "child"}], params_text=params_text,
            user_id=parent.user_id, org_id=parent.org_id, name=name or parent.name,
            parent_id=parent.id, tag=tag, parent_step=parent_step, auto_post=1,
        )

    @classmethod
    async def hold_parent(cls, parent: Task) -> bool:
        """把顶层任务挂起为 ``DRAFT + auto_post=1``，等它的子任务全部终态后由巡检投递。

        供类型的 ``hold_submit`` 钩子在建完子任务之后调用。先过堆积闸（``ensure_within_waiting_parents``）。
        返回是否挂起成功（状态被并发改走返回 False）。
        """
        await ensure_within_waiting_parents(parent.user_id, exclude_task_id=parent.id)
        updated = await Task.filter(
            id=parent.id, status__in=[TaskStatus.NONE, TaskStatus.DRAFT],
        ).update(status=TaskStatus.DRAFT, auto_post=1, updated_at=_now())
        return bool(updated)

    @classmethod
    async def _try_admit_child(
        cls, child: Task, *, cap_cache: Optional[dict] = None, inflight_cache: Optional[dict] = None
    ) -> str:
        """对**单个**子任务做一次准入尝试——准入策略的唯一权威。

        三条投递路径共用本函数，任何投递子任务的代码都必须经此，绝不对子任务直发 ``submit_task``：
        fan-out（即时准入）、retry 子任务（防穿透重新挂队）、补偿器 ``admit_pending_children``（排空 backlog）。

        只有一道闸：per-user 子任务额度（``count_active_children < child_concurrency_limit``）。
        "全局背压"那类闸不需要——主动领取之后"不淹没 worker"由"没空槽就不领"这个结构性质提供。

        过闸后：原子认领 DRAFT→PENDING → ``submit_task`` → 成功清 ``auto_post=0``。

        Returns:
            ``admitted`` 已投递 / ``deferred_quota`` 额度满，原样留队 / ``skipped`` 状态不符或认领失败（并发）/
            ``failed`` ``submit_task`` 未成功。⚠️ 这里的"失败"是**准入记账没走完**，不是任务失败：认领那步已把它翻进
            PENDING，它就待在池里、worker 照常领走执行，用户看不到失败也无需重试。残留两项且无副作用：
            ``auto_post`` 停在 1（准入要求 DRAFT，不会再选中）；``level`` 仍为 0（进池没走完，
            付费用户的这个子任务会排进免费档那组，组内位置由 rand_rank 正常决定）。
        """
        if not child.parent_id or child.status != TaskStatus.DRAFT or child.auto_post != 1 or child.is_delete:
            return "skipped"

        uid = child.user_id
        if cap_cache is not None and uid in cap_cache:
            cap, inflight = cap_cache[uid], inflight_cache[uid]
        else:
            level = await get_user_resolver().level(uid) if uid else 0
            cap = get_quota_policy().child_concurrency_limit(level)
            inflight = await count_active_children(uid)
            if cap_cache is not None:
                cap_cache[uid], inflight_cache[uid] = cap, inflight
        if inflight >= cap:
            return "deferred_quota"

        try:
            # 原子认领：仅当仍是 DRAFT+auto_post=1+未软删才接管，防与取消 / 删除竞态。走 claim_auto_post_child
            # 而不是手写 update：这条 UPDATE 还要写分发排序键，其公式只能在 SQL 里求值且必须与进池共用同一份
            if not await claim_auto_post_child(child.id):
                return "skipped"
            fresh = await cls.get_task(child.id)
            if not fresh:
                return "skipped"
            if await cls.submit_task(fresh):
                await Task.filter(id=fresh.id).update(auto_post=0)
                if inflight_cache is not None and uid in inflight_cache:
                    inflight_cache[uid] += 1
                return "admitted"
            return "failed"  # 不回滚，见 docstring
        except Exception as e:  # noqa: BLE001
            logger.error("[Admit] 子任务准入异常 child=%s: %s", child.id, e, exc_info=True)
            return "failed"

    @classmethod
    async def admit_pending_children(cls) -> int:
        """补偿器准入巡检：把等待中的 auto_post 子任务在 per-user 额度内投递。返回本轮投递数。

        选取 ``parent_id != '' AND status=DRAFT AND auto_post=1``，按 created_at FIFO。**不加 limit**：
        否则某用户的大 backlog 占据 FIFO 前段（都因额度满被跳过）会把窗口吃光，后面未满额用户的子任务饿死。
        额度满只跳过该用户，不能 break。
        """
        waiting = await Task.filter(
            status=TaskStatus.DRAFT, auto_post=1, is_delete=False,
        ).filter(~Q(parent_id="")).order_by("created_at")
        if not waiting:
            return 0
        cap_cache: dict[str, int] = {}
        inflight_cache: dict[str, int] = {}
        admitted = 0
        for child in waiting:
            if await cls._try_admit_child(child, cap_cache=cap_cache, inflight_cache=inflight_cache) == "admitted":
                admitted += 1
        if admitted:
            logger.info("自动准入子任务: 本轮投递 %d 个", admitted)
        return admitted

    @classmethod
    async def readmit_child(cls, child: Task) -> Task:
        """把一个子任务重新挂回准入闸门（retry 防守专用）。

        retry 不得对子任务直发 ``submit_task``（豁免并发闸 → 击穿额度）。本方法把子任务重置回
        ``DRAFT + auto_post=1`` 后走 ``_try_admit_child``：额度内即时直投、超额则留队。
        """
        await Task.filter(id=child.id).update(
            status=TaskStatus.DRAFT, auto_post=1, worker_id="", lease_until=None,
            lease_seq=F("lease_seq") + 1,  # 同 retry：离开 QUEUED/RUNNING 即无租户，且必须 bump
            updated_at=_now(),
        )
        fresh = await cls.get_task(child.id)
        if fresh:
            await cls._try_admit_child(fresh)
            fresh = await cls.get_task(child.id)
        return fresh or child

    @classmethod
    async def admit_waiting_parents(cls) -> int:
        """准入巡检的另一半：把子任务已全部终态的顶层任务投递出去。返回本轮投递数。

        选的是**另一侧**：``parent_id == ''``。两个选择集靠 parent_id 严格不相交。并发名额在这里才校验——
        由 ``submit_task`` 自己那道闸完成，本方法不重复：等待期不占名额是设计意图，而投递那一刻它就要占了。
        名额满时 ``submit_task`` 抛 ``QuotaExceededError``，本轮跳过、下一轮再试。
        """
        waiting = await Task.filter(waiting_parent_filter()).order_by("created_at")
        if not waiting:
            return 0
        promoted = 0
        for parent in waiting:
            try:
                if await cls._promote_waiting_parent(parent):
                    promoted += 1
            except Exception as e:  # noqa: BLE001 —— 逐个兜底：一个父任务的异常不该让别人的任务饿死
                logger.error("[Admit] 父任务提升异常 task=%s: %s", parent.id, e, exc_info=True)
        if promoted:
            logger.info("自动投递等待中的父任务: 本轮 %d 个", promoted)
        return promoted

    @classmethod
    async def _promote_waiting_parent(cls, parent: Task) -> bool:
        children = await cls.get_children(parent.id)
        if any(c.status in _CHILD_UNFINISHED for c in children):
            return False
        if not children:
            # 子任务不见了（被硬删 / 软删）。再等下去就是永远等，直接投递
            logger.warning("等待中的父任务找不到子任务，直接投递: task=%s", parent.id)

        # 先 submit、成功了再清 auto_post：若 submit 因名额满抛出，auto_post 已被清掉就再也不会被巡检选中，
        # 任务永久停在 DRAFT。
        # 同时要避免 hold_submit 钩子再把它挂起：子任务已终态，钩子应当自己判断"无需再挂"。
        try:
            ok = await cls.submit_task(parent)
        except QuotaExceededError:
            logger.debug("[Admit] 父任务 %s 名额满，下一轮再试", parent.id)
            return False
        if not ok:
            return False
        await Task.filter(id=parent.id).update(auto_post=0)
        return True

    # ==================== 取消 / 重试 / 改写 / 重做 ====================

    @classmethod
    async def cancel_task(cls, task: Task) -> bool:
        """原子取消。只在**确有正在进行 / 待投的计算**时才取消，返回是否取消成功。

        多阶段任务额外要求 ``sub_status ∈ {pending, running}``。若 worker 恰好先完成、sub_status 已是 SUCCESS
        （步骤完成、等用户追加）或 DRAFT（编辑窗口），则无正在进行的计算，取消无意义——原子 0 行、不取消。
        同步写 ``sub_status=DRAFT``：把 CANCELLED 定位为"暂停态 + 编辑窗口"，sub_step / steps 保留（断点信息）。
        与 update_step_status 的原子写互斥：cancel 先提交 → worker 的 stale 步骤更新 0 行作废。
        """
        q = Task.filter(id=task.id, status__in=list(ACTIVE_STATUSES))
        if get_stage_config(task.task_type).is_multi_stage():
            q = q.filter(sub_status__in=[SubStatus.PENDING, SubStatus.RUNNING])
        updated = await q.update(status=TaskStatus.CANCELLED, sub_status=SubStatus.DRAFT, updated_at=_now())
        if updated:
            await emit_task_event(task.id, EVENT_CANCELLED)
        return bool(updated)

    @classmethod
    def _check_retry_budget(cls, task: Task) -> None:
        if (task.retry or 0) >= task.allowed_retries:
            raise ValueError(f"重试次数已达上限 (retry={task.retry}, allowed={task.allowed_retries})")

    @classmethod
    def _merge_params(cls, task: Task, steps_kept: list[str], step: str, incoming: Optional[Any]) -> str:
        """把调用方传入的参数并入已有参数（类型可登记归一化钩子）。"""
        if not incoming:
            return task.params_text
        incoming_dict = incoming if isinstance(incoming, dict) else parse_params(incoming)
        hook = get_task_type(task.task_type).normalize_params
        if hook is not None:
            incoming_dict = hook(steps_kept, step, incoming_dict)
        return json.dumps({**parse_params(task.params_text), **incoming_dict}, ensure_ascii=False)

    @classmethod
    async def retry_task(cls, task: Task, params_text: Optional[Any] = None, step: Optional[str] = None) -> Task:
        """重试失败 / 取消的任务：立即翻 PENDING 排队。

        Raises:
            ValueError: 状态 / 重试次数 / 步骤不合法
        """
        if task.status not in (TaskStatus.FAILED, TaskStatus.CANCELLED):
            raise ValueError(f"仅 FAILED/CANCELLED 状态可重试，当前: {task.status}")
        cls._check_retry_budget(task)

        config = get_stage_config(task.task_type)
        steps_list = _loads(task.steps, [])
        if config.is_multi_stage():
            token = step or task.sub_step or ""
            if not token:
                initial = config.step_transitions.get("", frozenset())
                if len(initial) != 1:
                    raise ValueError(f"任务类型 {task.task_type} 初始步骤不唯一，retry 必须显式指定 step")
                token = next(iter(initial))
            retry_step, trunc_idx = resolve_step_token(task.task_type, steps_list, token)
            if not config.is_valid_step(retry_step):
                raise ValueError(f"非法步骤名: {retry_step}（支持: {', '.join(sorted(config.step_names))}）")
            # 命中轨迹（用户回退到更早步骤）→ 丢弃该步及之后；None（典型：失败时未完成的当前步）→ 保留全部
            new_steps = steps_list[:trunc_idx] if trunc_idx is not None else steps_list
        else:
            retry_step, new_steps = "", []  # 单阶段：sub_step 无业务语义，步骤列表重置

        merged = cls._merge_params(task, new_steps, retry_step, params_text)
        # 提交前置校验在消耗 retry / 翻 PENDING 之前：失败抛 400/403/409，任务原样停在 FAILED/CANCELLED
        await cls.precheck_submittable(task, retry_step, params_text=merged)

        new_result, orphans = prune_result_path(task.task_type, task.result_path, new_steps)
        await Task.filter(id=task.id).update(
            status=TaskStatus.PENDING, params_text=merged, steps=json.dumps(new_steps),
            # log_path / result_path 与 steps 同一次写入裁剪：不清的话，前端在新一轮推出日志前一直显示上一轮内容，
            # 更要紧的是 worker 恢复时会把上一代的下载地址当当代产物取回来
            log_path=prune_log_path(task.task_type, task.log_path, new_steps), result_path=new_result,
            sub_step=retry_step, sub_status=SubStatus.PENDING, retry=(task.retry or 0) + 1,
            last_heartbeat=0, error_message="",
            step_message="",   # 上一轮的进度文案必须一起清掉，否则用户以为卡住了去点取消
            requeues=0,        # 用户主动重试 = 新的一轮，"连续无进展"清账
            # 让任务重回"无租户"：三个字段缺一不可。lease_seq += 1 最容易漏——上一轮 worker 的在途回调
            # 不 bump 的话仍然匹配，会写进用户刚重置的这一轮
            worker_id="", lease_until=None, lease_seq=F("lease_seq") + 1, updated_at=_now(),
        )
        # 库已裁剪后再删文件（顺序不可反）。失败仅记日志，残留由存储侧的保留期清理兜底
        await cls._delete_orphans(task.id, orphans)

        fresh = await cls.get_task(task.id)
        if fresh is not None:
            await enter_pending_pool(fresh.id, [TaskStatus.PENDING.value], reset_submitted_at=True)
            await poke_task_server()
        return await cls.get_task(task.id)

    @classmethod
    async def revise_task(cls, task: Task) -> Task:
        """FAILED / CANCELLED → DRAFT，消耗 1 次 retry。

        与 retry 互补：retry 一锤子改参立即排队；revise 回到草稿，用户慢慢编辑再提交。
        保留断点与原参数；重置心跳 / 错误 / 进度文案；``sub_status=DRAFT`` 是前端"可编辑"UI 的严格触发条件。
        """
        if task.status not in (TaskStatus.FAILED, TaskStatus.CANCELLED):
            raise ValueError(f"仅 FAILED/CANCELLED 状态可转回 DRAFT，当前: {task.status}")
        cls._check_retry_budget(task)
        await Task.filter(id=task.id).update(
            status=TaskStatus.DRAFT, sub_status=SubStatus.DRAFT, retry=(task.retry or 0) + 1,
            last_heartbeat=0, error_message="", step_message="", requeues=0,
            worker_id="", lease_until=None, lease_seq=F("lease_seq") + 1, updated_at=_now(),
        )
        return await cls.get_task(task.id)

    @classmethod
    async def redo_task(cls, task: Task, step: str, params_text: Optional[Any] = None) -> Task:
        """重做已完成的步骤——**仅开放编辑窗口，不立即投递**。

        等价于"放弃指定步骤的当前结果，回到该步的编辑态"：截断 steps 到该步之前，
        ``sub_step=step, sub_status=DRAFT``，status 置为 RUNNING，消耗 1 retry；后续 PATCH 改参并提交。
        只要求 step 在合法集合内且**确实已完成过**（在 steps 里）；不走转换表——用户重做已完成步骤，
        转换合法性已隐含成立。失败步本身（不在 steps）的重跑走 retry / revise。
        """
        if task.status == TaskStatus.RUNNING:
            if task.sub_status not in (SubStatus.SUCCESS, SubStatus.DRAFT):
                raise ValueError(f"仅步骤成功或草稿态可重做，当前 sub_status: {task.sub_status}")
        elif task.status not in (TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.DRAFT):
            raise ValueError(f"仅 RUNNING（成功/草稿）/ FAILED / CANCELLED / DRAFT 状态可重做步骤，当前: {task.status}")

        config = get_stage_config(task.task_type)
        if not config.is_multi_stage():
            raise ValueError(f"任务类型 {task.task_type} 不支持步骤重做（单阶段）")

        steps_list = _loads(task.steps, [])
        step, trunc_idx = resolve_step_token(task.task_type, steps_list, step)
        if not config.is_valid_step(step):
            raise ValueError(f"非法步骤名: {step}（支持: {', '.join(sorted(config.step_names))}）")
        if trunc_idx is None:
            raise ValueError(f"步骤 {step} 尚未执行，无法重做")
        cls._check_retry_budget(task)

        new_steps = steps_list[:trunc_idx]
        merged = cls._merge_params(task, new_steps, step, params_text)
        new_result, orphans = prune_result_path(task.task_type, task.result_path, new_steps)
        await Task.filter(id=task.id).update(
            status=TaskStatus.RUNNING, params_text=merged, steps=json.dumps(new_steps),
            log_path=prune_log_path(task.task_type, task.log_path, new_steps), result_path=new_result,
            sub_step=step, sub_status=SubStatus.DRAFT, retry=(task.retry or 0) + 1,
            last_heartbeat=0, requeues=0,
            # redo 是唯一允许在 status=RUNNING 上执行的修订入口，但它要求 sub_status ∈ {SUCCESS, DRAFT}——
            # 即"刚跑完一步、正等用户"，worker 早已返回、释放槽位、注销心跳，租约也在 complete 里主动释放，
            # 所以不会踩到活跃租约。仍然照清不误：不变量要靠每条路径各自成立，不能靠"上游应该已经清了"
            worker_id="", lease_until=None, lease_seq=F("lease_seq") + 1, updated_at=_now(),
        )
        await cls._delete_orphans(task.id, orphans)
        return await cls.get_task(task.id)

    @classmethod
    async def _delete_orphans(cls, task_id: str, urls: list[str]) -> None:
        """孤儿产物的清理钩子位：目前交给存储服务的批量删除接口。失败只记日志。"""
        if not urls:
            return
        try:
            await storage_client.delete_results(task_id, urls)
        except Exception as e:  # noqa: BLE001
            logger.warning("清理孤儿产物失败 task=%s: %s", task_id, e)

    # ==================== 删除 ====================

    @classmethod
    async def purge_children(cls, parent_id: str, tag: Optional[str] = None, parent_step: Optional[str] = None) -> int:
        """【硬删除】父任务的子任务（连同存储目录），含软删除的。返回删除数。

        用于父任务级联删除（tag=None 删全部），以及同名子任务重新计算前彻底清旧任务。
        ``tag`` 与 ``parent_step`` 同传取交集，精确定位而不误伤同 tag 其它步骤的子任务。
        """
        if not parent_id:
            return 0
        q = Task.filter(parent_id=parent_id)
        if tag is not None:
            q = q.filter(tag=tag)
        if parent_step is not None:
            q = q.filter(parent_step=parent_step)
        children = await q
        for c in children:
            await cls._purge_files(c.id)
            await c.delete()
        if children:
            logger.info("硬删除子任务: parent=%s tag=%s count=%d", parent_id, tag, len(children))
        return len(children)

    @classmethod
    async def soft_cancel_children(cls, parent_id: str, tag: str) -> int:
        """软删除某 tag 的子任务：置 ``cancelled + is_delete`` 让 worker 感知终止，留待后续物理回收。"""
        if not parent_id or not tag:
            return 0
        children = await Task.filter(parent_id=parent_id, tag=tag, is_delete=False)
        for c in children:
            # 终态释放租约：否则已取消的子任务会带着指向未来的 lease_until，
            # 而它的 worker 正靠心跳里的"非 running"自行停下
            await Task.filter(id=c.id).update(
                status=TaskStatus.CANCELLED, is_delete=True, lease_until=None, updated_at=_now()
            )
            storage_client.delete_task_background(c.id)
        return len(children)

    @classmethod
    async def delete_task(cls, task: Task) -> None:
        """级联硬删除任务：子任务 → 任务行 → 存储。

        记录删掉**之后**再删文件——顺序不能反：先删文件而记录删失败，记录就指向了不存在的文件。
        本地与远端各删各的、都要做：本地 ``tasks/<id>/`` 是用户上传的输入文件，远端 ``tasks/<id>/results/``
        是 worker 上传的产物。同机部署时恰好是同一棵目录树，分离后是两台机器上的两棵。
        调用方负责事先判断 ``is_task_active``（在飞任务不可删，避免悬空回调）。
        """
        await cls.purge_children(task.id)
        await task.delete()
        await cls._purge_files(task.id)

    @classmethod
    async def _purge_files(cls, task_id: str) -> None:
        try:
            storage_service.store.delete_prefix(f"tasks/{task_id}")  # 整目录 rmtree（含符号链接）
        except Exception as e:  # noqa: BLE001
            logger.warning("删除任务本地存储失败: task=%s error=%s", task_id, e)
        storage_client.delete_task_background(task_id)  # 即发即忘：用户删任务不该因存储服务抖动而失败

    # ==================== worker 回调：状态 / 完成 / 失败 / 步骤 / 字段 ====================

    @classmethod
    async def update_status(cls, task: Task, new_status: TaskStatus, error_message: str = "") -> None:
        """更新任务状态（带状态机校验，单次 save）。

        迁入 RUNNING 时顺带写入 ``start_time``（本轮计算起点）：worker 分布在多台机器、时钟与时区各异，
        所以时间基准由服务端单一生成，不接受 worker 传值。
        """
        extra: dict[str, Any] = {}
        if error_message:
            extra["error_message"] = error_message
        if new_status == TaskStatus.RUNNING:
            extra["start_time"] = _now()
        await task.transition_to(new_status, **extra)

    @classmethod
    async def complete_task(cls, task: Task) -> None:
        """完成任务（仅状态迁移，不写 result_path / log_path）。

        多阶段：``end_step=""`` 单阶段，completed 即终态；``end_step!=""`` 且 steps 末尾不是 end_step →
        保持 running + ``sub_status=success``（等用户追加参数），到了 end_step 才 completed。

        所有写用 ``filter(status=RUNNING).update()`` 原子守卫，避免 stale 对象或竞态导致错误迁移。
        """
        fresh = await Task.filter(id=task.id).first()
        if not fresh:
            logger.error("完成任务时任务不存在: %s", task.id)
            return
        if fresh.status != TaskStatus.RUNNING:
            logger.warning("完成任务时状态已非 RUNNING: task=%s status=%s", task.id, fresh.status)
            return

        steps_list = _loads(fresh.steps, [])
        last_step = steps_list[-1] if steps_list else ""

        if fresh.end_step and last_step != fresh.end_step:
            updated = await Task.filter(id=task.id, status=TaskStatus.RUNNING).update(
                sub_status=SubStatus.SUCCESS, sub_step="",
                # 🔴 必须在此释放租约：worker 跑完这一步后 execute() 就返回了——槽位释放、心跳注销，
                # 没有任何人在给这个任务续租。不释放的话租约会自然到期，而回收判据是
                # `status IN (queued,running) AND lease_until < now()`——它会把一个正等着用户追加参数的任务
                # 当成"worker 失联"回收掉，再被某个 worker 领走重跑。
                # 选择"释放"而不是"在回收循环里加排除条件"，是因为它确实没有租户：判据来自数据本身，
                # 比多写一条排除条件更不容易在将来被漏掉。worker_id 刻意保留（排查线索）。
                lease_until=None, requeues=0,  # 推进了一步 → "连续无进展"清账
                updated_at=_now(),
            )
            if updated:
                await emit_task_event(task.id, EVENT_STEP_DONE, step=last_step)
            else:
                logger.warning("多阶段中间步更新失败（状态已变更）: task=%s", task.id)
            return

        updated = await Task.filter(id=task.id, status=TaskStatus.RUNNING).update(
            status=TaskStatus.COMPLETED, sub_status=SubStatus.SUCCESS,
            lease_until=None,  # 终态释放租约（worker_id 保留作排查线索）
            requeues=0, updated_at=_now(),
        )
        if updated:
            await emit_task_event(task.id, EVENT_COMPLETED)
        else:
            logger.warning("完成任务时状态已变更，跳过: task=%s", task.id)

    @classmethod
    async def fail_task(cls, task: Task, error_message: str) -> None:
        """失败任务（原子更新，强制 status / sub_status 双写 FAILED）。

        不变量：``status=FAILED ⇒ sub_status=FAILED``（成对终态）。filter 接受 RUNNING / FAILED 两态：
        上游 ``update_step_status(sub_status=FAILED)`` 可能已把 status 翻成 FAILED，此处仍需补写
        error_message 与对称字段，保证进程级错误 + 日志摘要必达。
        """
        updated = await Task.filter(
            id=task.id, status__in=[TaskStatus.RUNNING, TaskStatus.FAILED],
        ).update(
            status=TaskStatus.FAILED, sub_status=SubStatus.FAILED, error_message=error_message,
            # 终态释放租约。worker_id 刻意保留——"这个任务在哪台机器上失败的"是首要排查线索
            lease_until=None, updated_at=_now(),
        )
        if not updated:
            logger.warning("失败任务时状态非 RUNNING/FAILED: task=%s", task.id)
        else:
            await emit_task_event(task.id, EVENT_FAILED, message=error_message or "")

    @classmethod
    async def update_step_status(
        cls, task: Task, steps: Optional[list[str]] = None, sub_step: Optional[str] = None,
        sub_status: Optional[str] = None, step_message: Optional[str] = None,
    ) -> None:
        """更新步骤进度。仅 ``status=RUNNING`` 时接受写入（防御 stale worker 回调）。

        Raises:
            ValueError: 非法写入条件（状态不对 / step_message 守门不过 / 非法子状态迁移）
        """
        if task.status != TaskStatus.RUNNING:
            raise ValueError(f"步骤进度仅 RUNNING 状态可写入，当前状态: {task.status}")

        # step_message 守门：必须显式带 sub_step 且与当前任务一致。防止上一步骤残留的延迟请求
        # 把旧消息写到新步骤上（如上一步的进度覆盖到下一步）。校验须在 task.sub_step 被覆盖前完成
        if step_message is not None:
            if sub_step is None:
                raise ValueError("写入 step_message 必须同时传入 sub_step")
            if sub_step != task.sub_step:
                raise ValueError(f"step_message 守门：sub_step 不匹配（请求={sub_step!r}, 当前={task.sub_step!r}）")

        fields: dict[str, Any] = {"updated_at": _now()}
        if steps is not None:
            fields["steps"] = json.dumps(steps)
        if sub_step is not None:
            fields["sub_step"] = sub_step

        if sub_status is not None:
            new_sub = SubStatus(sub_status)
            cur = task.sub_status
            if cur == SubStatus.PENDING:
                valid = new_sub in (SubStatus.RUNNING, SubStatus.SUCCESS)
            elif cur == SubStatus.RUNNING:
                valid = new_sub in (SubStatus.SUCCESS, SubStatus.FAILED)
            elif cur == SubStatus.SUCCESS:
                valid = new_sub in (SubStatus.PENDING, SubStatus.DRAFT)  # 用户 PATCH 后重新投递 / 存草稿
            elif cur == SubStatus.DRAFT:
                valid = new_sub in (SubStatus.PENDING, SubStatus.DRAFT)  # 用户确认执行 / 修改草稿
            else:
                valid = False  # failed 为终态
            if not valid:
                raise ValueError(f"非法步骤状态迁移：{cur} → {new_sub}")
            fields["sub_status"] = new_sub
            # 不变量：sub_status=FAILED ⇒ status=FAILED。子步骤失败即整体失败：worker 进程已经退出，
            # 把 status 留在 RUNNING 会被补偿器误判为活任务并触发无效重投
            if new_sub == SubStatus.FAILED:
                fields["status"] = TaskStatus.FAILED

        if step_message is not None:
            fields["step_message"] = step_message

        # 原子条件写入：仅当任务仍为 RUNNING 才落库（与 cancel / complete / fail 互斥）。
        # 用 task.save(update_fields) 按 PK 局部写、守卫读内存 status 是 TOCTOU：worker 步骤回调与用户 cancel
        # 并发时，cancel 已把 status→CANCELLED，stale 的本调用仍过守卫、只写 steps/sub_status → 状态错配
        updated = await Task.filter(id=task.id, status=TaskStatus.RUNNING).update(**fields)
        if not updated:
            logger.warning("步骤更新被丢弃：任务已非 RUNNING（并发 cancel/complete/fail），task=%s", task.id)
        elif fields.get("status") == TaskStatus.FAILED:
            await emit_task_event(task.id, EVENT_FAILED, step=str(fields.get("sub_step") or ""),
                                  message=str(step_message or ""))

    @classmethod
    async def update_fields_with_mode(cls, task: Task, fields: dict[str, str], mode: str = "overwrite") -> dict[str, str]:
        """更新 result_path / log_path：``overwrite`` 直接覆盖，``merge`` 按 key 深度合并。"""
        kwargs: dict[str, Any] = {}
        for name, value in fields.items():
            kwargs[name] = merge_field_path(getattr(task, name, ""), value) if mode == "merge" else value
        result = dict(kwargs)
        kwargs["updated_at"] = _now()
        await Task.filter(id=task.id).update(**kwargs)
        for k, v in kwargs.items():
            setattr(task, k, v)
        return result

    # ==================== 用户编辑（PATCH）====================

    @classmethod
    def editable_case(cls, task: Task) -> Optional[str]:
        """任务当前处于哪种可编辑窗口；不可编辑返回 None。

        ``draft`` 提交前（含 none）· ``waiting`` 多阶段、等用户追加参数 · ``window`` 草稿 / 取消后的编辑窗口。
        """
        if task.status in (TaskStatus.DRAFT, TaskStatus.NONE, ""):
            return "draft"
        if task.status == TaskStatus.RUNNING and task.sub_status == SubStatus.SUCCESS and task.sub_step == "":
            return "waiting"
        if task.status in (TaskStatus.RUNNING, TaskStatus.CANCELLED) and task.sub_status == SubStatus.DRAFT:
            return "window"
        return None

    @classmethod
    async def patch_task(
        cls, task: Task, *, params: Optional[Any], step: Optional[str], submit: bool
    ) -> Task:
        """编辑 / 推进任务（调用方已校验归属与 auto_post 守卫）。

        Raises:
            ValueError: 状态不可编辑 / 缺少必需字段；``StepValidationError``；各业务异常
        """
        case = cls.editable_case(task)
        if case is None:
            raise ValueError(
                f"当前状态不可编辑（status={task.status}, sub_status={task.sub_status}, sub_step={task.sub_step!r}）"
            )
        steps_list = _loads(task.steps, [])
        existing = parse_params(task.params_text)

        if case == "draft":
            if step is not None:
                validate_step_transition(task.task_type, steps_list, step)
            if params is not None:
                incoming = params if isinstance(params, dict) else parse_params(params)
                validate_task_params(task.task_type, incoming)
                hook = get_task_type(task.task_type).normalize_params
                if hook is not None:
                    incoming = hook(steps_list, step or task.sub_step or "", incoming)
                existing = {**existing, **incoming}
            task.params_text = json.dumps(existing, ensure_ascii=False)
            if step is not None:
                task.sub_step = step
            if submit:
                await task.save(update_fields=["params_text", "sub_step", "updated_at"])
                if not await cls.submit_task(task):
                    raise RuntimeError("入待领取池失败")
                return await cls.get_task(task.id)
            if params is None and step is None:
                raise ValueError("至少需要提供 params_text 或 step 之一")
            await task.save(update_fields=["params_text", "sub_step", "updated_at"])
            return await cls.get_task(task.id)

        # waiting / window：多阶段推进，必须带 step
        if not step:
            raise ValueError("多阶段推进必须提供 step")
        if params is None and submit:
            raise ValueError("提交下一步必须提供 params_text")
        config = get_stage_config(task.task_type)
        if len(steps_list) >= config.max_steps:
            raise ValueError(f"步骤数已达上限 {config.max_steps}")
        validate_step_transition(task.task_type, steps_list, step)

        incoming = {}
        if params is not None:
            incoming = params if isinstance(params, dict) else parse_params(params)
            validate_task_params(task.task_type, incoming)
            hook = get_task_type(task.task_type).normalize_params
            if hook is not None:
                incoming = hook(steps_list, step, incoming)
        merged = json.dumps({**existing, **incoming}, ensure_ascii=False)

        if not submit:  # 保存草稿：合并参数，sub_status=draft，保持原 status 不投递
            await Task.filter(id=task.id).update(
                params_text=merged, sub_step=step, sub_status=SubStatus.DRAFT, updated_at=_now()
            )
            return await cls.get_task(task.id)

        cancelled_resubmit = task.status == TaskStatus.CANCELLED
        if cancelled_resubmit:
            cls._check_retry_budget(task)  # CANCELLED 草稿提交消耗 retry，避免被滥用为"无成本无限重试"

        await cls.precheck_submittable(task, step, params_text=merged)
        new_result, orphans = prune_result_path(task.task_type, task.result_path, steps_list)
        update: dict[str, Any] = dict(
            status=TaskStatus.PENDING, params_text=merged, sub_step=step, sub_status=SubStatus.PENDING,
            log_path=prune_log_path(task.task_type, task.log_path, steps_list), result_path=new_result,
            # 让任务重回"无租户"。running→pending 时任务可能还持有活跃租约、worker 还在跑（那台 worker 会在下一跳
            # 心跳收到 revoked 而自行停下）；即便是 success→pending，上一步的 worker 也可能有在途回调
            worker_id="", lease_until=None, lease_seq=F("lease_seq") + 1,
            requeues=0, updated_at=_now(),
        )
        if cancelled_resubmit:
            update.update(retry=(task.retry or 0) + 1, error_message="")
        await Task.filter(id=task.id).update(**update)
        await cls._delete_orphans(task.id, orphans)

        fresh = await cls.get_task(task.id)
        if not await cls.submit_task(fresh):
            raise RuntimeError("入待领取池失败")
        return await cls.get_task(task.id)


def _with(task: Task, **overrides) -> Task:
    """返回一个带覆盖字段的**未入库**副本，供前置校验按 prospective 值判定。"""
    clone = Task(**{f: getattr(task, f) for f in task._meta.fields_map if f in task.__dict__})
    for k, v in overrides.items():
        setattr(clone, k, v)
    return clone


task_service = TaskService()

# ==================== 补偿器钩子注册 ====================
# 准入巡检与失败通知的具体动作属于任务服务，import 本模块即完成注册（钩子注册表在模块级，
# 与补偿器实例无关，见 webx.services.reconciler）。
register_sweep("admit-children", TaskService.admit_pending_children)
register_sweep("admit-waiting-parents", TaskService.admit_waiting_parents)


async def _emit_failed_events(task_ids: list[str], reason: str) -> None:
    for tid in task_ids:
        await emit_task_event(tid, EVENT_FAILED, message=reason)


register_failed_hook(_emit_failed_events)

__all__ = ["TaskService", "task_service", "ParamsError", "parse_files", "parse_params"]
