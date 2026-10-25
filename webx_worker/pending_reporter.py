"""
PendingReporter — 待汇报事件的后台轮询消费者。

设计语义：
  调用点已经做了 5 次前台重试（result_client / callback），仍失败的事件进入
  本模块的内存队列，由 consumer 协程异步轮询单次重试，永不退出，永不放弃。

工作模型：
  - asyncio.Queue 接收新事件（来自 IPC server / 父进程 callback 失败）
  - consumer 协程：
      * await asyncio.wait_for(queue.get(), timeout=POLL_INTERVAL_SEC)
      * 有新事件 → 立刻试一次 → 失败入 retry_pool
      * 超时（POLL_INTERVAL_SEC 内无新事件）→ 扫一遍 retry_pool 试一次
  - 每个 entry 在 consumer 协程里**只重试 1 次**：失败保留下一轮再试
  - 不存在"重试上限"：队列即系统状态

生命周期：
  - worker 启动：先 restore_from_snapshot() 把上次退出时保存的事件灌回队列
  - worker 退出：snapshot_to_file() 把当前内存队列 + retry_pool dump 到文件
                 fail_task entry 不持久化（避免重启后覆盖补偿器重投后的工作）

事件结构（entry）：
  {
    "task_id": "...",
    "action": "update_step_status" / "update_task_fields" / "update_status" / "complete_task" / "fail_task",
    "payload": {...},          # HTTP body
    "context": {...},          # 发送时刻的任务快照（调试用）
    "ts": 1717000000.0,
  }

状态码分类（_try_report 内部）：
  - 200            → 成功，出队
  - 404            → 任务已不存在，出队
  - 400            → 业务规则拒绝，出队 + WARNING（重试无意义）
  - 403 + reason ∈ {not_lessee, gone, already_final}
                   → fencing 终态拒绝：**判据是响应体的 reason，不是状态码**。
                     not_lessee / gone 永久放弃，already_final 视为成功（幂等），均出队
  - 401 / 其它 403 → 确定性拒绝但不可丢，**保留在 retry_pool**（人工处理后自愈）
  - 5xx / 网络错   → 保留在 retry_pool，下一轮再试

fail_task 特殊处理：
  - 重试前先 GET /internal/tasks/{id}，若 server 状态已非 running（被补偿器
    重投到 queued 或别处接走），跳过本次 fail_task，避免覆盖正在执行的工作
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from .callback import WebCallbackClient

logger = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 20  # retry_pool 轮询间隔
SNAPSHOT_FILENAME = "pending_reports.snapshot.jsonl"

# 业务规则拒绝（不重试）的状态码
_BUSINESS_REJECT_CODES = {400, 404}

# 确定性拒绝但**不可丢**的状态码：保留在池里等人工处理后自愈。
# 401 = 令牌填错（修好即自愈）；403 在不带终态 reason 时（如 missing_fencing_token /
# bad_lease_seq）也保守保留——见 docs/worker.md 的协议缺口说明。
_AUTH_RETAIN_CODES = {401, 403}

# fencing 的三种终态拒绝（与 webx_worker/fencing.py 同一组，协议 §2）。
# 三者对本模块含义一致：**再试一万次也是同一个答案**，该出队。
_FENCE_TERMINAL = {"not_lessee", "gone", "already_final"}


def _fence_reason(resp: httpx.Response) -> str | None:
    """响应是否为 fencing 终态拒绝，是则返回 reason，否则 None。

    判据是**响应体的 reason** 而不是 403 这个状态码：Token 填错走的是 401，
    但按状态码归类会让将来任何一个别的 403 都被当成 fencing 丢掉 ——
    那正是 _AUTH_RETAIN_CODES 存在的理由（一次配置错误清空整个队列）。
    解析不出来一律 None（保守保留）。
    """
    if resp.status_code != 403:
        return None
    try:
        body = resp.json()
    except Exception:  # noqa: BLE001 —— 不是 JSON 就不是 fencing 拒绝
        return None
    if not isinstance(body, dict) or body.get("ok") is not False:
        return None
    reason = body.get("reason")
    return str(reason) if reason in _FENCE_TERMINAL else None

# action 名称（用于内部路由）
ACTION_UPDATE_STEP_STATUS = "update_step_status"
ACTION_UPDATE_TASK_FIELDS = "update_task_fields"
ACTION_UPDATE_STATUS = "update_status"
ACTION_COMPLETE_TASK = "complete_task"
ACTION_FAIL_TASK = "fail_task"


class PendingReporter:
    """汇报失败事件的内存队列 + 后台轮询消费者"""

    def __init__(self, callback: "WebCallbackClient", work_base_dir: str):
        self.callback = callback
        self.work_base_dir = Path(work_base_dir)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._retry_pool: list[dict] = []
        self._consumer_task: asyncio.Task | None = None
        self._stop_event: asyncio.Event = asyncio.Event()

    # ─── 入队 ─────────────────────────────────────────────────────

    async def enqueue(self, entry: dict) -> None:
        """父进程内部调用（callback 失败时直接入队，不走 IPC）"""
        await self._queue.put(entry)

    def enqueue_nowait(self, entry: dict) -> None:
        """IPC server 在协程外回调时用（已在 event loop 内）"""
        self._queue.put_nowait(entry)

    # ─── consumer 协程 ────────────────────────────────────────────

    async def run_loop(self) -> None:
        """启动 consumer 协程。被 worker 主入口 asyncio.create_task() 包装。"""
        logger.info(f"[Reporter] 启动，轮询间隔 {POLL_INTERVAL_SEC}s")
        try:
            while not self._stop_event.is_set():
                try:
                    # 新事件优先（最长等 POLL_INTERVAL_SEC）
                    entry = await asyncio.wait_for(
                        self._queue.get(), timeout=POLL_INTERVAL_SEC,
                    )
                except asyncio.TimeoutError:
                    # 超时：扫一遍 retry_pool
                    await self._sweep_retry_pool()
                    continue

                # 新事件立刻试一次
                await self._handle_entry(entry)
        except asyncio.CancelledError:
            logger.info("[Reporter] 收到取消信号，准备退出")
            raise
        except Exception as e:
            logger.error(f"[Reporter] consumer 协程异常: {e}", exc_info=True)
            raise

    async def _handle_entry(self, entry: dict) -> None:
        """处理单个 entry：试一次，分类结果"""
        outcome = await self._try_report(entry)
        if outcome == "success" or outcome == "drop":
            return
        # outcome == "retry"
        self._retry_pool.append(entry)

    async def _sweep_retry_pool(self) -> None:
        """扫一遍 retry_pool，每条试 1 次"""
        if not self._retry_pool:
            return
        pool = self._retry_pool
        self._retry_pool = []
        logger.debug(f"[Reporter] 轮询 retry_pool: {len(pool)} 条")
        for entry in pool:
            outcome = await self._try_report(entry)
            if outcome == "retry":
                self._retry_pool.append(entry)

    # ─── 单次重试（核心） ─────────────────────────────────────────

    async def _try_report(self, entry: dict) -> str:
        """尝试上报一次。

        Returns:
            "success" — 200，出队
            "drop"    — 4xx 业务拒绝，出队 + WARNING
            "retry"   — 5xx / 网络错 / 鉴权问题，保留在 retry_pool
        """
        action = entry.get("action", "")
        task_id = entry.get("task_id", "")
        payload = entry.get("payload", {})

        # fail_task 前置校验：避免覆盖重投后的工作
        if action == ACTION_FAIL_TASK:
            if not await self._safe_to_fail(task_id, entry):
                logger.warning(
                    f"[Reporter] fail_task 跳过 task_id={task_id}：server 状态已变"
                )
                return "drop"

        try:
            status_code, fence_reason = await self._do_post(
                action, task_id, payload, entry.get("fencing")
            )
        except Exception as e:
            logger.warning(f"[Reporter] {action} task_id={task_id} 异常: {e}")
            return "retry"

        if status_code == 200:
            logger.info(f"[Reporter] {action} task_id={task_id} 成功")
            return "success"
        if status_code in _BUSINESS_REJECT_CODES:
            logger.warning(
                f"[Reporter] {action} task_id={task_id} 被业务拒绝({status_code})，丢弃"
            )
            return "drop"
        if fence_reason:
            # fencing 终态拒绝 = **这一轮已经作废**（任务被回收 / 用户 retry /
            # 已推进到下一步），或**同一租户的重复上报已达终态**（幂等成功）。
            # 两种都是"再试一万次也是同一个答案"，必须出队。
            #
            # 这与上面那条「401/403 一律保留」的取舍不冲突：那条防的是
            # **配置错误**（Token 填错，修好即自愈），而 fencing 拒绝是
            # **数据结论**，人工介入也不会改变。判据是响应体的 reason 而非状态码，
            # 两者因此不会互相误伤。
            logger.info(
                f"[Reporter] {action} task_id={task_id} 被 fencing 拒绝"
                f"（{fence_reason}），丢弃"
            )
            return "drop"
        if status_code in _AUTH_RETAIN_CODES:
            # 只记 DEBUG：本请求走的是 callback 的同一个 httpx 客户端，已经过
            # link_health.ObservedTransport 记账并发出**聚合** ERROR 告警。
            # 这里若再逐条 ERROR，积压 N 条时每 20s 就刷 N 行同因日志，
            # 反而把真正的那一条告警淹没——与本次改动的目的正好相反。
            logger.debug(
                f"[Reporter] {action} task_id={task_id} 鉴权错误({status_code})，"
                f"保留待人工（聚合告警见 link_health）"
            )
            return "retry"
        # 5xx / 其他
        logger.warning(f"[Reporter] {action} task_id={task_id} 失败({status_code})，下轮再试")
        return "retry"

    async def _do_post(self, action: str, task_id: str, payload: dict,
                       fencing: dict | None = None) -> tuple[int, str | None]:
        """根据 action 调用 callback 客户端。

        返回 ``(状态码, fencing 拒绝原因)``；后者非 None 即「这一轮已作废，该丢」。
        单返回状态码不够：**403 有两种含义**——配置错（该保留）与 fencing 拒绝
        （该丢弃），只有响应体的 reason 分得开。

        ⚠️ `fencing` 必须由**条目**提供，不能靠 callback 客户端那个自动注入 header 的
        钩子 —— 它读的是父进程内存里的活注册表，而重放恒发生在 Lane 结束、
        令牌已被 unregister 之后（跨重启更是从盘上恢复的）。查表只会查到空，
        于是上报不带令牌、被判「不是租户」403，然后在 _AUTH_RETAIN_CODES 里
        **永久重试**：既送不出去，还把「队列长度即待送达数」这个运维信号搞废。

        令牌走 body（服务端 header / body 两条都认，协议 §2）：
        条目本来就是 JSON，令牌是它的一个字段，与 payload 同源同寿。
        """
        client = await self.callback._get_client()
        base = self.callback.base_url
        if fencing:
            payload = {**payload, **fencing}

        # action → (方法, 路径)：与 docs/worker-protocol.md §4 一致
        # （PUT /status、PATCH /steps、PATCH /fields，其余 POST）
        if action == ACTION_UPDATE_STEP_STATUS:
            url = f"{base}/internal/tasks/{task_id}/steps"
            resp = await client.patch(url, json=payload)
        elif action == ACTION_UPDATE_TASK_FIELDS:
            mode = payload.pop("_mode", "overwrite") if "_mode" in payload else "overwrite"
            url = f"{base}/internal/tasks/{task_id}/fields"
            if mode == "merge":
                url += "?mode=merge"
            resp = await client.patch(url, json=payload)
        elif action == ACTION_UPDATE_STATUS:
            url = f"{base}/internal/tasks/{task_id}/status"
            resp = await client.put(url, json=payload)
        elif action == ACTION_COMPLETE_TASK:
            url = f"{base}/internal/tasks/{task_id}/complete"
            resp = await client.post(url, json=payload or {})
        elif action == ACTION_FAIL_TASK:
            url = f"{base}/internal/tasks/{task_id}/fail"
            resp = await client.post(url, json=payload)
        else:
            logger.error(f"[Reporter] 未知 action: {action}, 丢弃")
            return 400, None  # 当作业务拒绝丢弃

        return resp.status_code, _fence_reason(resp)

    async def _safe_to_fail(self, task_id: str, entry: dict) -> bool:
        """fail_task 前置校验：仅 server 当前 status=running 时允许重试 fail_task。

        覆盖以下场景：
          - server 404（任务已被删）→ 视为安全（其实丢弃也行，这里返回 False 让 drop）
          - server status=queued（被补偿器重投）→ 不安全
          - server status=running → 安全
          - 网络故障查不到 → 保守，返回 False 让 retry，下轮重新查
        """
        result = await self.callback.get_task_status(task_id)
        if result.is_not_found:
            return False  # 任务不存在了，fail_task 没意义
        if result.is_error:
            return False  # 查不到 → 保守，下轮再查
        if not result.is_ok:
            return False
        server_status = result.task_status
        return server_status == "running"

    # ─── 启动恢复 / 退出快照 ─────────────────────────────────────

    async def restore_from_snapshot(self) -> int:
        """worker 启动时把上次保存的 snapshot 灌回队列"""
        snapshot_file = self.work_base_dir / SNAPSHOT_FILENAME
        if not snapshot_file.exists():
            return 0

        count = 0
        try:
            text = snapshot_file.read_text(encoding="utf-8")
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    await self._queue.put(entry)
                    count += 1
                except json.JSONDecodeError as e:
                    logger.warning(f"[Reporter] snapshot 行解析失败: {e}")
            snapshot_file.unlink(missing_ok=True)
            if count:
                logger.info(f"[Reporter] 已恢复 {count} 条 snapshot 事件")
        except OSError as e:
            logger.error(f"[Reporter] snapshot 读取失败: {e}")

        # 同时扫各任务目录下的 pending_reports.jsonl（子进程 IPC 降级写的兜底文件）
        count += await self._restore_per_task_files()
        return count

    async def _restore_per_task_files(self) -> int:
        """扫 {WORK_DIR}/{task_id}/pending_reports.jsonl，灌回队列后删除"""
        if not self.work_base_dir.exists():
            return 0
        count = 0
        try:
            for task_dir in self.work_base_dir.iterdir():
                if not task_dir.is_dir():
                    continue
                pending_file = task_dir / "pending_reports.jsonl"
                if not pending_file.exists():
                    continue
                try:
                    text = pending_file.read_text(encoding="utf-8")
                    for line in text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            await self._queue.put(entry)
                            count += 1
                        except json.JSONDecodeError:
                            pass
                    pending_file.unlink(missing_ok=True)
                except OSError as e:
                    logger.error(f"[Reporter] 任务级 pending 文件读取失败 {pending_file}: {e}")
        except OSError as e:
            logger.error(f"[Reporter] 扫描任务目录失败: {e}")

        if count:
            logger.info(f"[Reporter] 已恢复 {count} 条任务级降级事件")
        return count

    def snapshot_to_file(self) -> int:
        """worker 退出时把当前队列 + retry_pool dump 到 snapshot 文件。

        排除 fail_task entry —— 避免重启后误覆盖被补偿器重投的工作。
        """
        entries: list[dict] = []

        # drain queue
        while not self._queue.empty():
            try:
                entries.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        # 合并 retry_pool
        entries.extend(self._retry_pool)
        self._retry_pool.clear()

        # 过滤 fail_task
        persistable = [e for e in entries if e.get("action") != ACTION_FAIL_TASK]
        dropped_fail = len(entries) - len(persistable)
        if dropped_fail:
            logger.warning(
                f"[Reporter] snapshot 跳过 {dropped_fail} 条 fail_task entry "
                "（由 Recovery / 补偿器兜底）"
            )

        if not persistable:
            logger.info("[Reporter] snapshot 无内容")
            return 0

        try:
            self.work_base_dir.mkdir(parents=True, exist_ok=True)
            snapshot_file = self.work_base_dir / SNAPSHOT_FILENAME
            with snapshot_file.open("w", encoding="utf-8") as f:
                for entry in persistable:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            logger.info(f"[Reporter] snapshot 已写入: {len(persistable)} 条 -> {snapshot_file}")
        except OSError as e:
            logger.error(f"[Reporter] snapshot 写入失败: {e}")
        return len(persistable)

    # ─── 停止 ─────────────────────────────────────────────────────

    def request_stop(self) -> None:
        """请求 consumer 协程退出（配合 cancel 使用）"""
        self._stop_event.set()


# ─── 全局单例（由 __main__.py 启动时填充）────────────────────────────────

_GLOBAL: PendingReporter | None = None


def set_global_reporter(reporter: PendingReporter) -> None:
    """worker 主入口启动时注入全局实例。"""
    global _GLOBAL
    _GLOBAL = reporter


def get_global_reporter() -> PendingReporter | None:
    """callback.py 等模块通过此函数访问全局实例（运行时绑定，避免 import 顺序问题）。"""
    return _GLOBAL


async def enqueue_global(entry: dict) -> None:
    """模块级入队入口：callback.py 失败时调用。"""
    if _GLOBAL is None:
        logger.error(f"[Reporter] 全局实例未初始化，事件丢失: {entry.get('task_id')}/{entry.get('action')}")
        return
    await _GLOBAL.enqueue(entry)
