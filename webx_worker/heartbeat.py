"""
心跳管理器 — 多任务版

职责：
  - 为每个活跃任务启动独立心跳循环
  - 按 task_id 隔离 server_status，互不干扰
  - 支持双向心跳：解析服务端响应中的 status 字段
"""

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

from .callback import callback_client

logger = logging.getLogger(__name__)

# 动态心跳节拍（秒）：递增质数序列，跑完后封顶保持末值（爬升后保持）。
# 前期密集 → 取消信号/日志早出；稳定段恒 29s → 服务端压力可控、最坏取消延迟 ≤29s
# （优于旧版固定 30s）。质数错峰、相邻间隔互不整除，降低多 worker 心跳同相撞击概率。
_HEARTBEAT_INTERVALS: tuple[int, ...] = (5, 7, 11, 13, 17, 19, 23, 29)
# 从第几跳起触发 on_beat 日志推送：前 2 跳子进程多半尚无有效输出，跳过避免空推。
_LOG_STREAM_START_BEAT = 3

# 自我熔断的安全余量（秒）：worker 必须**严格早于**服务端回收租约才停下。
# 需覆盖「时钟/网络抖动 + killpg 真正生效所需时间」。
# ⚠️ 方向搞反等于没做：会出现「服务端已把任务租给 B、A 仍认为自己持有」的窗口，
# 正是 fencing 要消灭的那个状态。
_SELF_FENCE_MARGIN_SEC = 10.0


class HeartbeatManager:
    """多任务心跳管理器。

    每个任务有独立的 asyncio.Task 和状态字典，Lane 并发时互不干扰。
    """

    def __init__(self, interval: int = 30):
        """
        Args:
            interval: 兼容旧构造签名而保留；实际节拍由 _HEARTBEAT_INTERVALS 序列驱动，
                      本参数不再决定心跳间隔（仅作向后兼容占位）。
        """
        self.interval = interval
        self._hb_tasks: dict[str, asyncio.Task] = {}    # task_id → asyncio.Task
        self._statuses: dict[str, str] = {}             # task_id → server_status
        # task_id → 自熔时刻（本地 monotonic）。仅 lease 路径有值；
        # 由 _note_renewed 每次成功续租时按服务端告知的 lease_until 刷新。
        self._lease_deadline: dict[str, float] = {}
        # task_id → 取消信号 Event：心跳探到非 running 时 set()，run_subprocess 并发
        # await 它，被 set 即立即终止子进程（事件驱动推送，与子进程 stdout 解耦）。
        self._cancel_events: dict[str, asyncio.Event] = {}
        # task_id → 每跳成功后的回调（增量日志上传）。心跳只负责"按节拍调用"，
        # 具体上传逻辑留在 executor，保持 heartbeat 通用、与日志解耦。
        self._on_beats: dict[str, Callable[[str], Awaitable[None]]] = {}
        # on_beat 后台派发：单任务串行 + 合并最新（trailing-edge）。
        #   _beat_inflight: 当前在途的上传 Task（后台并发，不拖累心跳/取消检测）
        #   _beat_pending : 在途期间又来了新节拍 → 待补发（以最新内容为准，不取消在途请求）
        self._beat_inflight: dict[str, asyncio.Task] = {}
        self._beat_pending: set[str] = set()

    def seed_lease(self, task_id: str, remaining_ms: int | None) -> None:
        """用已知的剩余租约（ack 响应）预置自熔基准，使“从未成功续租过”的任务也有 deadline。"""
        if remaining_ms is not None and remaining_ms > 0:
            self._note_renewed(task_id, {"lease_until": 1, "lease_remaining_ms": remaining_ms})

    async def register(
        self,
        task_id: str,
        on_beat: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> None:
        """注册任务心跳（若已存在则先注销旧的）。

        Args:
            task_id: 任务 ID
            on_beat: 可选异步回调，签名 (task_id)。每次成功心跳（确认 running）后、
                     从第 _LOG_STREAM_START_BEAT 跳起被**后台派发**调用（不阻塞心跳循环），
                     用于增量日志推送。回调内部异常被吞掉（仅告警），绝不影响心跳与取消。
        """
        # 防御性：同一 task_id 重复注册时先注销旧的
        if task_id in self._hb_tasks:
            await self.unregister(task_id)

        self._statuses[task_id] = "running"
        self._cancel_events[task_id] = asyncio.Event()
        if on_beat is not None:
            self._on_beats[task_id] = on_beat
        self._hb_tasks[task_id] = asyncio.create_task(
            self._heartbeat_loop(task_id)
        )
        logger.debug(f"[心跳] 注册 task_id={task_id}")

    async def unregister(self, task_id: str) -> None:
        """注销任务心跳。"""
        task = self._hb_tasks.pop(task_id, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # 任务收尾：撤销在途/待补发的增量日志上传。注意——这与"积压不取消在途"是
        # 两码事：那条规则约束的是运行期新节拍不得打断在途请求；此处是任务已终结、
        # 终态日志由 executor 末尾 _upload_step_log 权威上传，残留的流式上传无需保留。
        self._beat_pending.discard(task_id)
        beat = self._beat_inflight.pop(task_id, None)
        if beat is not None and not beat.done():
            beat.cancel()
            try:
                await beat
            except asyncio.CancelledError:
                pass
        self._statuses.pop(task_id, None)
        self._cancel_events.pop(task_id, None)
        self._on_beats.pop(task_id, None)
        logger.debug(f"[心跳] 注销 task_id={task_id}")

    def get_cancel_event(self, task_id: str) -> Optional[asyncio.Event]:
        """取消信号 Event：run_subprocess 并发 await 它，被 set 即立即终止子进程。

        探到 404 / cancelled / 任何非 running 状态时由 _mark_non_running 触发 set()。
        未注册的任务返回 None（调用方据此退化为"不可中断地等子进程跑完"）。
        """
        return self._cancel_events.get(task_id)

    def _mark_non_running(self, task_id: str, status: str) -> None:
        """记录服务端非 running 状态并触发取消信号（推送给正在等待的 run_subprocess）。"""
        self._statuses[task_id] = status
        ev = self._cancel_events.get(task_id)
        if ev is not None:
            ev.set()

    def get_server_status(self, task_id: str) -> str:
        """获取指定任务的服务端返回状态。

        Args:
            task_id: 任务 ID

        Returns:
            服务端返回的状态（如 "running", "cancelled"），默认 "running"
        """
        return self._statuses.get(task_id, "running")

    @staticmethod
    def _interval_sequence():
        """心跳间隔生成器：按 _HEARTBEAT_INTERVALS 递增，跑完后无限保持末值。"""
        for v in _HEARTBEAT_INTERVALS:
            yield v
        while True:
            yield _HEARTBEAT_INTERVALS[-1]

    async def _fire_on_beat(self, task_id: str) -> None:
        """派发增量日志上传（单任务串行 + 合并最新 trailing-edge）。

        保留 async 签名（便于未来在派发前后扩展逻辑，调用点无需改动）；但本协程**只做派发**
        ——把真正的上传交给后台 Task（_run_on_beat），自身不 await 上传，故不阻塞心跳循环、
        不拖累取消检测。
        合并最新策略：
          - 无在途 → 立即起一个后台上传 Task。
          - 有在途未完成 → 仅标记 pending（既不并发起第二个，也不取消在途请求），
            待在途结束后用**最新**内容补发一次（_stream_log 每次读最新截断内容，天然以最新为准）。
        """
        if task_id not in self._on_beats:
            return
        inflight = self._beat_inflight.get(task_id)
        if inflight is not None and not inflight.done():
            self._beat_pending.add(task_id)  # 在途未完成 → 待补发，不打断在途请求
            return
        self._beat_inflight[task_id] = asyncio.create_task(self._run_on_beat(task_id))

    async def _run_on_beat(self, task_id: str) -> None:
        """后台执行一次 on_beat 上传；结束后若有积压（pending）则用最新内容补发一次。

        失败仅告警，绝不影响心跳/取消。被 unregister 取消（CancelledError 属 BaseException，
        不被 except Exception 吞）时直接退出、不再补发。
        """
        cb = self._on_beats.get(task_id)
        if cb is not None:
            try:
                await cb(task_id)
            except Exception as e:
                logger.warning(f"[心跳] on_beat 日志推送失败 task_id={task_id}: {e}")
        # 在途期间又有新节拍 → 合并为单次"尾发"，用最新内容补一次
        if task_id in self._beat_pending and task_id in self._on_beats:
            self._beat_pending.discard(task_id)
            self._beat_inflight[task_id] = asyncio.create_task(self._run_on_beat(task_id))
        else:
            self._beat_pending.discard(task_id)
            self._beat_inflight.pop(task_id, None)

    async def _heartbeat_loop(self, task_id: str):
        """单个任务的心跳循环（后台 Task）。

        节拍：_interval_sequence（5,7,11,…,29 递增后保持 29），非固定 30s。
        404 / cancelled → _mark_non_running（set 取消 Event）+ 跳出循环（停止心跳）。
        running → 从第 _LOG_STREAM_START_BEAT 跳起触发 on_beat（增量日志推送）。
        网络故障 → 继续循环（不阻塞，下次重试），不触发 on_beat（服务端不可达，上传无意义）。
        run_subprocess 并发 await 该任务的取消 Event，被 set 时立即终止子进程
        （事件驱动推送，不依赖子进程是否输出 stdout）。
        """
        intervals = self._interval_sequence()
        beat = 0
        # 是否已经见过 RUNNING。**起跑前的 QUEUED 不是取消信号**：
        # 心跳现在从「下载输入之前」就开始跳（executor 为了让租约在下载期间持续被续），
        # 而正常路径上 RUNNING 由领取分发器在 execute 之前上报（可能失败重试、也可能
        # 整段被跳过——如补偿重投），那一刻服务端仍可能如实答 `queued`，
        # 若照搬「非 running 即取消」，这类任务会在第一跳自杀。
        #
        # 为什么这样放宽是安全的：真被回收/重置时，任务的 worker_id 已被清空或换人，
        # 服务端走的是 fencing 分支、答的是 **revoked** 而不是 queued（协议 §4.2：
        # 撤销回 200 + status=revoked）。也就是说「holder 还是我 + status=queued」只可能是我自己
        # 尚未起跑的那一段。见过 RUNNING 之后，任何非 running 一律照旧当取消处理。
        seen_running = False
        while task_id in self._hb_tasks:
            beat += 1
            try:
                result = await callback_client.update_heartbeat(task_id)
                if result.is_not_found:
                    self._mark_non_running(task_id, "not_found")  # set 取消事件 → 立即终止子进程
                    logger.warning(f"[心跳] {task_id} → 404(not_found)，停止心跳")
                    break  # 任务已不存在，停止心跳循环
                if result.is_ok:
                    server_status = result.heartbeat_status
                    if server_status == "running":
                        seen_running = True
                    elif server_status == "queued" and not seen_running:
                        # 起跑前：续租照做（这正是提前注册心跳的目的），但不推日志
                        # ——此刻子进程还没起来，没有任何输出可推。
                        self._note_renewed(task_id, result.data)
                        if self._check_self_fence(task_id):
                            break
                        if task_id in self._hb_tasks:
                            await asyncio.sleep(next(intervals))
                        continue
                    if server_status and server_status != "running":
                        self._mark_non_running(task_id, server_status)  # set 取消事件
                        logger.warning(f"[心跳] {task_id} → {server_status}，停止心跳")
                        break  # cancelled / queued / failed 等非 running，停止心跳
                    # 续租成功：记下本地基准，供失联时的自我熔断使用（见 _check_self_fence）
                    self._note_renewed(task_id, result.data)
                    # 存活确认：从第 3 跳起增量推送日志（前 2 跳子进程多半尚无有效输出）
                    if beat >= _LOG_STREAM_START_BEAT:
                        await self._fire_on_beat(task_id)
                # result.is_error → 网络故障，继续循环
            except Exception as e:
                logger.error(f"[心跳] 异常 task_id={task_id}: {e}")

            # 自我熔断：即使连不上云端，也要在租约到期前自己停下（见方法 docstring）
            if self._check_self_fence(task_id):
                break

            if task_id in self._hb_tasks:
                await asyncio.sleep(next(intervals))

    # ------------------------------------------------------------------
    # 自我熔断（网络分区下限制算力浪费）
    # ------------------------------------------------------------------

    def _note_renewed(self, task_id: str, data: dict | None) -> None:
        """记录一次成功续租：本地单调时钟基准 + 服务端告知的租约时长。

        **用 monotonic 而不是 wall clock**：本机的 NTP 跳变不该影响熔断判定。

        **租约时长必须来自服务端**，不能本地推算：服务端的 grace 随任务已投入时长
        增长（跑得越久越有耐心），所以租约每跳都在变；本地照自己的想法算，
        熔断时刻会漂到服务端回收之后 —— 那正是「服务端已把任务租给 B、A 仍认为
        自己持有」的窗口，也就是 fencing 要消灭的状态。
        """
        if not isinstance(data, dict):
            return
        lease_until_ms = data.get("lease_until")
        if lease_until_ms is None:
            # 无租约（升级前遗留的在途任务）：不启用自熔
            self._lease_deadline.pop(task_id, None)
            return
        try:
            remaining_ms = data.get("lease_remaining_ms")
            if remaining_ms is not None:
                # 服务端在同一事务里用自己的时钟算好的剩余时长：不含任何跨机时钟偏移（协议 G5）
                remain_sec = int(remaining_ms) / 1000.0
            else:
                # 旧服务端只给绝对时刻：本机墙钟减它，时钟偏移会线性进入判据（局限 K2）
                remain_sec = int(lease_until_ms) / 1000.0 - time.time()
        except (TypeError, ValueError):
            return
        if remain_sec <= 0:
            return
        # 减去安全余量：**必须严格早于服务端回收**，方向搞反等于没做
        self._lease_deadline[task_id] = time.monotonic() + remain_sec - _SELF_FENCE_MARGIN_SEC

    def _check_self_fence(self, task_id: str) -> bool:
        """租约将到期而续租一直失败 → 主动终止子进程。返回是否已熔断。

        ## 为什么需要它

        网络分区时 A 一直在正常计算，只是**通知不到云端**。租约到期后任务被重投给 B，
        而 A 在分区期间会一直烧算力。分区 5 小时就白烧 5 小时。
        等云端来叫停是没用的 —— 分区时云端恰恰是讲不了话的那一方。

        ## 为什么它做得到

        **租约区别于锁的根本性质：过期时刻是双方都知道的，因此不需要通信就能达成一致。**
        A 不需要谁来告诉它「你被吊销了」，它自己算得出来。
        （这也是支持「worker 主动来领」而非「服务端主动推」的最强论据：
        推的一方在分区时正好失声，而拉的一方手里有全部判据。）

        浪费上界因此 = 租约时长，**与分区持续多久无关**。

        ## 它不解决什么

        熔断把 A 的浪费限制在一个租约内，但**不阻止 A 在那段时间里继续往共享存储
        写字节**。服务端的 fencing 保护的是 DB 里的指针，保护不了文件本身。
        """
        deadline = self._lease_deadline.get(task_id)
        if deadline is None:
            return False  # 无租约（遗留在途任务）或尚未拿到过租约信息
        if time.monotonic() < deadline:
            return False

        logger.error(
            f"[心跳] {task_id} 租约已到期且续租持续失败 → 自我熔断，终止子进程"
            f"（服务端多半已把任务重新分配）"
        )
        self._mark_non_running(task_id, "lease_expired")  # 复用既有取消 Event
        self._lease_deadline.pop(task_id, None)
        return True

    async def shutdown_all(self):
        """停止所有心跳（用于 Worker 优雅退出）。"""
        task_ids = list(self._hb_tasks.keys())
        for tid in task_ids:
            await self.unregister(tid)
        logger.info("[心跳] 全部停止")
