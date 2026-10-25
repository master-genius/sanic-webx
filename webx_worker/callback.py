"""
任务服务回调客户端
用于 Worker 向任务服务上报任务状态（路径与方法见 docs/worker-protocol.md §4）

ServerResult 区分三类服务端响应：
  - 200 正常 → is_ok=True, data 有值
  - 404 任务不存在 → is_not_found=True, 需标记 not_found
  - 网络故障/5xx → is_error=True, 需保留目录下次重试

汇报重试策略（按"重试是否可能改善"分档，而非按状态码大小）：
  - 200                → 成功
  - 404 / 400          → 业务拒绝，立即出局，不重试、不入队
  - 401 / 403          → **确定性拒绝，只试 1 次**即入队。
                         同一个 token 重试 5 次必然 5 次都错，前台空转 3s 纯属浪费
                         且占着 Lane；重试交给 PendingReporter（20s 一轮、永不放弃，
                         "运维介入后自愈"），并由 link_health 发出持续 ERROR 告警。
                         （fencing 的终态拒绝 not_lessee / gone / already_final 例外：
                         由 _note_fence_reject 记闩锁，入队时直接丢弃。）
  - 5xx / 网络错       → 前台 5 次 async 重试（覆盖反代闪断，总耗时 ≈ 3s），
                         仍失败则入父进程队列由后台协程异步轮询

链路健康观测：
  所有请求经 `link_health.observed_async_client` 装配的观测咽喉，鉴权（401）与 TLS 故障
  会被聚合成"每故障一条 ERROR"，而非散落成"每任务一行 warning"。见 link_health.py。
"""

import asyncio
import logging
import time
import re
from dataclasses import dataclass, field
from typing import Optional

import httpx

from . import envnames as E
from .config import worker_settings
from .link_health import observed_async_client

logger = logging.getLogger(__name__)

# 前台 async 重试：覆盖反代闪断（总耗时 ≈ 3s）
_CALLBACK_MAX_ATTEMPTS = 5
_CALLBACK_BACKOFF_BASE_SEC = 0.2  # 0.2, 0.4, 0.8, 1.6, ...
_BUSINESS_REJECT_CODES = {400, 404}
# 确定性拒绝：前台只试 1 次（重试不会改善），入队交后台。
# 注意这与 link_health 的「是不是鉴权故障」是**两个不同的问题**：
#   · 这里问"重试有没有意义"—— 401(token 不对) 和 403(fencing/路径拒绝) 都没意义，故都在内。
#   · link_health 问"要不要发全局鉴权告警"—— 只有 401 算，因为内部接口的 403
#     是 fencing / 路径守卫的业务拒绝（见 link_health.classify_status 的说明）。
# 两者取值恰好不同，别把它们合并成一个集合。
_NO_RETRY_CODES = {401, 403}

# 心跳专用超时（秒）——**不能改客户端的全局 30s**，那是上传等慢接口共用的。
# 为什么必须小于心跳节拍：心跳是「续租」的载体，服务端据两次成功心跳的间隔判定
# worker 是否还在。节拍封顶 29s（见 heartbeat._HEARTBEAT_INTERVALS），若超时也取 30s，
# 一次失败就要吃掉 29+30+29=88s，与租约时长贴得过近。
# 取 10s 而非 5s：5s 对偶发慢链路偏紧（一次真实耗时 6s 的正常心跳会被误判失败）。
# 由此两次成功续租的最坏间隔通式为 29 + 39N（N=连续失败次数）。
# ⚠️ 改动此值必须同步核对服务端的租约时长。
_HEARTBEAT_TIMEOUT_SEC = 10.0

# 字段更新（log_path / result_path）的重试次数：**首发 + 重试 1 次**。
#
# 为什么不复用上面那套「5 次前台重试 + 入 PendingReporter 永不放弃」：
# 那套的语义是**终态一致性**（状态机必须推进到位），而本方法写的是可见性字段。
# 且 `PATCH /fields` 是严格 fencing 的 —— 一旦本轮结束（用户推进下一步、
# lease_seq 已 +1），重放必被判 not_lessee 丢弃，入队只会在队列里堆僵尸，
# 把「队列长度即待送达数」这个运维信号搞脏。
#
# 一次重试恰好覆盖占绝大多数的那类故障（反代闪断、单次 5xx）；仍失败就记 error 认了：
# 代价是前端这一步显示「暂无日志」，而 .log 文件其实已经在 storage 上。
_FIELDS_MAX_ATTEMPTS = 2
_FIELDS_RETRY_DELAY_SEC = 1.0

# 任务 ID 在内部端点 URL 里的位置：/internal/tasks/<task_id>/xxx
# （末尾的 `/` 或字符串结尾都算：DELETE /internal/tasks/<id>/lease 之外，
#  GET /internal/tasks/<id> 也要能取到 task_id）
_TASK_ID_IN_URL = re.compile(r"/internal/tasks/([^/?]+)")


async def _inject_fencing_headers(request: "httpx.Request") -> None:
    """按 URL 自动给每个内部请求补上 fencing header。

    做成**请求钩子而非逐调用点传参**的理由：本类有十余个请求出口（且还会新增），
    逐个传参既啰嗦又必然漏掉将来新加的那个；而 task_id 本来就在 URL 里，
    一处提取即可全覆盖。

    未持有租约的任务 `headers_for` 返回空 dict，这条请求不带任何 fencing header
    （按协议，服务端对写操作会明确拒绝，而不是悄悄放行）。
    """
    m = _TASK_ID_IN_URL.search(str(request.url))
    if not m:
        return
    from .fencing import headers_for

    for k, v in headers_for(m.group(1)).items():
        # 不覆盖调用方显式带的头：重放旧条目时令牌来自条目快照（上一轮的 lease_seq），钩子读的却是当前活注册表——
        # 覆盖的话，同一台 worker 重新领到该任务后，旧的 fail 会带着新一轮的令牌通过校验，把健康的新一轮标成失败
        if k not in request.headers:
            request.headers[k] = v


def _enqueue_reason(resp: Optional["httpx.Response"], last_exc: Optional[BaseException]) -> str:
    """入队原因的人话描述 —— 供几个 `_send_with_retry` 调用点共用。

    存在的意义是**不让日志撒谎**：鉴权失败只试了 1 次，若沿用"5 次重试失败"的固定
    文案，排障时会误以为已经充分重试过、从而把注意力引向网络抖动。
    """
    if resp is not None and resp.status_code in _NO_RETRY_CODES:
        return (
            f"确定性拒绝(HTTP {resp.status_code})，前台仅试 1 次——重试不会改善"
            "（401 另有链路告警，见上方）"
        )
    return f"{_CALLBACK_MAX_ATTEMPTS} 次重试失败(last_exc={last_exc})"


@dataclass
class ServerResult:
    """服务端请求结果 — 区分 404 / 网络故障 / 正常。"""

    data: dict | None = field(default=None)
    is_not_found: bool = field(default=False)
    is_error: bool = field(default=False)

    @property
    def is_ok(self) -> bool:
        """200 正常响应。"""
        return not self.is_not_found and not self.is_error

    @property
    def heartbeat_status(self) -> str | None:
        """便捷取值：心跳响应中的 status 字段。"""
        if self.data:
            return self.data.get("status")
        return None

    @property
    def task_status(self) -> str | None:
        """便捷取值：任务状态查询中的 status 字段。"""
        if self.data:
            return self.data.get("status")
        return None


class WebCallbackClient:
    """任务服务回调客户端"""

    def __init__(self):
        self._client: httpx.AsyncClient | None = None
        # (task_id, lease_seq) → fencing 拒绝原因。由 _note_fence_reject 写入，
        # _enqueue_pending 取出并消费（一次性），据此丢弃而非入队。
        #
        # ⚠️ 键**必须带 lease_seq**。闩锁的语义是「**这一轮**的上报被永久拒了」，
        # 不是「这个任务被拒了」。只用 task_id 当键时，一条没人来取的陈旧闩锁
        # （`ack_lease` 那条路会置位却不入队）会在**下一轮**被误消费 ——
        # 同一台 worker 再次领到该任务、跑完后 complete_task 撞上一次网络抖动，
        # 那条本该进重试队列的合法上报会被静默吞掉，任务白跑一轮等租约回收。
        # 带上代次之后，上一轮的闩锁与这一轮的键物理上不可能相等。
        self._fence_rejected: dict[tuple[str, int | None], str] = {}
        # task_id → (ack 响应里的 lease_remaining_ms, 收到时的 monotonic)。供心跳注册时预置自熔基准
        self._ack_info: dict[str, tuple[int, float]] = {}

    # ──────────────────────────────────────────────────────────────
    # 以下几项一律【访问期】现读 worker_settings，不在 __init__ 里拷贝快照。
    #
    # 为什么：本类的模块级单例 callback_client 在 import 期就构造完成，早于
    # main() 的 parse_args()。若构造期拷贝，则 --ext-env / --work-dir 之类
    # 启动参数对配置的覆盖将对本客户端完全失效（值已冻死）。改为 property 后，
    # 「必须在某个 import 之前完成覆盖」这条隐式时序约束直接消失 —— 只要在
    # 首次发请求之前改 worker_settings 即可，而首次请求发生在 asyncio.run()
    # 之后，窗口宽到不可能撞上。
    #
    # 只读不写：全仓无任何处对这些属性赋值，故不提供 setter；若将来有人误写，
    # 会直接抛 AttributeError，比静默偏离配置好。
    # ──────────────────────────────────────────────────────────────

    @property
    def base_url(self) -> str:
        return worker_settings.WEB_BASE_URL

    @property
    def storage_url(self) -> str:
        """存储服务【对外】URL（落库 download_url 展示用，面向浏览器）。

        ⚠️ 与旧实现不同：**不再回退到任务服务地址**——协议规定产物只上传到存储服务，
        任务服务上没有对应端点，回退过去只会 404。未配置即为空串。"""
        return worker_settings.STORAGE_URL

    @property
    def storage_internal_url(self) -> str:
        """存储服务【内网】URL：worker 实际上传/下载走此地址。未配置则回退对外 storage_url。
        仅请求地址不同，鉴权/上传契约一致。"""
        return worker_settings.STORAGE_INTERNAL_URL or self.storage_url

    @property
    def internal_token(self) -> str:
        """⚠️ 边界：本值会在 _get_client() 里被烤进 httpx 客户端的默认 header，
        而该客户端是懒建 + 缓存的。故本 property 保证的是「首次请求之前的覆盖生效」，
        **不是**运行期热更新 —— 客户端一旦建立，改这里不会影响已有连接的 header。
        启动阶段定版的场景（--ext-env 等）完全覆盖，热改 token 请另行 close() 客户端。"""
        return worker_settings.INTERNAL_TOKEN

    @property
    def storage_token(self) -> str:
        """请求存储服务用的 Token：专属优先、回退主 Token
        （STORAGE_INTERNAL_TOKEN → INTERNAL_TOKEN），与子进程 SDK 同一条回退链。"""
        return worker_settings.STORAGE_INTERNAL_TOKEN.strip() or self.internal_token

    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建 httpx 客户端"""
        if self._client is None or self._client.is_closed:
            # 链路健康观测的单一咽喉：本客户端所有请求（含未来新增的方法）
            # 的响应与异常都会被记账，无需逐个调用点加日志。只读不改，
            # 不吞异常、不改状态码，且不影响 httpx 的代理/超时默认行为。
            self._client = observed_async_client(
                headers={
                    E.HEADER_INTERNAL_TOKEN: self.internal_token,
                    "Content-Type": "application/json",
                },
                timeout=30.0,
                event_hooks={"request": [_inject_fencing_headers]},
            )
        return self._client

    async def close(self):
        """关闭客户端"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _send_with_retry(
        self, method: str, url: str, payload: dict | None = None,
    ) -> tuple[Optional[httpx.Response], Optional[BaseException]]:
        """前台 async 重试请求（method 由调用点按协议给出：POST / PUT / PATCH）。

        Returns:
            (resp, None)     成功（200）、业务拒绝（400/404）或鉴权/fencing 拒绝（401/403）
            (None, last_exc) 5xx / 网络错重试耗尽

        鉴权失败为何也**带 resp 返回**而不是当成异常重试：401/403 是确定性的，
        同一个 token 再试 4 次仍是 401，退避那 3 秒纯粹是空转、还占着 Lane 不放。
        带 resp 返回让调用点得以区分"鉴权失败"与"重试耗尽"，日志才不会撒谎地
        写成"5 次重试失败"。入队与否的语义完全不变（仍然入队、仍然永不放弃）。
        """
        client = await self._get_client()
        last_exc: Optional[BaseException] = None
        for attempt in range(_CALLBACK_MAX_ATTEMPTS):
            try:
                resp = await client.request(method, url, json=payload)
                if resp.status_code == 200 or resp.status_code in _BUSINESS_REJECT_CODES:
                    return resp, None
                if resp.status_code in _NO_RETRY_CODES:
                    # 只试 1 次即返回；告警已由 ObservedTransport 统一发出
                    self._note_fence_reject(url, resp)
                    return resp, None
                last_exc = httpx.HTTPError(f"HTTP {resp.status_code}")
            # TransportError 是传输层异常的基类：ConnectTimeout / WriteTimeout / PoolTimeout 都不是 OSError 的子类，
            # 列举具体类会漏掉它们（防火墙丢 SYN 时已跑完的任务因此被判失败重跑）
            except (httpx.TransportError, OSError) as e:
                last_exc = e
            if attempt < _CALLBACK_MAX_ATTEMPTS - 1:
                await asyncio.sleep(_CALLBACK_BACKOFF_BASE_SEC * (2 ** attempt))
        return None, last_exc

    async def update_status(self, task_id: str, status: str) -> bool:
        """更新任务状态 → PUT /internal/tasks/{id}/status。

        失败处理：
          - 200 → True
          - 400 / 404 → False（业务拒绝，不入队）
          - 5xx / 网络错 → 5 次重试后入父进程队列，返回 False
        """
        url = f"{self.base_url}/internal/tasks/{task_id}/status"
        payload = {"status": status}

        resp, last_exc = await self._send_with_retry("PUT", url, payload)
        if resp is not None and resp.status_code == 200:
            return True
        if resp is not None and resp.status_code in _BUSINESS_REJECT_CODES:
            logger.error(
                f"更新状态被业务拒绝（不入队）: {task_id} -> {status}, "
                f"status={resp.status_code}, body={resp.text[:200]}"
            )
            return False

        # 入队让后台 consumer 重试
        logger.warning(
            f"更新状态未成功，入父进程队列: {task_id} -> {status}, "
            f"原因={_enqueue_reason(resp, last_exc)}"
        )
        await self._enqueue_pending("update_status", task_id, payload)
        return False

    def _note_fence_reject(self, url: str, resp: "httpx.Response") -> None:
        """记下一次 fencing 终态拒绝，供 `_enqueue_pending` 据此丢弃而非入队。

        为什么必须丢弃：`PendingReporter` 的设计语义是「**永不退出，永不放弃**」，
        且退出时 snapshot 跨重启持久化。它假设失败只有一种 —— **暂时性故障**
        （网断了、云端在重启），对那种失败无限重试完全正确。
        而 fencing 造出了一种全新的失败类型：**永久性拒绝**。把它当成暂时故障，
        那条记录会永远重试下去；更糟的是，该队列的定位是「队列长度即系统状态」，
        混进永远送不出去的僵尸后**这个运维指标就废了**，真正的积压看不出来。

        不是 fencing 错了，也不是 PendingReporter 错了 —— 是两者的接口没对齐。
        """
        from .fencing import is_terminal_reject

        try:
            reason = is_terminal_reject(resp.json())
        except Exception:
            reason = None
        if not reason:
            return

        m = _TASK_ID_IN_URL.search(url)
        if not m:
            return
        task_id = m.group(1)
        from .fencing import lease_seq_of

        self._fence_rejected[(task_id, lease_seq_of(task_id))] = reason

        if reason == "already_final":
            # ⚠️ 这**不是**错误：同一租户的重复上报（回调重投、超时后其实成功了）。
            # 当成「你丢了租约」记 error 会污染日志、淹没真正的异常。
            logger.info(f"[fence] {task_id} 已达终态（幂等成功），丢弃本次上报")
        else:
            logger.warning(f"[fence] {task_id} 上报被拒（{reason}），永久丢弃，不入队")

    async def _enqueue_pending(self, action: str, task_id: str, payload: dict) -> None:
        """父进程内部入队：lazy import 避免循环依赖。

        若全局 PendingReporter 未启动（如启动早期阶段失败），事件被记录但丢失。

        ⚠️ fencing 终态拒绝**直接丢弃**（见 `_note_fence_reject`）：入队意味着
        「等会儿再试」，而永久性拒绝再试一万次也是同一个答案。
        """
        from .fencing import lease_seq_of

        if self._fence_rejected.pop((task_id, lease_seq_of(task_id)), None) is not None:
            return

        import time as _time
        try:
            from .pending_reporter import enqueue_global

            # fencing 令牌**快照进条目**，不能等重放时再去查注册表。
            #
            # 那个自动注入的请求钩子读的是父进程内存里的 `_leases`，而它在
            # `lease._run_one` 的 finally 就被 unregister 了 —— 重放恒发生在**那之后**
            # （跨重启从 snapshot 恢复的更是必然）。查表只会查到空，于是这条上报
            # 不带令牌，被服务端判为「不是租户」403，落进 _AUTH_RETAIN_CODES 永久重试。
            #
            # 令牌的正确语义是「**这条上报是哪一次运行产生的**」——随数据走的值，
            # 不是随进程走的状态。服务端 header / body 两条通道都认（协议 §2），
            # 这里走 body。
            entry: dict = {
                "task_id": task_id,
                "action": action,
                "payload": payload,
                "context": {},
                "ts": _time.time(),
            }
            seq = lease_seq_of(task_id)
            if seq is not None:
                entry["fencing"] = {
                    # 访问期现读，与本类那几个 property 同一理由（配置覆盖发生在 import 之后）
                    "worker_id": worker_settings.WORKER_ID, "lease_seq": seq,
                }
            await enqueue_global(entry)
        except Exception as e:
            logger.error(f"[Callback] 入父进程队列失败 task_id={task_id} action={action}: {e}")

    async def update_heartbeat(self, task_id: str) -> ServerResult:
        """上报心跳 → POST /internal/tasks/{id}/heartbeats，返回 ServerResult。

        撤销（租约已被收走）时服务端回 `200 {"ok": false, "status": "revoked"}`（协议 §4.2），
        调用方据 `status != "running"` 取消子进程，无需额外分支。
        """
        client = await self._get_client()
        url = f"{self.base_url}/internal/tasks/{task_id}/heartbeats"

        try:
            resp = await client.post(url, json={}, timeout=_HEARTBEAT_TIMEOUT_SEC)
            if resp.status_code == 200:
                data = resp.json()
                return ServerResult(data=data)
            elif resp.status_code == 404:
                logger.warning(f"心跳 404：任务不存在 task_id={task_id}")
                return ServerResult(is_not_found=True)
            else:
                logger.error(f"心跳上报失败：{task_id}, 状态码：{resp.status_code}")
                return ServerResult(is_error=True)
        except Exception as e:
            logger.error(f"心跳上报异常：{task_id}, 错误：{e}")
            return ServerResult(is_error=True)

    async def ack_lease(self, task_id: str) -> bool:
        """领取后**立刻**发出的确认，把短租约续到常规时长。

        为什么必须有它、而不是把认领短租约调大：短租约要覆盖的是
        「认领 → 首次回调」，而这段里塞着环境激活、子进程拉起、输入文件下载。
        大输入任务若超过短租约才发出第一次回调，**租约会在一台健康 worker
        正在启动时到期**，任务被回收、两份并发跑 —— 正是「设错了会静默抢任务」
        的那类故障，只是发生在领取阶段。

        有了即时 ack，短租约只需覆盖一个网络往返，启动多慢都不再有关系。

        它同时确认了 worker 真的接住了这个任务（`POST /internal/tasks/<id>/ack`）。
        """
        url = f"{self.base_url}/internal/tasks/{task_id}/ack"
        resp, last_exc = await self._send_with_retry("POST", url, {})
        if resp is not None and resp.status_code == 200:
            try:
                remaining = resp.json().get("lease_remaining_ms")
                if isinstance(remaining, (int, float)):
                    self._ack_info[task_id] = (int(remaining), time.monotonic())
            except ValueError:
                pass
            return True
        # ⚠️ 本方法是**唯一**「会置 fencing 闩锁却不入队」的出口：403 时
        # `_send_with_retry` 已经把闩锁记下了，而下面直接 return，没有 `_enqueue_pending`
        # 来消费它。键带代次已经堵掉了误伤（见 __init__ 的说明），这里再清一次
        # 是为了不留慢性泄漏 —— 长驻进程里每次 ack 被拒都留一条，只增不减。
        from .fencing import lease_seq_of

        self._fence_rejected.pop((task_id, lease_seq_of(task_id)), None)
        # ack 失败不入队、不重试到底：任务会在短租约到期后自动回池，
        # 由别人（或自己）重新领取，比抱着一个可能已被回收的任务硬跑安全。
        logger.warning(
            f"[lease] ack 未成功 task_id={task_id}，"
            f"原因={_enqueue_reason(resp, last_exc)}（任务将在短租约到期后回池）"
        )
        return False

    def pop_ack_lease_remaining_ms(self, task_id: str) -> int | None:
        """取走 ack 时服务端告知的剩余租约（已扣除自收到起流逝的时间）；没有则 None。

        用来给心跳预置自熔基准：否则“ack 成功之后一次心跳也没成功过”（服务端立刻不可达）时根本没有 deadline，
        自熔永远不启动，worker 无限期继续算、服务端早已把任务租给了别人。
        """
        info = self._ack_info.pop(task_id, None)
        if info is None:
            return None
        remaining_ms, at = info
        return int(remaining_ms - (time.monotonic() - at) * 1000)

    async def release_lease(self, task_id: str, extra_headers: dict | None = None) -> bool:
        """主动归还租约 → DELETE /internal/tasks/{id}/lease，任务立刻回池，不必等租约自然到期。

        两个调用场景：
          · **优雅退出 / drain**：把「等租约到期」压到 0
          · **重启恢复**：`recovery` 扫到「目录里没有 ok.json」即知上一轮没跑完，
            此时归还比等到期快一个数量级（重启间隔几秒 vs 一个完整租约周期）

        被拒（403）是**正常结果**，不是错误：说明租约早已到期、任务被别人接管了，
        此时什么都不做才是对的。故不重试、不入队。

        `extra_headers`：显式传入 fencing 令牌，**供停机路径用**。
        平时令牌由 `_inject_fencing_headers` 钩子在**发请求那一刻**去注册表现取，
        而停机时我们要先同步取消在跑任务（见 lease.cancel_running_tasks），
        它们的 finally 会把注册表清空 —— 等到请求真正构造出来时钩子已经取不到令牌，
        归还会被服务端判 `missing_fencing_token` 直接拒掉。
        故停机路径先同步捕获令牌、再显式传进来。钩子只做 `headers[k]=v`，
        取不到时不会清掉这里传的值，两者可以共存。

        DELETE **不带请求体**（api-conventions：网关会丢 DELETE 的请求体），
        fencing 令牌只走请求头。
        """
        url = f"{self.base_url}/internal/tasks/{task_id}/lease"
        try:
            resp = await (await self._get_client()).delete(
                url, timeout=10.0, headers=extra_headers or None
            )
            if resp.status_code == 200:
                logger.info(f"[lease] 已归还租约 task_id={task_id}")
                return True
            logger.info(f"[lease] 归还未生效 task_id={task_id} status={resp.status_code}"
                        f"（多半已被接管，属正常）")
        except Exception as e:
            # 归还失败只是慢一点：任务会在租约到期后自动回池。不值得重试。
            logger.warning(f"[lease] 归还请求失败 task_id={task_id}: {e}（将由租约到期兜底）")
        return False

    async def complete_task(self, task_id: str) -> bool:
        """完成任务（仅触发状态迁移）→ POST /internal/tasks/{id}/complete。

        5 次重试失败 → 入父进程队列由后台 consumer 异步重试
        （吸收了原 Recovery 启动时才扫 ok.json 的兜底，运行中也能恢复）
        """
        url = f"{self.base_url}/internal/tasks/{task_id}/complete"
        payload: dict = {}

        resp, last_exc = await self._send_with_retry("POST", url, payload)
        if resp is not None and resp.status_code == 200:
            return True
        if resp is not None and resp.status_code in _BUSINESS_REJECT_CODES:
            logger.error(
                f"完成任务被业务拒绝（不入队）: {task_id}, "
                f"status={resp.status_code}, body={resp.text[:200]}"
            )
            return False

        logger.warning(
            f"完成任务未成功，入父进程队列: {task_id}, "
            f"原因={_enqueue_reason(resp, last_exc)}"
        )
        await self._enqueue_pending("complete_task", task_id, payload)
        return False

    async def fail_task(
        self, task_id: str, error_message: str
    ) -> bool:
        """失败任务（仅上报错误信息，不传 log_path）→ POST /internal/tasks/{id}/fail

        进程级日志摘要已合入 error_message；
        结构化 log_path 由 executor / 任务脚本通过 PATCH /fields 管理。

        5 次重试失败 → 入父进程队列。PendingReporter 重试 fail_task 前会做
        server 状态前置校验，避免覆盖补偿器重投后的工作；snapshot 时
        也不持久化 fail_task entry。
        """
        url = f"{self.base_url}/internal/tasks/{task_id}/fail"
        payload = {
            "error_message": error_message if error_message is not None else "",
        }

        resp, last_exc = await self._send_with_retry("POST", url, payload)
        if resp is not None and resp.status_code == 200:
            return True
        if resp is not None and resp.status_code in _BUSINESS_REJECT_CODES:
            logger.error(
                f"失败任务被业务拒绝（不入队）: {task_id}, "
                f"status={resp.status_code}, body={resp.text[:200]}"
            )
            return False

        logger.warning(
            f"失败任务未成功，入父进程队列（带前置校验）: {task_id}, "
            f"原因={_enqueue_reason(resp, last_exc)}"
        )
        await self._enqueue_pending("fail_task", task_id, payload)
        return False

    async def get_task_status(self, task_id: str) -> ServerResult:
        """查询任务状态信息 → GET /internal/tasks/{id}（协议 §4.10）。"""
        client = await self._get_client()
        url = f"{self.base_url}/internal/tasks/{task_id}"

        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                body = resp.json()
                # 协议 §4.10 没写清响应是否包在 data 里（`{id, status, ...}` 还是
                # `{"data": {...}}`）—— 两种都兼容，见 docs/worker.md 的协议缺口。
                if isinstance(body, dict) and isinstance(body.get("data"), dict):
                    body = body["data"]
                return ServerResult(data=body if isinstance(body, dict) else None)
            elif resp.status_code == 404:
                logger.warning(f"查询任务状态 404：任务不存在 task_id={task_id}")
                return ServerResult(is_not_found=True)
            else:
                logger.warning(f"查询任务状态失败：{task_id}, 状态码：{resp.status_code}")
                return ServerResult(is_error=True)
        except Exception as e:
            logger.error(f"查询任务状态异常：{task_id}, 错误：{e}")
            return ServerResult(is_error=True)

    async def update_step_status(
        self,
        task_id: str,
        steps: list[str] | None = None,
        sub_step: str | None = None,
        sub_status: str | None = None,
        step_message: str | None = None,
    ) -> bool:
        """更新步骤进度 → PATCH /internal/tasks/{task_id}/steps"""
        client = await self._get_client()
        url = f"{self.base_url}/internal/tasks/{task_id}/steps"

        payload = {}
        if steps is not None:
            payload["steps"] = steps
        if sub_step is not None:
            payload["sub_step"] = sub_step
        if sub_status is not None:
            payload["sub_status"] = sub_status
        if step_message is not None:
            payload["step_message"] = step_message

        try:
            resp = await client.patch(url, json=payload)
            if resp.status_code == 200:
                return True
            else:
                logger.error(f"步骤进度更新失败：{task_id}, 状态码：{resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"步骤进度更新异常：{task_id}, 错误：{e}")
            return False

    async def upload_result_file(
        self, task_id: str, filename: str, content: bytes, timeout: float = 60.0
    ) -> str | None:
        """上传结果文件到存储服务，返回下载 URL。

        `POST {存储服务}/internal/tasks/<task_id>/results`（multipart，创建成功 201），
        见 docs/storage-service.md。产物**不经过任务服务**（协议 §5）。
        使用独立 httpx 客户端（multipart 上传与共享客户端的 JSON Content-Type 冲突）。

        Args:
            timeout: 请求超时（秒）。终态产物上传用默认 60s；心跳增量日志流式上传传更短的
                     35s，避免单次上传长时间占住"在途"槽、拖延后续合并补发。
        """
        base = self.storage_internal_url.strip()
        if not base:
            logger.error(
                f"上传文件失败：未配置存储服务地址（{E.ENV_STORAGE_INTERNAL_URL} / "
                f"{E.ENV_STORAGE_URL}）：{task_id}/{filename}"
            )
            return None
        url = f"{base.rstrip('/')}/internal/tasks/{task_id}/results"
        try:
            # 独立客户端也要观测：它与共享客户端走同一类链路、同一种故障，
            # 漏掉就会在"上传能不能成"这条路径上重演「坏了但不吭声」。
            # 存储服务只认令牌、不做 fencing，故这里不挂 fencing 钩子。
            async with observed_async_client(timeout=timeout) as client:
                resp = await client.post(
                    url,
                    headers={E.HEADER_INTERNAL_TOKEN: self.storage_token},
                    files={"file": (filename, content, "text/plain")},
                    data={"filename": filename},
                )
            # 存储服务创建成功回 201；兼容 200（兼容别名 / 网关改写）
            if resp.status_code in (200, 201):
                return self._public_download_url(resp.json().get("download_url"))
            logger.error(f"上传文件失败：{task_id}/{filename}, 状态码：{resp.status_code}")
            return None
        except Exception as e:
            logger.error(f"上传文件异常：{task_id}/{filename}, 错误：{e}")
            return None

    @staticmethod
    def _public_download_url(rel: str | None) -> str | None:
        """存储端回传相对路径 → 落库用的【对外】URL。

        配置了对外地址才拼接；未配置则保持相对路径（由前端按页面 origin 解析）。
        与 uploader / sdk.result_client 同口径。"""
        if not rel or rel.startswith(("http://", "https://")):
            return rel
        pub = worker_settings.STORAGE_URL.strip().rstrip("/")
        return f"{pub}/{rel.lstrip('/')}" if pub else rel

    async def update_task_fields(
        self,
        task_id: str,
        log_path: str | None = None,
        result_path: str | None = None,
        mode: str = "merge",
    ) -> bool:
        """更新任务字段（log_path / result_path），支持 merge 模式。
        → PATCH /internal/tasks/{id}/fields[?mode=merge]

        失败处理：**只重试 1 次**，不入 PendingReporter（理由见 `_FIELDS_MAX_ATTEMPTS`）。
        200 / 业务拒绝(400,404) / 确定性拒绝(401,403) 一律立即返回，只有
        5xx 与网络错才值得再试一次。
        """
        client = await self._get_client()
        url = f"{self.base_url}/internal/tasks/{task_id}/fields"
        if mode == "merge":
            url += "?mode=merge"

        payload = {}
        if log_path is not None:
            payload["log_path"] = log_path
        if result_path is not None:
            payload["result_path"] = result_path
        if not payload:
            return True

        last = ""
        for attempt in range(_FIELDS_MAX_ATTEMPTS):
            try:
                resp = await client.patch(url, json=payload)
                if resp.status_code == 200:
                    return True
                if resp.status_code in _BUSINESS_REJECT_CODES | _NO_RETRY_CODES:
                    # 业务拒绝 / 确定性拒绝：再试一次也是同一个答案
                    logger.error(
                        f"更新字段被拒（不重试）：{task_id}, 状态码：{resp.status_code}"
                    )
                    return False
                last = f"HTTP {resp.status_code}"
            except Exception as e:  # noqa: BLE001 —— 网络错，值得再试一次
                last = repr(e)
            if attempt < _FIELDS_MAX_ATTEMPTS - 1:
                await asyncio.sleep(_FIELDS_RETRY_DELAY_SEC)

        # 说清「已经重试过了」，否则排障时会误以为只发了一次、把注意力引向偶发抖动
        logger.error(
            f"更新字段失败（已重试 {_FIELDS_MAX_ATTEMPTS - 1} 次，不入队）："
            f"{task_id}, last={last}"
        )
        return False


# 全局单例
callback_client = WebCallbackClient()
