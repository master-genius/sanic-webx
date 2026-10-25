"""内部回调链路的健康记账 —— 把「每任务一行 warning」聚合成「每故障一条 ERROR」。

## 为什么需要这个模块

worker 有**两条彼此独立的通道**，故障时的表现极不对称：

    ① 领取 → 领取服务（HTTP + token）  主动领任务
    ② HTTP → 任务服务（httpx over TLS） 报状态 / 取状态

证书、内部令牌、反向代理任意一环坏掉，**只断 ②**，而 ① 照常打出
「领取正常」。于是故障的外在表现是：**任务收得到，却全部无声地跑不起来**
——服务端看到的是一批永远停在 `queued` 的任务，worker 日志里看着一切正常。

历史事故的教训不是"系统没发现"，而是**"系统发现了却只肯小声说"**：失败确实被记录了，
但记录形式是每个任务一行 warning，散落在成千上万行日志里，没有任何**一处**说出
「链路坏了」这个全局事实。`pending_reporter` 的设计注释写着 401 属「运维介入后自愈」，
可如果没有任何机制去通知运维该介入，这句话就是空的。

## 本模块做什么

只做一件事：给**确定性故障**（鉴权 / TLS）记账并对外发声。

  - 确定性故障 = 重试一万次也不会好的故障（token 不匹配、证书链不完整、反代未透传
    鉴权头）。与之相对的是**瞬时故障**（5xx、超时、连接被重置），那类重试有意义，
    不归本模块管。
  - 首次发生立刻 ERROR；持续期间按 `ALARM_INTERVAL_SEC` 限流继续 ERROR（带累计次数
    与持续时长，避免刷屏又不会沉默）；恢复时打一条 INFO 收尾。
  - 每条告警都**直接列出可能的原因**，让人不必回来读代码。

## 不做什么

不改变任何重试 / 放弃语义。谁该重试、谁该入队、谁该 fail-open，仍由调用点决定；
本模块纯粹是旁路观测，摘掉它不影响任何功能路径。
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import time

import httpx

logger = logging.getLogger(__name__)

# 确定性故障：重试不会改善，必须人工介入
KIND_AUTH = "auth"   # 401 / 403
KIND_TLS = "tls"     # 证书链 / 主机名 / CA 校验失败

# 启动自检专用：连不上 / 应答不对。运行期不用它——连接被拒是瞬时故障，
# 重试有意义，不该触发"需人工介入"的持续告警；但**开机时**探不通就是配置问题，
# 必须当场说清楚，且提示方向与鉴权完全不同。
KIND_UNREACHABLE = "unreachable"

# 首次之后的持续告警间隔（秒）：既不刷屏，也不会安静超过一分钟
ALARM_INTERVAL_SEC = 60.0

# 各类故障的排查提示 —— 按「最常见 → 最少见」排列，照着查一遍即可定位
_HINTS: dict[str, tuple[str, ...]] = {
    KIND_AUTH: (
        "WEBX_WORKER_INTERNAL_TOKEN 与服务端配置的内部令牌是否逐字一致"
        "（注意：同名项写两次时取【最后一行】；真实环境变量会【覆盖】.env 与覆盖层）",
        "反向代理是否透传了 X-Internal-Token 请求头",
        "WEBX_WORKER_WEB_BASE_URL 是否指向了别的服务（证书/SNI 不匹配时 nginx 会落到 default server）",
    ),
    KIND_TLS: (
        "服务端是否用了 fullchain 证书（只配叶子证书会缺中间 CA，客户端无法构建信任链）",
        "证书是否过期，或域名与 WEBX_WORKER_WEB_BASE_URL 不匹配",
        "worker 所在机器的 CA 根证书是否过旧（ca-certificates / certifi）",
    ),
    KIND_UNREACHABLE: (
        "任务服务是否已启动（启动顺序：服务端 → worker；worker 先起会探不通）",
        "WEBX_WORKER_WEB_BASE_URL 是否正确（协议/域名/端口）",
        "网络与防火墙是否放行，反向代理的 upstream 是否健康（502/504 多为此因）",
    ),
}


class LinkHealth:
    """内部回调链路的健康状态记账。

    线程/协程安全性：worker 内所有回调都在同一 event loop 里跑，方法体内无 await，
    不存在交错，故无需加锁。
    """

    def __init__(self, alarm_interval_sec: float = ALARM_INTERVAL_SEC) -> None:
        self.alarm_interval_sec = alarm_interval_sec
        self._kind: str | None = None        # 当前故障类别，None = 健康
        self._count: int = 0                 # 本轮故障累计次数
        self._first_mono: float = 0.0        # 本轮首次失败的 monotonic 时刻
        self._first_wall: float = 0.0        # 本轮首次失败的墙钟（仅用于展示）
        self._last_alarm_mono: float = 0.0   # 上次打 ERROR 的 monotonic 时刻

    @property
    def failing(self) -> bool:
        """当前是否处于确定性故障中。"""
        return self._kind is not None

    @property
    def failure_count(self) -> int:
        """本轮故障累计次数（恢复后归零）。"""
        return self._count

    def note_failure(self, kind: str, detail: str, *, task_id: str = "") -> None:
        """记录一次确定性故障；首次立刻 ERROR，之后按间隔限流继续 ERROR。

        Args:
            kind:    KIND_AUTH / KIND_TLS
            detail:  具体现象（如 "HTTP 401 POST /internal/tasks/x/ack"）
            task_id: 触发本次失败的任务（仅用于在告警里给一个可复现的样本）
        """
        now = time.monotonic()

        # 换了一类故障 → 当作新一轮重新计数（避免 auth 的计数被 tls 继承）
        if self._kind != kind:
            self._kind = kind
            self._count = 0
            self._first_mono = now
            self._first_wall = time.time()
            self._last_alarm_mono = 0.0

        self._count += 1

        is_first = self._count == 1
        due = (now - self._last_alarm_mono) >= self.alarm_interval_sec
        if not (is_first or due):
            return

        self._last_alarm_mono = now
        since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._first_wall))
        elapsed = int(now - self._first_mono)
        hints = "".join(f"\n         {i}) {h}" for i, h in enumerate(_HINTS.get(kind, ()), 1))
        sample = f" 样本 task={task_id}" if task_id else ""

        logger.error(
            "⛔ 内部回调链路持续失败（%s）：累计 %d 次，自 %s 起已 %ds%s\n"
            "         现象: %s\n"
            "         注意: 领取通道与本通道相互独立——能领到任务不代表回调可用。\n"
            "         请依次检查:%s",
            kind, self._count, since, elapsed, sample, detail, hints,
        )

    def note_success(self) -> None:
        """记录一次成功往返；若此前处于故障中，打一条恢复日志并归零。

        「成功」的判据是**服务端给出了业务语义的应答**（200/400/404 都算），
        而不只是"没抛异常"——鉴权失败同样不抛异常，却绝不该被算作链路健康。
        """
        if self._kind is None:
            return
        elapsed = int(time.monotonic() - self._first_mono)
        logger.info(
            "✅ 内部回调链路已恢复（此前 %s 故障，累计失败 %d 次，持续约 %ds）"
            "；积压的状态上报将由 PendingReporter 自动回放，无需重启 worker。",
            self._kind, self._count, elapsed,
        )
        self._kind = None
        self._count = 0


# 全局单例：worker 单进程单 event loop，无需注入
link_health = LinkHealth()


def classify_exception(exc: BaseException) -> str | None:
    """把异常归类为确定性故障；返回 None 表示"瞬时故障，重试有意义"。

    TLS 判定不 import ssl 之外的东西、也不依赖 httpx 的内部异常层级——
    httpx 把 `ssl.SSLError` 包进 `httpx.ConnectError` 后原异常挂在 `__cause__`/
    `__context__` 上，各版本包法不同。沿异常链找 `ssl.SSLError` 是最稳的判据；
    兜底再看文本，覆盖被字符串化掉的情况。
    """
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, ssl.SSLError):
            return KIND_TLS
        cur = cur.__cause__ or cur.__context__

    text = str(exc).lower()
    if "certificate" in text or "ssl" in text or "tlsv1" in text:
        return KIND_TLS
    return None


def classify_status(status_code: int) -> str | None:
    """把 HTTP 状态码归类为确定性故障；返回 None 表示不属于本模块管辖。

    ⚠️ **只认 401，不认 403** —— 这是依据 docs/worker-protocol.md，不是通用 HTTP 语义：

      · 内部鉴权失败恒为 **401**：令牌缺失或不匹配一律 401（协议 §2）。
      · 内部接口的 **403 是业务/安全拒绝**：fencing 拒绝（`not_lessee` / `gone` /
        `already_final` / `missing_fencing_token`）与路径穿越守卫都回 403，
        客户端不应重试，也**不应触发全局鉴权告警**（协议 §2 原话）。

    若把 403 也算成鉴权故障，一次正常的租约丢失就会触发全局告警、让运维去核对令牌和
    反向代理配置——"大声地指错方向"，与本模块的目的正好相反。

    403 因此归入"链路健康"：服务端能给出业务语义应答，说明 TLS + 路由 + token 全通。

    若将来协议把鉴权失败改为 403，**必须同步修改此处**。
    """
    return KIND_AUTH if status_code == 401 else None


# 启动自检用的哨兵任务 ID：协议规定 `GET /internal/tasks/<id>` 对不存在的任务回 404
# （docs/worker-protocol.md §4.10），因此用它探测是**纯只读**的：既不会改任何真实任务，
# 也不依赖任何写端点的 fencing 行为。
#
# 探测语义：
#   · 404 → 可达且鉴权通过（服务端对哨兵给出了业务语义的应答）—— 链路通
#   · 401 → 令牌不对
#   · 其它 → 可达但应答不对（反代路径没转发、网关改写……），按「不可达」类提示
#
# 哨兵取「字母数字、长度 15」：旧服务端对 task_id 有形态守卫（长度/字符集）会回 400，
# 新协议没有写明这类约束，故保守沿用一个合规形状（见 docs/worker.md 已知限制）。
PROBE_TASK_ID = "probe0000000000"

# 探测被接受的状态码：200（极端情况下该 ID 真存在）与 404
_PROBE_OK_CODES = (200, 404)


async def check_web_link(
    callback, *, attempts: int = 3, delay_sec: float = 3.0, timeout: float = 5.0,
) -> tuple[bool, str | None, str]:
    """启动自检：把内部回调链路**在收到第一个任务之前**探通一次。

    为什么必须有这一步：
      worker 启动时只验证了领取通道（能领到任务即视为正常），HTTP 回调通道
      要等**第一个任务到达**才第一次被使用。于是证书/token/反代这类开机就存在的故障，
      发现时机被推迟到"任务已经投递之后"，表现为一批任务莫名其妙卡在 queued。
      本自检把发现时机提前到启动瞬间。

    探测路径刻意选用 `/internal/tasks/{id}` 而非 `/health`：它与 worker 真正
    要走的路径同一个 base_url、同一 TLS、同一 token、同一鉴权关卡，
    因此"探测通过"能真正代表"回调可用"。`/health` 不带鉴权，探不出 token 问题。

    不阻止启动：探测失败只打横幅告警后继续。理由是链路恢复后 worker 本可自愈
    （PendingReporter 回放积压、新任务正常执行），一次误判不该让 worker 长期躺平。
    可见性由横幅 + link_health 的 60s 持续 ERROR 共同保证。

    ⏱ **耗时必须有界，且远小于 callback 客户端的 30s 业务超时。**
      · `timeout` 默认 **5s** 且**逐请求覆盖**共享客户端的 30s。
      · **确定性故障立即返回，不重试**：401 / TLS 再试两次仍是同一个答案，纯浪费。
        只有"不可达"才重试——那对应"服务端还没起来"这一个真实场景（启动顺序竞态）。
    最坏耗时 `3×5 + 2×3 = 21s`，且仅出现在服务端完全不可达时。
    调用方应把它放到后台执行（见 `selfcheck_and_report`），使启动路径零延迟。

    Returns:
        (ok, kind, detail)
          ok     —— True 表示链路通
          kind   —— 失败类别（KIND_AUTH / KIND_TLS / KIND_UNREACHABLE），成功时 None。
                    **由分类函数判定后带出**，不让展示层再去解析 detail 文本猜类别：
                    猜错的代价就是"大声地指错方向"。
          detail —— 失败现象原文（成功时为空串）
    """
    url = f"{callback.base_url}/internal/tasks/{PROBE_TASK_ID}"
    kind: str | None = KIND_UNREACHABLE
    detail = ""
    for attempt in range(1, attempts + 1):
        try:
            client = await callback._get_client()
            # 逐请求覆盖超时：共享客户端是 30s 业务超时，探活用不着等那么久
            resp = await client.get(url, timeout=timeout)
            if resp.status_code in _PROBE_OK_CODES:
                return True, None, ""
            detail = f"HTTP {resp.status_code}（期望 404 或 200）"
            # 401 → 鉴权；403/5xx/意外 2xx-3xx → 服务端可达但应答不对（见 classify_status）
            kind = classify_status(resp.status_code) or KIND_UNREACHABLE
        except Exception as exc:  # 连接/TLS/超时
            detail = f"{type(exc).__name__}: {exc}"
            # TLS 有确定性判据；连不上/超时归入"不可达"，提示查服务与地址而非 token
            kind = classify_exception(exc) or KIND_UNREACHABLE
        # 确定性故障（token 不对 / 证书不对）重试没有意义，立刻返回，不占启动时间
        if kind in (KIND_AUTH, KIND_TLS):
            return False, kind, detail
        if attempt < attempts:
            await asyncio.sleep(delay_sec)
    return False, kind, detail


async def selfcheck_and_report(callback) -> bool:
    """跑一次启动自检并打横幅。供调用方**放到后台任务里**执行。

    独立成函数是为了让 `app.py` 能一行 `asyncio.create_task(...)`，
    从而让自检完全不占用启动路径——worker 立刻去领任务，自检在旁边跑。
    """
    ok, kind, detail = await check_web_link(callback)
    log_link_check_banner(ok, kind, detail, callback.base_url)
    return ok


def log_link_check_banner(ok: bool, kind: str | None, detail: str, base_url: str) -> None:
    """把自检结果打成一眼可见的横幅。失败时按**类别**列出排查清单。

    类别由 `check_web_link` 判定后传入，本函数不再从 detail 文本反推——反推必然把
    「连接被拒」误判成鉴权问题，从而给出一份完全无关的排查清单。
    """
    if ok:
        logger.info("[自检] 内部回调链路正常 → %s", base_url)
        return

    hints = "".join(f"\n         {i}) {h}" for i, h in enumerate(_HINTS.get(kind or "", ()), 1))
    logger.error(
        "\n%s\n"
        "  ⛔ 内部回调链路自检失败 —— worker 仍会启动，但**领到的任务多半跑不起来**\n"
        "%s\n"
        "  目标: %s\n"
        "  现象: %s\n"
        "  说明: 这条通道与领取通道相互独立；能领到任务也不代表回调可用。\n"
        "  请检查:%s\n"
        "%s",
        "=" * 72, "=" * 72, base_url, detail, hints, "=" * 72,
    )


class ObservedTransport(httpx.AsyncBaseTransport):
    """观测用 transport **包装器**：把一个已有 transport 包起来，只记账、不改行为。

    为什么做在 transport 层而不是逐个调用点加日志：
      `WebCallbackClient` 有十余个方法各自发请求、各自 `except`。逐个加观测既啰嗦，
      更要命的是**将来新增一个方法必然会忘**——而"忘了一处"在这里的代价就是本次事故
      （链路坏了但那条路径不吭声）。transport 是 httpx 的单一出口。

    ⚠️ 为什么是「包装器」而不是 `AsyncHTTPTransport` 的子类：
      httpx 的装配规则是 `allow_env_proxies = trust_env and transport is None`
      （见 `httpx.AsyncClient.__init__`）。也就是说，**只要显式传 `transport=`，
      环境变量代理 HTTP_PROXY/HTTPS_PROXY 就会被整体禁用**。在设了全局代理的机器上，
      这是一次静默的出网路径变更——与本模块"消灭静默故障"的目的正好相反。
      故改为：先让 httpx 按自己的默认规则把 `_transport` 与 `_mounts`（含代理）装配好，
      再逐个包一层观测。见 `observed_async_client`。

    只读不改：绝不吞异常、绝不改状态码，原样透传。摘掉本类不影响任何功能路径。
    """

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner

    @staticmethod
    def _observe(fn, *args) -> None:
        """执行一次观测动作，**吞掉其自身的任何异常**。

        观测是旁路，绝不允许反过来搞坏功能路径：万一日志 handler 出错、格式化抛异常，
        代价必须是"少一条告警"，而不是"这个请求失败了"。这是包装器的安全底线——
        没有它，一个观测层的小 bug 会放大成全链路故障。
        """
        try:
            fn(*args)
        except Exception:  # noqa: BLE001 — 观测失败绝不外溢
            logger.debug("[link_health] 观测动作异常（已忽略，不影响请求）", exc_info=True)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        try:
            response = await self._inner.handle_async_request(request)
        except Exception as exc:
            kind = classify_exception(exc)
            if kind is not None:
                self._observe(
                    link_health.note_failure,
                    kind,
                    f"{type(exc).__name__}: {exc} — {request.method} {request.url}",
                )
            # 瞬时故障（超时/连接重置/5xx）不归本模块管，交给各调用点的重试逻辑
            raise

        kind = classify_status(response.status_code)
        if kind is not None:
            self._observe(
                link_health.note_failure,
                kind,
                f"HTTP {response.status_code} {request.method} {request.url.path}",
            )
        else:
            # 服务端给出了业务语义应答（含 400/404）→ 链路本身是通的
            self._observe(link_health.note_success)
        return response

    # 下面三个是**纯转发**，一个都不能少。
    # `httpx.AsyncClient.__aenter__` 会调 `self._transport.__aenter__()`（并对每个
    # mounts 代理也调一次），而基类 `AsyncBaseTransport.__aenter__` 只是 `return self`
    # ——包装器若不转发，内层连接池就永远不会被 enter/exit：
    #   · __aexit__ 不转发 → `async with observed_async_client(...)` 退出时**连接池不关闭**，
    #     每次调用泄漏一组连接（`async with observed_async_client(...)` 的用法）。
    #   · aclose 不转发   → 长连客户端 close() 时同样泄漏。
    # 包装器的第一原则是"除观测外行为完全一致"，任何生命周期钩子都必须透传。
    async def __aenter__(self) -> "ObservedTransport":
        await self._inner.__aenter__()
        return self

    async def __aexit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        await self._inner.__aexit__(exc_type, exc_value, traceback)

    async def aclose(self) -> None:
        await self._inner.aclose()


def observed_async_client(**kwargs) -> httpx.AsyncClient:
    """构建一个"所有出口都被观测"的 AsyncClient，且**不改变任何 httpx 默认行为**。

    做法：正常构造（不传 `transport=`，于是 trust_env / 代理 / limits / verify 全按
    httpx 默认规则装配），随后把装配好的 `_transport` 与 `_mounts` 各包一层观测。
    两个下划线属性是 httpx 的稳定内部结构，`tests/worker/test_link_health.py` 有用例在
    httpx 升级后改名时立刻报错，不会静默失效。

    直接传 `transport=ObservedTransport(...)` 是错的：那会触发
    `allow_env_proxies = trust_env and transport is None` 而禁用环境变量代理。
    """
    client = httpx.AsyncClient(**kwargs)
    client._transport = ObservedTransport(client._transport)
    client._mounts = {
        pattern: (ObservedTransport(t) if t is not None else None)
        for pattern, t in client._mounts.items()
    }
    return client
