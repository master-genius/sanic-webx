"""lease 分发 —— worker 主动来领（`POST /task/leases`，docs/worker-protocol.md §3.1）。

## Lane 从「队列」变成「槽位」

本模块**不预取**。一条 Lane 同时最多跑一个任务（`_lane_worker_loop` 是串行的），
跑完了才去领下一个。去掉本地队列并不是给 worker 瘦身 —— 执行逻辑一行没少，
少的是 worker 对云端状态的**本地副本**：

  · 不再需要「本地队列里这批任务在云端是什么状态」这份会过期的镜像
  · 不再需要 QueuedHeartbeat 替排队中的任务续命
  · 不再有「队列满了 await put() 卡住 dispatcher」的跨 Lane 队头阻塞
  · 不存在「收到不认识的 channel 就静默丢弃」这条路径 —— 只索取自己跑得了的类型

## 一个 worker 一条请求，不是一个空槽一条

挂起连接数因此是「每 worker 一条」而非「每空槽一条」，直接降低 领取服务 的负担。

## 回包如何对应到 Lane：按 task_type 查，**不能按下标**

服务端按槽位顺序尝试认领，但**只把成功的追加进返回列表**。若槽位 0（重型）没领到、
槽位 1（轻型）领到了，返回列表只有一个元素 —— 此时按下标 zip 会把轻型任务派给
重型 Lane。而 `task_to_lane` 是 dict（每个类型恰属一条 Lane），按类型查天然正确，
且不需要改协议。

## 断线重连是设计的基本性质，不是补丁

挂起中的请求**尚未认领任何任务**，连接被掐断则 DB 未改动、零损失、重连即可。
因为没有需要保活的长寿连接，也就不存在「socket 还是 ESTAB 但已经死了、
静默停收任务、必须人工重启」的半开故障。
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

from . import envnames as E
from .config import worker_settings
from .fencing import register as fence_register
from .link_health import observed_async_client

logger = logging.getLogger(__name__)

# 在跑的任务：task_id → asyncio.Task。
# 做成**模块级**而非 LeaseDispatcher 的实例属性，理由与 release_held_leases 相同：
# 停机路径上 `__main__` 拿不到 dispatcher 实例（它建在 run_worker 内部）。
_running_tasks: dict[str, asyncio.Task] = {}

#: 停机排空标志：置位后领取循环不再发请求，且把已经领到手的新任务原样还回去。
#: 模块级而非挂在 dispatcher 上，因为停机路径（app.async_main）拿不到 dispatcher 实例，
#: 与 `_running_tasks` / `release_held_leases` 同理。
_draining = False

# 退避：正常返回立即重连（那是心跳式的常态，不是错误）；只有真出错才退避。
_BACKOFF_BASE_SEC = 1.0
_BACKOFF_MAX_SEC = 30.0
# 抖动**不是可选的**：领取服务 重启后所有 worker 会在同一瞬间重连，
# 不加抖动就是惊群，会把刚起来的服务再打趴一次。
_JITTER_RATIO = 0.3
# 401/403 = 确定性拒绝（token 不对），重试无意义，等运维介入
_NO_RETRY_CODES = {401, 403}
# ack 失败后本 Lane 的冷却时长（秒）。ack 打的是任务服务，它不可用通常以分钟计，
# 5s 足以把热循环压到可忽略，又不至于让一次偶发抖动白等太久。
_ACK_FAIL_COOLDOWN_SEC = 5.0
# 「没有任何可领类型」时的空转间隔（秒）。空 LANES 已按文档归入单 Lane，
# 正常配置下走不到这里；保留它只为兜住「channel 全被过滤掉」之类的真配置错误，
# 做成模块常量是为了测试里可以把它压短。
_NO_SLOT_SLEEP_SEC = 5.0


class LeaseClient:
    """/task/leases 的 HTTP 客户端。

    独立于 `callback_client`：那个客户端的默认超时是 30s（上传等慢接口共用），
    而 lease 请求要挂起到 HOLD_SECONDS，需要更长且独立的超时。
    """

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    def _get(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            # ⚠️ 必须走 observed_async_client，不能裸建 —— 领取是 worker 唯一一条
            # 「配错就整站零分发」的通道，偏偏也是全仓最后一条绕过链路观测的：
            # 401（token/反代配错）在别的通道上会汇成一条 ERROR 告警，在这里却只有
            # `acquire()` 里逐请求的一行日志。而这条通道恰恰最需要那条告警——
            # 它的症状（一个任务都领不到）与「回调自检通过」同时出现时，
            # 运维根本无从判断该查哪一端。
            #
            # 包装器只记账、不改行为：超时/连接错归为瞬时故障（classify_exception
            # 只认 TLS），所以正常挂起 25s 再返回不会被误报。
            # `tests/worker/test_link_health.py` 有全包 AST 扫描守着这条规则。
            #
            # 超时须 > 服务端的 hold 时长，否则每次都在服务端正常挂起时被客户端掐断
            self._client = observed_async_client(
                headers={
                    E.HEADER_INTERNAL_TOKEN: worker_settings.INTERNAL_TOKEN,
                    "Content-Type": "application/json",
                    # worker_id **同时**走 header 与 body（协议 §3.1：两者都认，header 优先）。
                    # 放在客户端默认头里而不是逐请求塞 —— 它是整台机器的常量，
                    # 与 per-task 的 fencing 令牌（那个必须逐请求算）性质完全不同。
                    E.HEADER_WORKER_ID: worker_settings.WORKER_ID,
                },
                timeout=worker_settings.LEASE_HOLD_SECONDS + 15.0,
            )
        return self._client

    async def acquire(self, slots: list[list[str]]) -> list[dict[str, Any]] | None:
        """请求领取。返回任务列表（可能为空），出错返回 None（调用方据此退避）。

        Args:
            slots: 每个元素 = 一个空闲 Lane 能跑的类型列表。**无 n 参数**——一 Lane 一槽。
        """
        # 空 = 回落 WEB_BASE_URL：常见部署是在同一反代下给领取服务挂路径前缀，
        # 这样继承既有的 TLS 与 IP 规则，**不新增对外的防火墙面**。
        base = worker_settings.TASK_SERVER_URL or worker_settings.WEB_BASE_URL
        url = f"{base.rstrip('/')}/task/leases"
        payload: dict[str, Any] = {
            "worker_id": worker_settings.WORKER_ID, "slots": slots,
        }
        # 筛选是整台机器的属性，放在请求顶层而非每个槽位里。
        # **不配置就不带这两个键**，请求体与未引入筛选时逐字节一致。
        from .config import lease_filters

        max_level, max_points = lease_filters()
        if max_level is not None:
            payload["max_level"] = max_level
        if max_points is not None:
            payload["max_compute_points"] = max_points
        try:
            resp = await self._get().post(url, json=payload)
        except Exception as e:
            # **必须带上 url**：领取通道与回调通道是两个不同的地址，而失败时
            # 两者的日志长得一模一样。不打出来的话，「回调自检通过、领取一直连不上」
            # 这种最常见的组合无从判断该查哪一个 —— 实跑时正是这样卡住的。
            #
            # 这里也是唯一能看出「TASK_SERVER_URL 为空回落到了 WEB_BASE_URL」的地方：
            # 回落之后打的是任务服务的端口，而它上面未必有 /task/leases 这条路由。
            logger.warning("[lease] 请求失败（将退避重连）：%s ← %s", e, url)
            return None

        if resp.status_code == 200:
            return resp.json().get("tasks") or []
        if resp.status_code in _NO_RETRY_CODES:
            logger.error(
                "[lease] 鉴权被拒 %s —— 需运维介入（token 配置）← %s",
                resp.status_code, url,
            )
            return None
        if resp.status_code == 404:
            # 404 有一个压倒性常见的成因，直接把结论写出来，别让人去猜：
            # 反代没配 /task/leases 的转发，请求打到了没有这条路由的服务上。
            # 它的症状是**整站零分发**，而这行日志是唯一线索。
            logger.error(
                "[lease] 404 ← %s —— 该地址上没有 /task/leases 路由。"
                "多半是反代未配置转发，或 WEBX_WORKER_TASK_SERVER_URL 为空而回落到了 WEBX_WORKER_WEB_BASE_URL",
                url,
            )
            return None
        logger.warning("[lease] 服务端返回 %s（将退避重连）← %s", resp.status_code, url)
        return None

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        self._client = None


lease_client = LeaseClient()


class LeaseDispatcher:
    """按空闲 Lane 领取任务并交给对应 Lane 执行。

    **没有队列，也没有背压判断**。
    「没空槽就不发请求」本身就是背压 —— 任务安静地待在 DB 的 PENDING 池里，
    不需要任何绕行或丢弃逻辑。
    """

    def __init__(self, lane_manager, executor) -> None:
        self._lm = lane_manager
        self._executor = executor
        # lane_id → 是否正在跑任务。Lane 是槽位，值只有 True/False。
        self._busy: dict[int, bool] = {i: False for i in range(lane_manager.num_lanes)}
        self._freed = asyncio.Event()
        self._running = False
        self._backoff = 0.0

    def _lane_types(self, lane_id: int) -> list[str]:
        """该 Lane 能跑的任务类型 —— 直接来自 worker 本地的 task_to_lane 映射。

        **能力是 worker 的权威**，随请求带给服务端即可，服务端不需要也不应该
        维护一张能力注册表（那会变成一份必然过期的镜像）。
        """
        return self._lm.types_of(lane_id)

    def _idle_lanes(self) -> list[int]:
        return [i for i, busy in self._busy.items() if not busy]

    async def _run_one(self, lane_id: int, task: dict[str, Any]) -> None:
        """在指定 Lane 上执行一个任务，结束后释放槽位。"""
        task_id = str(task.get("id") or "")
        try:
            # 令牌先落注册表，再做任何上报 —— 顺序反了的话 ack 就不带 fencing header
            fence_register(task_id, int(task.get("lease_seq") or 0))

            # 立刻 ack：把认领的短租约续到常规时长（见 callback.ack_lease 的说明）。
            # ack 失败就放弃本次执行：任务会在短租约到期后自动回池，
            # 比抱着一个可能已被回收的任务硬跑安全得多。
            from .callback import callback_client

            if not await callback_client.ack_lease(task_id):
                logger.warning(
                    "[lease] ack 失败，放弃执行 task_id=%s（冷却 %.0fs 再领下一个）",
                    task_id, _ACK_FAIL_COOLDOWN_SEC,
                )
                # ⚠️ **必须冷却**，否则会空转啃整个池子：
                # ack 打的是任务服务，而领取服务 / DB 可能好好的。任务服务一挂，
                # 「认领 → ack 失败 → 放弃 → 立刻再认领」就是一个没有任何节流的热循环，
                # 每转一圈就把一个任务翻成 queued 挂短租约。配合租约回收，
                # 整池任务会被一起推向服务端的重投上限。
                #
                # 冷却放在 return 之前（而不是之后）是刻意的：finally 里才释放槽位，
                # 所以这段时间本 Lane 保持 busy，主循环不会绕过冷却又去领。
                #
                # 取固定 5s 而不是复用 _sleep_backoff：那个退避在每次 acquire 成功时
                # 就被清零，而这里 acquire 恰恰是成功的（坏的是任务服务），指数增长根本长不起来。
                await asyncio.sleep(_ACK_FAIL_COOLDOWN_SEC)
                return

            # 上报 running（PUT /status）—— **不可省**。它做两件事，缺一样都会出问题：
            #   ① 把任务从 QUEUED 翻到 RUNNING（否则库里永远显示排队中）；
            #   ② 服务端据此写 start_time，而 grace = α × (now - start_time) 全靠它——
            #      start_time 为空时 grace 恒取下限，「跑得越久越有耐心」直接失效。
            # 3 次重试，失败即放弃执行（协议 §7.2）。
            running_reported = False
            for attempt in range(3):
                if await callback_client.update_status(task_id, "running"):
                    running_reported = True
                    break
                if attempt < 2:
                    await asyncio.sleep(1.0)

            if not running_reported:
                # 与 ack 失败同一判断：宁可不跑，也不抱着一个云端不知情的任务硬跑。
                # 差别是这里**主动归还租约**（带 fencing，退不掉别人的），
                # 让任务立刻回池而不是干等一个完整租约周期。
                logger.error(
                    "[lease] 上报 running 连续失败，放弃执行 task_id=%s（归还租约回池）",
                    task_id,
                )
                await callback_client.release_lease(task_id)
                await asyncio.sleep(_ACK_FAIL_COOLDOWN_SEC)
                return

            # ⚠️ 两个参数：execute(task_id, payload)。
            # 曾误写成 execute(_to_payload(task)) —— payload 里虽然带着 task_id，
            # 但签名要的是分开的两个位置参数，于是**每个任务一领到就 TypeError**，
            # 主路径彻底不工作。
            await self._executor.execute(task_id, _to_payload(task))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[lease] 执行异常 task_id=%s: %s", task_id, e, exc_info=True)
            # 异常逃到这里 = **本轮什么都没干成**。executor 内部会自行处理业务异常并
            # 上报 fail，能穿透到这里的是结构性故障（如签名不匹配、导入错误）。
            #
            # 必须归还租约，否则任务要抱着租约干等一个完整周期才被回收，
            # 而 worker 重启也救不了它：任务目录是在 execute() **内部**创建的，
            # 若异常发生在进入 execute 之前，本地压根没有目录，recovery 扫不到、
            # 无从归还 —— 那时唯一的出路只剩补偿器的租约回收。
            # 归还带 fencing，退不掉别人的租约。
            try:
                # 就地 import：上面那个 `from .callback import ...` 在 try 内部，
                # 异常若发生在它之前（如 fence_register），这里引用它会是 NameError。
                from .callback import callback_client as _cb

                await _cb.release_lease(task_id)
            except Exception:  # noqa: BLE001 —— 归还失败只是慢一点，由租约到期兜底
                pass
        finally:
            from .fencing import unregister as fence_unregister

            fence_unregister(task_id)
            self._busy[lane_id] = False
            self._freed.set()

    async def loop(self) -> None:
        """主循环：有空槽就去领，没空槽就等有 Lane 空出来。"""
        # worker_id 是 fencing 令牌的一半，空值会让所有上报被服务端判为「不是租户」。
        # 那是配置故障，必须**响亮失败**而不是带着空令牌跑起来 —— 后者的现象是
        # 「任务领得到、跑得完、但所有上报都被拒」，极难排查。
        if not (worker_settings.WORKER_ID or "").strip():
            logger.error(
                "[lease] WORKER_ID 为空，拒绝启动领取循环。"
                "正常启动路径会由 config.derive_worker_id() 派生；"
                "若从非标准入口启动，请显式设置 WEBX_WORKER_ID 或 WEBX_WORKER_PREFIX。"
            )
            return

        self._running = True
        global _draining
        _draining = False
        from .config import lease_filters

        lv, mp = lease_filters()
        logger.info(
            "[lease] 分发循环启动 worker_id=%s lanes=%s 档位上限=%s 算力点上限=%s",
            worker_settings.WORKER_ID, self._lm.task_to_lane,
            lv if lv is not None else "不限", mp if mp is not None else "不限",
        )

        while self._running and not _draining:
            idle = self._idle_lanes()
            if not idle:
                # 满载 = 天然背压。不发请求，任务就安静待在 DB 池里。
                self._freed.clear()
                await self._freed.wait()
                continue

            slots = [self._lane_types(i) for i in idle]
            slots = [s for s in slots if s]
            if not slots:
                # 该 Lane 没配任何任务类型（配置问题），别空转打服务端
                await asyncio.sleep(_NO_SLOT_SLEEP_SEC)
                continue

            tasks = await lease_client.acquire(slots)

            if _draining:
                # 长轮询在途时开始了停机，回包里仍可能带着刚认领的任务：不执行，立刻还回池。
                # 否则它会停在 queued，要等 60s 短租约到期才被回收（停机窗口，见 begin_drain）。
                if tasks:
                    await _give_back_claimed(tasks)
                return

            if tasks is None:
                await self._sleep_backoff()
                continue
            self._backoff = 0.0  # 成功一次即清零

            if not tasks:
                # 服务端挂满 hold 后的正常空返回 —— **立即重连，不退避**
                continue

            for task in tasks:
                lane_id = self._lm.task_to_lane.get(task.get("task_type"))
                if lane_id is None or self._busy.get(lane_id):
                    # 理论上不会发生（只索取了空闲 Lane 的类型）。真发生了说明
                    # 配置与请求不一致，此时**不执行**：任务会在短租约到期后回池，
                    # 比派给错误的 Lane 安全。
                    logger.error(
                        "[lease] 回包无法映射到空闲 Lane（type=%s lane=%s），放弃 task_id=%s",
                        task.get("task_type"), lane_id, task.get("id"),
                    )
                    continue
                self._busy[lane_id] = True
                t = asyncio.create_task(self._run_one(lane_id, task))
                # 登记在册，停机时才取消得掉（见 cancel_running_tasks）。
                # done 回调自动摘除，跑完即不再占位。
                tid = str(task.get("id") or "")
                if tid:
                    _running_tasks[tid] = t
                    t.add_done_callback(lambda _t, k=tid: _running_tasks.pop(k, None))

    async def _sleep_backoff(self) -> None:
        self._backoff = min(
            _BACKOFF_MAX_SEC, self._backoff * 2 if self._backoff else _BACKOFF_BASE_SEC
        )
        delay = self._backoff * (1 + random.uniform(-_JITTER_RATIO, _JITTER_RATIO))
        await asyncio.sleep(max(0.1, delay))

    def stop(self) -> None:
        self._running = False
        self._freed.set()

    async def release_all(self) -> None:
        """优雅退出时归还所有在手租约，让任务**立刻**回池而不是等租约到期。

        为什么值得做：worker 自己最清楚「我不干了」，而云端只能靠超时猜。
        把这个信息说出来，滚动重启的恢复耗时从一个完整租约周期压到近乎 0。

        归还带 fencing，所以即使这台 worker 早已丢了租约也不会误退别人的。
        实现见模块级的 `release_held_leases`（停机路径要在更早的时刻调它）。
        """
        await release_held_leases()


def begin_drain() -> None:
    """**同步**停止领取新任务。停机收尾的第一步，必须先于令牌捕获 / 取消 / 归还。

    ## 要关掉的窗口

    归还租约要 await；await 期间领取循环仍在运行，空闲 Lane 会把**刚归还的任务又领回来**，
    回池变成了 queued，而此刻进程马上退出、没人再 ack 它——只能等 60s 短租约到期。
    每次滚动重启都有概率让一个任务白白多等一分钟。

    只置标志，不取消 dispatcher：真正的取消仍在收尾末尾（`worker_task.cancel()`），
    这里只保证"从此刻起不会再有新的认领被执行"。
    """
    global _draining
    _draining = True


async def _give_back_claimed(tasks: list[dict[str, Any]]) -> None:
    """排空期间领到的任务：带着刚拿到的令牌原样归还，不执行。"""
    from .fencing import headers_for, unregister

    tokens: dict[str, dict] = {}
    for task in tasks:
        tid = str(task.get("id") or "")
        if not tid:
            continue
        fence_register(tid, int(task.get("lease_seq") or 0))
        tokens[tid] = headers_for(tid)
    try:
        await release_held_leases(tokens)
    finally:
        for tid in tokens:
            unregister(tid)


def cancel_running_tasks() -> int:
    """**同步**取消所有在跑的任务，返回取消了几个。

    ## 为什么必须有它，而且必须是同步的

    停机时 systemd 按 cgroup 一并 SIGTERM，**在跑任务的子进程同时被杀**。
    executor 看到的只是「子进程非零退出」，于是走 `except Exception` 那条普通失败
    分支去调 `fail_task` —— 而那一刻租约还在自己手上，fencing 会**放行**：
    一次运维重启就把一个本可自动回池重跑的任务判成 failed，要用户手动 retry。

    只把归还提前是**不够的**：归还要 await，await 期间事件循环照样在推进那条失败链，
    两个 POST 并发，先后仍是概率问题（归还赢面很大，但不是保证）。

    `Task.cancel()` 是**同步**的：它在本轮任何 await 之前就把取消排进去，那条失败链
    因此在下一次被调度时拿到的是 `CancelledError`，走的是 `execute` 的**取消分支**
    （写 ok.json cancelled、**绝不上报 fail**）。先后于是从概率变成结构。

    ## 这不是新状态

    「任务非终态 + 本地 ok.json 记 cancelled + 目录留有半成品」这个组合，
    心跳自熔（`lease_expired`）与被服务端吊销（`revoked`）时**每天都在走**：
    两者同样让 `run_subprocess` 返回 -1、同样写 cancelled、同样等着被重新领取。
    本函数只是让停机也走进这条已经跑熟的路，而不是新造一条。

    ⚠️ 调用顺序：**先捕获令牌（`held_fencing_headers`）→ 再取消 → 最后归还**。
    取消会触发 `_run_one` 的 finally → `fence_unregister`，注册表随之清空；
    归还若还指望钩子去现取令牌，就会拿到空手而被判 `missing_fencing_token`。
    """
    tasks = [t for t in _running_tasks.values() if not t.done()]
    for t in tasks:
        t.cancel()
    return len(tasks)


def held_fencing_headers() -> dict[str, dict]:
    """**同步**捕获当前在手租约的 fencing 令牌：`{task_id: headers}`。

    停机路径专用，且必须在 `cancel_running_tasks()` **之前**调用 —— 理由见那里。
    """
    from .fencing import headers_for, snapshot

    return {tid: headers_for(tid) for tid in snapshot()}


async def release_held_leases(tokens: dict[str, dict] | None = None) -> int:
    """归还当前在手的全部租约。**并发、带 fencing、幂等**，返回归还了几个。

    做成模块级函数而不是只当 `LeaseDispatcher` 的方法：停机路径上 `__main__`
    必须在取消 dispatcher **之前**先把租约还掉（见那里的推导），而那一刻拿不到
    dispatcher 实例。两处共用同一份实现，避免口径漂移。

    **并发而非逐个**：单次 release_lease 的超时是 10s，任务服务不可达时串行归还会
    线性吃掉停机预算（2 条 Lane 就是 20s，叠加 uploader 的 10s 宽限期正好顶满
    unit 的 TimeoutStopSec=30，再多一条 Lane 就要被 SIGKILL 打断）。并发之后
    这一段恒为一个超时的时长，与 Lane 数无关。归还彼此独立，没有需要串行的理由。

    **幂等**：重复归还只会拿到 403（已非租户），`release_lease` 视其为正常结果。

    `tokens`：预先捕获的 `{task_id: fencing headers}`。停机路径必须传它 ——
    那条路上在跑任务已被同步取消，注册表随时会被它们的 finally 清空，
    临到发请求时再去现取就是空手（见 `cancel_running_tasks`）。不传则现取，
    供 dispatcher 正常退出时使用。
    """
    from .callback import callback_client

    if tokens is None:
        tokens = held_fencing_headers()
    if not tokens:
        return 0
    logger.info("[lease] 归还 %d 个在手租约", len(tokens))
    await asyncio.gather(
        *(callback_client.release_lease(tid, extra_headers=h) for tid, h in tokens.items()),
        return_exceptions=True,
    )
    return len(tokens)


def _to_payload(task: dict[str, Any]) -> dict[str, Any]:
    """把认领回来的任务行整形成 executor 期望的 payload。

    字段见协议 §3.1 的 `<Task>`；`expected_output` 已从协议中删除，不再透传。
    """
    import json as _json

    steps = task.get("steps")
    if isinstance(steps, str):
        try:
            steps = _json.loads(steps) if steps else []
        except Exception:
            steps = []

    return {
        "task_id": task.get("id"),
        "name": task.get("name") or "",
        "task_type": task.get("task_type"),
        "files_json": task.get("files_json"),
        "params_text": task.get("params_text"),
        "allowed_retries": task.get("allowed_retries"),
        "steps": steps or [],
        "sub_step": task.get("sub_step") or "",
        "end_step": task.get("end_step") or "",
        "timeout_seconds": task.get("timeout_seconds", 172800),
    }
