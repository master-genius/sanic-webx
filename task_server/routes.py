"""领取服务的三个端点。

## 两个语义完全不同的暴露面

| 端点 | 来源 | 鉴权 | 说明 |
|---|---|---|---|
| ``POST /task/leases`` | 外部 worker | **X-Internal-Token** | 领取任务，可挂起 |
| ``POST /task/pokes`` | 本机 Web / 补偿器 | **无**（靠只绑 127.0.0.1） | 通知"有新任务" |
| ``GET /health`` | 运维 | 无 | 存活探针 |

⚠️ 正因为 ``/task/pokes`` 不鉴权，本服务默认只绑 127.0.0.1。要让 worker 访问 ``/task/leases``，
应在反代上挂路径前缀（继承既有 TLS 与 IP 规则），**不要**把监听地址改成 0.0.0.0。

协议全文见 ``docs/worker-protocol.md`` §3。
"""

from __future__ import annotations

import asyncio
import logging
import time

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

from webx.config import settings
from webx.core.guards import require_internal_token
from webx.services.claim import claim_for_slots
from webx.services.lease import extract_fencing
from task_server.waiters import poke_signal, wait_for_poke

logger = logging.getLogger(__name__)

lease_bp = Blueprint("task_lease", url_prefix="/task")
health_bp = Blueprint("task_health")

# 单次请求最多接受的空闲槽位数。worker 的 Lane 实践中是 1~2 条；给一点余量即可，纯属防御畸形请求
_MAX_SLOTS = 16
_MAX_TYPES_PER_SLOT = 32


def parse_slots(body: dict) -> list[list[str]] | None:
    """解析并校验 slots。返回 None 表示格式非法。

    契约：``{"slots": [["a"], ["b", "c"]]}``，每个元素 = 一个空闲 Lane 能跑的类型列表。
    **没有 n 参数**——一条 Lane 一个槽。
    """
    raw = body.get("slots")
    if not isinstance(raw, list) or not raw or len(raw) > _MAX_SLOTS:
        return None
    slots: list[list[str]] = []
    for group in raw:
        if not isinstance(group, list) or not group or len(group) > _MAX_TYPES_PER_SLOT:
            return None
        types = [t.strip() for t in group if isinstance(t, str) and t.strip()]
        if not types:
            return None
        slots.append(types)
    return slots


def parse_filters(body: dict) -> tuple[int | None, int | None, str | None]:
    """解析可选筛选 ``max_level`` 与 ``max_compute_points``，都是**上限**语义。

    返回 ``(max_level, max_compute_points, 错误信息)``；两者均可为 None（= 不限）。

    为什么两个都用上限：语义上正好——"这台机器最高服务到哪一档"，``max_level=0`` 即只服务免费档；
    性能上最优——索引里 level / compute_points 都在，``<=`` 对应索引中一段连续且已排好序的区间。
    不需要下限：排序本来就是 ``level DESC``，不加筛选的 worker 天然优先拿高档位任务。真正的需求是**给低档位留产能**，
    那正是上限。
    """
    out: list[int | None] = []
    for key in ("max_level", "max_compute_points"):
        raw = body.get(key)
        if raw is None:
            out.append(None)
            continue
        try:
            v = int(raw)
        except (TypeError, ValueError):
            return None, None, f"invalid {key}"
        if v < 0:
            return None, None, f"invalid {key}"
        out.append(v)
    return out[0], out[1], None


@lease_bp.post("/leases")
@require_internal_token
async def create_lease(request: Request):
    """领取任务（长连接）。

    行为：立刻查一次；无任务则挂起，直到 ① 被 poke 唤醒并抢到任务，或 ② 挂满 ``TASK_LEASE_HOLD_SECONDS``。
    **两种情况都正常返回**，worker 收到空列表即重连。

    为什么空返回也是 200：挂起期间尚未认领任何任务，超时 / 断开**零损失**。这正是本模型不需要
    "保活一条长寿连接"的原因，也因此结构上不存在"半开连接静默停收任务"的故障。
    """
    body = request.json or {}
    # worker_id 与内部写端点的 fencing 令牌走**完全同一条规则**（header 优先，其次 body）。
    # 认领时写进行里的 worker_id，正是此后每一次回调要拿来比对的那半个令牌——两处的解析规则若有任何差异
    # （大小写、strip、优先级），就会出现"领得到任务、但所有上报都被判为不是租户"这种极难排查的错位。
    # lease_seq 在这一步无意义：它由服务端在认领时 +1 生成，故忽略。
    worker_id, _ = extract_fencing(request)
    if not worker_id:
        return json({"error": "worker_id required"}, status=400)

    slots = parse_slots(body)
    if slots is None:
        return json({"error": "invalid slots"}, status=400)

    # 筛选是**整台 worker 的属性**，放在请求顶层。只缩小候选集，不改变互斥语义——
    # 互斥恒由那条带条件的 UPDATE 保证
    max_level, max_points, err = parse_filters(body)
    if err:
        return json({"error": err}, status=400)

    deadline = time.monotonic() + settings.TASK_LEASE_HOLD_SECONDS
    while True:
        # ⚠️ 顺序不可颠倒：先取事件代次，再查库。反过来会丢唤醒（见 waiters 模块）
        event = poke_signal.current()
        try:
            claimed = await claim_for_slots(
                worker_id, slots, max_level=max_level, max_compute_points=max_points
            )
        except asyncio.CancelledError:
            raise  # 客户端断开：挂起期未认领任何东西，直接放行取消，无需善后
        except Exception as e:  # noqa: BLE001
            # DB 抖动不该让 worker 陷入紧密重试；交给它按退避策略重连
            logger.exception("[lease] 认领失败 worker_id=%s: %s", worker_id, e)
            return json({"error": "claim failed"}, status=503)

        if claimed:
            return json({"tasks": claimed, "count": len(claimed)})

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return json({"tasks": [], "count": 0})
        await wait_for_poke(event, min(remaining, settings.TASK_LEASE_FALLBACK_SECONDS))


@lease_bp.post("/pokes")
async def create_poke(request: Request):
    """通知"有新任务进入 PENDING"。**空 body，不带任何任务内容。**

    规则是"任何使任务进入 PENDING 的路径都要 poke"，不是给几处各打一个补丁。best-effort：poke 失败只损失延迟
    不损失正确性（任务已持久在 PENDING，兜底轮询也会发现它）。不鉴权：仅监听 127.0.0.1。
    """
    poke_signal.poke()
    return json({"ok": True})


@health_bp.get("/health")
async def health(request: Request):
    return json({"ok": True, "service": "task_server", "pokes": poke_signal.poke_count})
