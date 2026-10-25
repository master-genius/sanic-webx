"""fencing 令牌注册表 —— 「我现在持有哪些任务、各自是第几次租约」。

## 为什么需要两半

`worker_id` 只能区分「哪台机器」，区分不了「哪一次租约」。反例：

    A 领到 t1 → 网络分区丢租约 → B 领走 → B 失败 → t1 回池 → 【A 再次领到 t1】
       ↓
    A 第一次运行残留的在途回调此刻到达：worker_id 恰好匹配 → 放行 → 污染第二次运行

加上单调递增的 `lease_seq` 才闭合。这是分布式系统里的标准 fencing token。

## 为什么走 header

各内部端点的请求体各不相同，塞进 body 要改一堆模型；走 header 则服务端一个装饰器
统一覆盖，与 X-Internal-Token 同一通道（docs/worker-protocol.md §2）。
body / 表单字段是第二通道，只给「令牌来自持久化条目快照」的异步路径用。

## 子进程怎么办

任务子进程经 `sdk.result_client` 直接调内部端点，拿不到本进程的内存。
故 executor 必须把令牌**注入子进程环境变量**（WEBX_WORKER_ID / WEBX_WORKER_LEASE_SEQ）。
漏了这一步，子进程的所有上报都会被 fencing 拒掉 —— 而 steps / fields / 产物上传
这些主力上报恰恰都在子进程里。
"""

from __future__ import annotations

import logging

from . import envnames as E

logger = logging.getLogger(__name__)

HEADER_WORKER_ID = E.HEADER_WORKER_ID
HEADER_LEASE_SEQ = E.HEADER_LEASE_SEQ

# 服务端 fencing 拒绝的三种原因。**必须区分**：压成一桶就永久失去了对真正异常的
# 告警能力，尤其 already_final 是幂等成功，当成「你丢了租约」会淹没真正的问题。
REASON_NOT_LESSEE = "not_lessee"
REASON_GONE = "gone"
REASON_ALREADY_FINAL = "already_final"

# task_id → lease_seq。由 lease 领取时写入、任务结束时移除。
_leases: dict[str, int] = {}


def register(task_id: str, lease_seq: int) -> None:
    """记录一次认领。"""
    _leases[task_id] = int(lease_seq)


def unregister(task_id: str) -> None:
    _leases.pop(task_id, None)


def snapshot() -> list[str]:
    """当前持有租约的全部 task_id（优雅退出时逐个归还）。"""
    return list(_leases.keys())


def lease_seq_of(task_id: str) -> int | None:
    """取该任务的租约代次；未持有租约（升级前遗留的在途任务）返回 None。"""
    return _leases.get(task_id)


def headers_for(task_id: str) -> dict[str, str]:
    """构造该任务的 fencing header。

    **无租约时返回空 dict**（不带任何 fencing header）。按协议，服务端对写操作
    一律强制 fencing，无令牌的请求会被拒（`missing_fencing_token` / `not_lessee`），
    所以「没有租约却发写请求」只会得到明确的 403，而不会被悄悄放行。
    """
    seq = _leases.get(task_id)
    if seq is None:
        return {}

    from .config import worker_settings

    return {
        HEADER_WORKER_ID: worker_settings.WORKER_ID,
        HEADER_LEASE_SEQ: str(seq),
    }


def env_for(task_id: str) -> dict[str, str]:
    """构造注入任务子进程的 fencing 环境变量（见模块 docstring）。"""
    seq = _leases.get(task_id)
    if seq is None:
        return {}

    from .config import worker_settings

    return {
        E.ENV_WORKER_ID: worker_settings.WORKER_ID,
        E.ENV_LEASE_SEQ: str(seq),
        E.ENV_TASK_ID: task_id,
    }


def is_terminal_reject(payload: dict) -> str | None:
    """判断一个 200/403 响应体是否为 fencing 的终态拒绝，是则返回 reason。

    调用方据此**丢弃**该上报而不是重试 —— `PendingReporter` 的设计语义是
    「永不退出，永不放弃」且跨重启持久化，若把永久性拒绝当成暂时故障，
    那条记录会永远重试下去，还会污染「队列长度即待送达上报数」这个运维信号。
    """
    if not isinstance(payload, dict) or payload.get("ok") is not False:
        return None
    reason = payload.get("reason")
    if reason in (REASON_NOT_LESSEE, REASON_GONE, REASON_ALREADY_FINAL):
        return str(reason)
    return None
