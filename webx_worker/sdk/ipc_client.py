"""
IPC 客户端 — 子进程脚本通过本模块把"汇报失败事件"发回 worker 父进程。

设计语义：
  - 业务调用前台已经做了 5 次重试（在 result_client 内部），仍失败才调本模块
  - send_report() fire-and-forget：不阻塞业务，发不出就降级写文件
  - 父进程负责后台 consumer 协程轮询重试 + 退出时 snapshot

通道：
  - AF_UNIX socket，路径由父进程通过环境变量 WEBX_WORKER_IPC_SOCK 注入
  - lazy connect：首次 send 时建立长连接，后续复用
  - sendall 失败：重置 socket + 降级写文件

降级文件：
  - {WEBX_WORKER_WORK_DIR}/{task_id}/pending_reports.jsonl，append-only
  - 父进程不在运行中读这个文件；仅 worker 启动恢复时扫描

task 上下文：
  - set_task_context() / update_task_context()：业务脚本在主入口设置一次
  - send_report() 调用时自动注入 context，避免业务每次手填
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any

from webx_worker import envnames as E

logger = logging.getLogger(__name__)

_ENV_IPC_SOCK = E.ENV_IPC_SOCK
_ENV_WORK_DIR = E.ENV_WORK_DIR
_CONNECT_TIMEOUT_SEC = 2.0
_SEND_TIMEOUT_SEC = 2.0


# ─── task 上下文 ─────────────────────────────────────────────────────────────


class _TaskContext:
    """子进程内的任务快照，供 send_report 自动附加到 entry.context"""
    task_id: str = ""
    status: str = "running"
    sub_status: str = ""
    sub_step: str = ""
    end_step: str = ""
    steps_done: list[str] = []


_CTX = _TaskContext()
_CTX_LOCK = threading.Lock()


def set_task_context(
    task_id: str,
    status: str = "running",
    sub_status: str = "",
    sub_step: str = "",
    end_step: str = "",
    steps_done: list[str] | None = None,
) -> None:
    """子进程脚本启动时调用，初始化任务上下文。"""
    with _CTX_LOCK:
        _CTX.task_id = task_id
        _CTX.status = status
        _CTX.sub_status = sub_status
        _CTX.sub_step = sub_step
        _CTX.end_step = end_step
        _CTX.steps_done = list(steps_done) if steps_done else []


def update_task_context(**kwargs: Any) -> None:
    """步骤推进时增量更新上下文。"""
    with _CTX_LOCK:
        for k, v in kwargs.items():
            if hasattr(_CTX, k):
                if k == "steps_done" and v is not None:
                    _CTX.steps_done = list(v)
                else:
                    setattr(_CTX, k, v)


def _snapshot_context() -> dict:
    with _CTX_LOCK:
        return {
            "status": _CTX.status,
            "sub_status": _CTX.sub_status,
            "sub_step": _CTX.sub_step,
            "end_step": _CTX.end_step,
            "steps_done": list(_CTX.steps_done),
        }


# ─── IPC socket（lazy connect 长连接）──────────────────────────────────────


_SOCK: socket.socket | None = None
_SOCK_LOCK = threading.Lock()
_FILE_FALLBACK_ACTIVE: bool = False  # 一旦降级写文件，本次子进程内不再重试 IPC


def _connect_ipc() -> socket.socket | None:
    """建立 AF_UNIX 连接。失败返回 None。"""
    sock_path = os.environ.get(_ENV_IPC_SOCK, "")
    if not sock_path:
        return None
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(_CONNECT_TIMEOUT_SEC)
        s.connect(sock_path)
        s.settimeout(_SEND_TIMEOUT_SEC)
        return s
    except (FileNotFoundError, ConnectionRefusedError, OSError) as e:
        logger.warning(f"[IPC] 连接父进程失败: {e}，降级写文件")
        return None


def _send_via_socket(entry: dict) -> bool:
    """通过 IPC socket 发送一行 JSON。失败重置连接。"""
    global _SOCK, _FILE_FALLBACK_ACTIVE

    if _FILE_FALLBACK_ACTIVE:
        return False

    with _SOCK_LOCK:
        if _SOCK is None:
            _SOCK = _connect_ipc()
            if _SOCK is None:
                _FILE_FALLBACK_ACTIVE = True
                return False
        try:
            line = (json.dumps(entry, ensure_ascii=False) + "\n").encode("utf-8")
            _SOCK.sendall(line)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logger.warning(f"[IPC] 发送失败: {e}，标记降级")
            try:
                _SOCK.close()
            except Exception:
                pass
            _SOCK = None
            _FILE_FALLBACK_ACTIVE = True
            return False


# ─── 文件降级（仅在 IPC 不可用时使用）────────────────────────────────────


def _persist_to_file(entry: dict) -> None:
    """父进程信道不可用时，追加到任务目录 pending_reports.jsonl 等启动恢复"""
    work_dir = os.environ.get(_ENV_WORK_DIR, "")
    if not work_dir:
        logger.error(f"[IPC] 无 {_ENV_WORK_DIR} 环境变量，事件丢失: {entry}")
        return
    task_id = entry.get("task_id", "")
    if not task_id:
        logger.error(f"[IPC] 事件无 task_id，丢失: {entry}")
        return
    task_dir = Path(work_dir) / task_id
    try:
        task_dir.mkdir(parents=True, exist_ok=True)
        pending_file = task_dir / "pending_reports.jsonl"
        with pending_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.error(f"[IPC] 文件降级写入失败 task_id={task_id}: {e}")


# ─── 对外 API ────────────────────────────────────────────────────────────


def _fencing_snapshot() -> dict | None:
    """本子进程所属任务的 fencing 令牌（由 executor 注入 env）。取不到返回 None。

    与 `sdk.result_client._fencing_headers` 读同一对环境变量，只是这里要的是
    **可 JSON 序列化的值**（要随条目落盘），而不是 HTTP header。
    """
    worker_id = (os.environ.get(E.ENV_WORKER_ID) or "").strip()
    raw_seq = os.environ.get(E.ENV_LEASE_SEQ)
    if not worker_id or raw_seq is None:
        return None
    try:
        return {"worker_id": worker_id, "lease_seq": int(raw_seq)}
    except (TypeError, ValueError):
        # 令牌畸形 = 没有令牌（与服务端 fencing 的解析同口径：永不抛异常，取不到就是无令牌）
        return None


def send_report(action: str, payload: dict, *, task_id: str | None = None) -> None:
    """子进程把汇报失败事件发给父进程。fire-and-forget。

    Args:
        action: "update_step_status" / "update_task_fields"
        payload: HTTP body 内容
        task_id: 任务 ID，默认取 _CTX.task_id（set_task_context 设置过）
    """
    if task_id is None:
        task_id = _CTX.task_id
    if not task_id:
        logger.error(f"[IPC] send_report 未提供 task_id，事件丢失: action={action}")
        return

    entry = {
        "task_id": task_id,
        "action": action,
        "payload": payload,
        "context": _snapshot_context(),
        "ts": time.time(),
    }

    # fencing 令牌快照 —— 与 payload 一同落进条目。
    #
    # 本函数是**失败兜底**路径：事件会被父进程的 PendingReporter 在很久之后重放，
    # 甚至跨 worker 重启。那时谁都拿不到「当时是哪一次租约」了，只有条目自己记得。
    # 服务端 header / body 两条通道都认，重放侧走 body（见 pending_reporter._do_post）。
    fencing = _fencing_snapshot()
    if fencing:
        entry["fencing"] = fencing

    if _send_via_socket(entry):
        return

    _persist_to_file(entry)
