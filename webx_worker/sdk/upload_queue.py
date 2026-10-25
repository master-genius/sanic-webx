"""异步上传队列 —— **磁盘目录即消息**，生产者与消费者之间零通知、零回边。

大产物（数 GB 的轨迹/模型文件）在任务子进程里同步上传，意味着算完之后 Lane 还要被占住几十分钟，
GPU/CPU 全程闲着。本模块把这类上传摘出计算的关键路径：子进程只往队列**投一张条子**，
真正的上传交给独立的 uploader 进程（见 ``webx_worker/uploader.py``）。

【为什么用磁盘目录，不用 IPC / 内存队列】
  · 通知需要"送达保证"，而写文件成功**就是**送达——不需要重连、缓冲、丢失补偿
  · 子进程可能在通知发出前被 kill（取消 / OOM），条子已经在盘上了
  · uploader 可能没在运行（崩溃重启的空档），目录不在乎谁在听
  · 进程崩溃/重启天然幸存，无须 snapshot 那类补丁
  · 可观测：``ls`` 就知道积压多少；父进程据此做背压，同样不需要谁通知谁

【目录结构】``{WORK_DIR}/.uploads/``
  ``pending/{task_id}__{step}_{field}.json``    待上传
  ``inflight/{task_id}__{step}_{field}.json``   正在传（uploader 启动时搬回 pending）

【落库字段一直是空串】投递时不写任何占位值，uploader 传完直接 merge 真 URL 进去。
不搞占位哨兵是刻意的：占位值要先由任务脚本推上云，而那发生在**本步全部产物传完之后**，
中间这几十秒里 uploader 已经扫到条目、问云端、发现"没有我这个占位值"——就会把还没
发布的条目当成过期的删掉。空串没有这个先后次序问题，前端也不必认识新前缀。

【条目文件名即身份】``{task_id}__{step}_{field}.json``（如 ``t1__step1_out.json``）。
从 key_path 派生而非取随机数，除了直观，更重要的是**天然幂等**：同一个
(任务, 步骤, 字段) 恒得同一个文件名，重复投递直接覆盖，队列里不可能出现两条指向
同一产物的条目。
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

QUEUE_DIRNAME = ".uploads"
PENDING_DIR = "pending"
INFLIGHT_DIR = "inflight"

# ==================== 条目 ====================


# 文件名净化：key_path 通常是任务脚本自己生成的（``{步骤}_{序号}`` / ``out_url``），
# 但其中若混入来自用户的名字，文件名里不能出现路径分隔符等危险字符。这里兜一层，纯属防御。
_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_SLUG = 120


def entry_slug(key_path: List[str]) -> str:
    """key_path → 条目文件名主体。``["step1","out_url"] → "step1_out"``。

    尾部的 ``_url`` 去掉纯为可读（``step1_out`` 比 ``step1_out_url`` 顺眼）；
    字段本身就叫 ``url`` 的保持不动，否则会缩成空串。
    """
    parts = [str(k) for k in key_path]
    if parts and parts[-1] != "url" and parts[-1].endswith("_url"):
        parts[-1] = parts[-1][: -len("_url")]
    raw = "_".join(p for p in parts if p)
    return (_UNSAFE_CHARS.sub("-", raw).strip("-.")[:_MAX_SLUG]) or "entry"


@dataclass
class Entry:
    """一条待上传记录。``path`` 是它当前所在的文件位置（pending/ 或 inflight/）。"""

    task_id: str
    local_path: str
    remote_name: str
    # 回写位置：result_path 里的嵌套路径，如 ["step2", "out_url"]。
    # 任意深度都行（如 ["group", "item", "file"]），服务端按嵌套 key 幂等 merge（协议 §4.9）。
    key_path: List[str]
    size: int = 0
    queued_at: float = 0.0
    attempts: int = 0
    # 两阶段：上传成功后原地记下 URL，动作降级为"纯回写"。
    # 否则"上传 200、回写失败"会让几个 GB 重传一遍。
    uploaded_url: str = ""
    # 分片续传会话：崩溃重启后凭它 GET offset 接着传，不从头再来几 GB
    session_id: str = ""
    first_queued_at: float = 0.0

    path: Optional[Path] = field(default=None, compare=False, repr=False)

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "path"}
        return d

    @property
    def filename(self) -> str:
        """条目身份 = 从 (task_id, key_path) 派生，**不是随机数**。

        因此同一个 (任务, 步骤, 字段) 恒得同一个文件名：重复投递直接覆盖，
        队列里不可能出现两条指向同一产物的条目（redo 重跑同一步时尤其重要）。
        """
        return f"{_UNSAFE_CHARS.sub('-', self.task_id)}__{entry_slug(self.key_path)}.json"


def _from_dict(d: dict, path: Path) -> Optional[Entry]:
    try:
        return Entry(
            task_id=str(d["task_id"]),
            local_path=str(d["local_path"]),
            remote_name=str(d["remote_name"]),
            key_path=[str(k) for k in (d.get("key_path") or [])],
            size=int(d.get("size") or 0),
            queued_at=float(d.get("queued_at") or 0.0),
            attempts=int(d.get("attempts") or 0),
            uploaded_url=str(d.get("uploaded_url") or ""),
            session_id=str(d.get("session_id") or ""),
            first_queued_at=float(d.get("first_queued_at") or 0.0),
            path=path,
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.warning(f"[upload_queue] 条目结构非法，跳过: {path.name}, {e}")
        return None


# ==================== 目录 ====================


def queue_root(work_dir: str | Path) -> Path:
    return Path(work_dir) / QUEUE_DIRNAME


def pending_dir(work_dir: str | Path) -> Path:
    return queue_root(work_dir) / PENDING_DIR


def inflight_dir(work_dir: str | Path) -> Path:
    return queue_root(work_dir) / INFLIGHT_DIR


def ensure_dirs(work_dir: str | Path) -> None:
    pending_dir(work_dir).mkdir(parents=True, exist_ok=True)
    inflight_dir(work_dir).mkdir(parents=True, exist_ok=True)


def _atomic_write(target: Path, payload: dict) -> None:
    """同目录 .tmp + os.replace 原子落位。

    必须同目录：跨文件系统的 rename 不是原子操作，会退化成 copy+unlink，
    中途崩溃就留下半截 JSON——而消费者随时可能读到它。
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.{secrets.token_hex(4)}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)


# ==================== 投递侧（任务子进程调用） ====================


def enqueue(
    work_dir: str | Path,
    task_id: str,
    local_path: str | Path,
    remote_name: str,
    key_path: List[str],
) -> bool:
    """投一张上传条子。成功 True / 失败 False。

    落库字段一律保持**空串**——不写任何占位值。占位值必须由任务脚本推上云才生效，
    而那发生在本步全部产物传完之后；中间几十秒里 uploader 已经扫到条目、问云端、
    发现"没有我这个占位值"，就会把还没发布的条目当过期的删掉。空串没有这个次序问题。

    投递失败返回 False —— 调用方据此退回同步上传，绝不因为投递失败让步骤失败：
    产物上传本就是尽力而为，队列只是让它更可靠，不该成为新的失败源。

    ⚠️ 不复制文件。条目只记路径，几 GB 的产物复制一份是不可接受的；
    对应地，清理侧必须跳过仍被队列引用的文件。
    """
    src = Path(local_path)
    if not src.is_file():
        logger.warning(f"[upload_queue] 投递跳过（文件不存在）: {src}")
        return False

    now = time.time()
    entry = Entry(
        task_id=task_id,
        local_path=str(src.resolve()),
        remote_name=remote_name,
        key_path=list(key_path),
        size=src.stat().st_size,
        queued_at=now,
        first_queued_at=now,
    )
    try:
        _atomic_write(pending_dir(work_dir) / entry.filename, entry.to_dict())
    except OSError as e:
        logger.error(f"[upload_queue] 投递失败: {remote_name}, {e}")
        return False

    logger.info(
        f"[upload_queue] 已投递: task={task_id} file={remote_name} "
        f"size={entry.size} entry={entry.filename}"
    )
    return True


# ==================== 消费侧（uploader 进程调用） ====================


def _load_dir(d: Path) -> List[Entry]:
    if not d.is_dir():
        return []
    out: List[Entry] = []
    for f in sorted(d.glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            # 半截/损坏的条目：只可能来自非原子写入的历史残留。记一笔并删掉，
            # 留着会让每一轮扫描都重复报错，反而淹没真问题。
            logger.warning(f"[upload_queue] 条目损坏，已删除: {f.name}, {e}")
            f.unlink(missing_ok=True)
            continue
        e2 = _from_dict(data, f)
        if e2 is not None:
            out.append(e2)
    return out


def iter_pending(work_dir: str | Path) -> List[Entry]:
    """列出待上传条目（按文件名排序，天然按 task 聚集）。"""
    return _load_dir(pending_dir(work_dir))


def claim(work_dir: str | Path, entry: Entry) -> Optional[Entry]:
    """pending → inflight。返回新位置的条目；已被取走则返回 None。

    用 os.rename 而非"读+标记"：rename 在同目录内是原子的，天然做到"只有一个
    消费者能拿到"，即便将来 uploader 起多实例也不必加锁。
    """
    src = pending_dir(work_dir) / entry.filename
    dst = inflight_dir(work_dir) / entry.filename
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(src, dst)
    except OSError:
        return None
    entry.path = dst
    return entry


def release(work_dir: str | Path, entry: Entry) -> None:
    """inflight → pending，attempts +1，保留 uploaded_url / session_id。

    保留这两个字段是关键：上传成功但回写失败时，下一轮只做回写，不重传几个 GB；
    分片传到一半失败时，下一轮凭 session_id 取 offset 接着传。
    """
    entry.attempts += 1
    entry.queued_at = time.time()
    try:
        _atomic_write(pending_dir(work_dir) / entry.filename, entry.to_dict())
        (inflight_dir(work_dir) / entry.filename).unlink(missing_ok=True)
    except OSError as e:
        logger.error(f"[upload_queue] 退回队列失败: {entry.filename}, {e}")
        return
    entry.path = pending_dir(work_dir) / entry.filename


def update(work_dir: str | Path, entry: Entry) -> None:
    """原地更新 inflight 中的条目（记 uploaded_url / session_id，不改变所在队列）。"""
    try:
        _atomic_write(inflight_dir(work_dir) / entry.filename, entry.to_dict())
    except OSError as e:
        logger.error(f"[upload_queue] 条目更新失败: {entry.filename}, {e}")


def drop(work_dir: str | Path, entry: Entry) -> None:
    """彻底移除条目（传完了，或云端已判定不需要）。两个目录都清，幂等。"""
    (pending_dir(work_dir) / entry.filename).unlink(missing_ok=True)
    (inflight_dir(work_dir) / entry.filename).unlink(missing_ok=True)


def requeue_inflight(work_dir: str | Path) -> int:
    """把 inflight/ 里的条目搬回 pending/ —— uploader 启动的第一件事。

    inflight 里的东西只可能是"上次崩在半路"的：进程没了，没人在传它们。
    幂等，跑几次都无害。

    ⚠️ pending 已存在同名条目时**不覆盖，只删 inflight 那份**：pending 的是
    release() 刚写下的更新版本（带 attempts / uploaded_url / session_id），
    用 inflight 的旧副本盖回去会丢掉 uploaded_url，导致几个 GB 白传一遍。
    """
    src_dir, dst_dir = inflight_dir(work_dir), pending_dir(work_dir)
    if not src_dir.is_dir():
        return 0
    dst_dir.mkdir(parents=True, exist_ok=True)
    moved = 0
    for f in sorted(src_dir.glob("*.json")):
        dst = dst_dir / f.name
        try:
            if dst.exists():
                f.unlink(missing_ok=True)   # pending 更新，丢弃 inflight 旧副本
            else:
                os.replace(f, dst)
                moved += 1
        except OSError as e:
            logger.warning(f"[upload_queue] 搬回 pending 失败: {f.name}, {e}")
    if moved:
        logger.info(f"[upload_queue] 启动搬回 {moved} 条上次未完成的上传")
    return moved


# ==================== 观测 / 背压 ====================


def pending_bytes(work_dir: str | Path) -> int:
    """待传总字节数——父进程接新任务前的背压依据。

    算得比传得快时队列会无限堆积，而队列里的文件**不能删**（清理侧要跳过），
    磁盘会被撑爆。这是唯一会真正拖垮机器的失败模式，故背压必须有。
    读的是条目里记录的 size，不 stat 文件：便宜，且文件没了也不影响判断。
    """
    total = 0
    for e in iter_pending(work_dir) + _load_dir(inflight_dir(work_dir)):
        total += e.size
    return total


def referenced_paths(work_dir: str | Path) -> set:
    """队列当前引用的本地文件路径集合 —— 清理侧据此跳过，避免删掉还没传的产物。"""
    out = set()
    for e in iter_pending(work_dir) + _load_dir(inflight_dir(work_dir)):
        out.add(os.path.realpath(e.local_path))
    return out
