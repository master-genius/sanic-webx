"""大文件分片上传（断点续传）—— 独立可选功能。

**不用就完全不会碰到这里的东西**：核心存储层 ``webx.lib.storage`` 全是无状态操作，
会话目录、会话元数据、超龄清理这套只存在于本模块。

与「下载断点续传」不是一回事：下载靠 HTTP ``Range``，无状态、纯标准头解析
（见 ``webx.lib.ranges``）；上传要跨多次请求累积字节，只能在服务端存会话。

协议采用 tus 的核心语义（够用即可，没实现完整 tus）：

    POST   创建会话            → {upload_id, offset: 0}
    GET    查询当前 offset      → 断线重连后先问这里，再从该处续发
    PATCH  带 Upload-Offset 追加 → offset 必须等于服务端已收字节数
    POST   完成                 → 校验总长后原子落位到目标键

磁盘布局（会话目录与业务对象隔离，位于存储根下）::

    {root}/.uploads/{upload_id}/part        已收字节，直接 append
    {root}/.uploads/{upload_id}/meta.json   目标键 / 声明总长 / 创建时刻

两条容易踩的地方：

1. **追加必须在 flock 内复核 offset**。"先读大小再写"存在 TOCTOU，
   多 worker 并发 PATCH 同一会话会串写错位。
2. **会话必须能被回收**。init 之后从不 complete 的会话会永久残留，
   所以判龄逻辑遇到损坏的 meta 也要能回落 mtime——否则一个坏文件就让该目录永远清不掉，
   正好是这套清理机制本该防住的泄漏。
"""

from __future__ import annotations

import errno
import fcntl
import json
import logging
import os
import re
import secrets
import shutil
import time
from pathlib import Path
from typing import Optional

from webx.lib.storage import ObjectStore, StoreError

logger = logging.getLogger(__name__)

# 会话目录名（位于存储根下，与业务对象隔离）
SESSIONS_DIRNAME = ".uploads"

_SAFE_UPLOAD_ID = re.compile(r"^[A-Za-z0-9_-]+$")


class OffsetConflict(StoreError):
    """分片 offset 与服务端已收字节不一致（tus 语义 409）。

    携带真实 offset，客户端据此重新对齐后继续发送——这是断点续传能自愈的关键。
    """

    def __init__(self, current_offset: int):
        super().__init__(f"offset 冲突，服务端当前 offset={current_offset}", status=409)
        self.current_offset = current_offset


class ChunkedUpload:
    """分片上传会话管理器。

    落位依赖 ``ObjectStore``（复用它的键校验与路径守卫），但存储层反过来不知道
    本模块的存在——依赖是单向的。
    """

    def __init__(self, store: ObjectStore, sessions_dirname: str = SESSIONS_DIRNAME):
        self.store = store
        self.sessions_dir = store.root / sessions_dirname

    def ensure_dirs(self) -> None:
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        logger.info("分片上传会话目录已就绪: %s", self.sessions_dir)

    def _session_dir(self, upload_id: str) -> Path:
        if not upload_id or not _SAFE_UPLOAD_ID.match(upload_id):
            raise StoreError(f"非法 upload_id: {upload_id!r}", status=400)
        return self.sessions_dir / upload_id

    # ==================== 会话生命周期 ====================

    def create(self, key: str, total: int = -1) -> dict:
        """创建会话，返回含 ``upload_id`` 与 ``offset=0`` 的 meta。

        Args:
            key: 目标对象键。**此刻即校验**——非法输入不该等到 complete 才暴露。
            total: 声明的总字节数；``-1`` 表示不声明，complete 时不校验长度。
        """
        key = self.store.normalize(key)
        upload_id = secrets.token_hex(16)
        sdir = self.sessions_dir / upload_id
        sdir.mkdir(parents=True, exist_ok=True)
        # 预建空 part，让 offset 查询从 0 起始一致
        (sdir / "part").touch()

        meta = {
            "upload_id": upload_id,
            "key": key,
            "total": int(total),
            "created_ms": int(time.time() * 1000),
        }
        (sdir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
        logger.info("分片会话创建: upload_id=%s key=%s total=%s", upload_id, key, total)
        return {**meta, "offset": 0}

    def get(self, upload_id: str) -> Optional[dict]:
        """读取会话 meta + 当前 offset；不存在返回 None。"""
        sdir = self._session_dir(upload_id)
        meta_file, part_file = sdir / "meta.json", sdir / "part"
        if not meta_file.exists() or not part_file.exists():
            return None
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        meta["offset"] = part_file.stat().st_size
        return meta

    def append(self, upload_id: str, offset: int, data: bytes) -> int:
        """在 offset 处追加分片，返回追加后的新 offset。

        tus 语义：offset 必须等于服务端当前已收字节数。
          - 相等   → 追加（正常推进）
          - 不相等 → ``OffsetConflict``(409)，客户端据其 ``current_offset`` 重新对齐

        以 ``ab`` 打开天然写到末尾；加排他 flock 后**在锁内**复核 offset，
        关掉「先读大小再写」的 TOCTOU。正常客户端顺序发片本不并发，
        此锁是多进程部署下的正确性兜底。
        """
        part_file = self._session_dir(upload_id) / "part"
        if not part_file.exists() or not (part_file.parent / "meta.json").exists():
            raise StoreError("分片会话不存在或已过期", status=404)

        try:
            f = open(part_file, "ab")
        except FileNotFoundError:
            raise StoreError("分片会话不存在或已过期", status=404) from None
        with f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                # 拿到锁后复核：打开文件到拿到锁之间，会话可能已被 complete()（part 被 rename 成正式对象）
                # 或被清理。此时手里的 fd 指向的已不是会话的 part——继续写会把数据追加进已发布的对象
                try:
                    if os.fstat(f.fileno()).st_ino != os.stat(part_file).st_ino:
                        raise FileNotFoundError
                except FileNotFoundError:
                    raise StoreError("分片会话不存在或已过期", status=404) from None
                current = os.fstat(f.fileno()).st_size
                if offset != current:
                    raise OffsetConflict(current)
                try:
                    f.write(data)
                    f.flush()
                except OSError as e:
                    if e.errno in (errno.ENOSPC, errno.EDQUOT):
                        raise StoreError("存储空间不足", status=507) from e
                    raise
                return f.tell()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def complete(self, upload_id: str) -> tuple[Path, int, str]:
        """完成上传：校验字节数后原子落位到目标键。

        Returns:
            ``(最终路径, 字节数, 对象键)``
        """
        session = self.get(upload_id)
        if session is None:
            raise StoreError("分片会话不存在或已过期", status=404)
        total = session["total"]
        final_path = self.store.path_for(session["key"])
        part_file = self._session_dir(upload_id) / "part"

        # 与 append() 取同一把排他锁，并在锁内重新量大小：否则正在途中的 PATCH 会在落位之后
        # 把数据追加进已经发布的对象（complete 回报 5 字节，文件实际 25 字节）
        try:
            lock_fh = open(part_file, "rb")
        except FileNotFoundError:
            raise StoreError("分片会话不存在或已过期", status=404) from None
        with lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            offset = os.fstat(lock_fh.fileno()).st_size
            if total >= 0 and offset != total:
                raise StoreError(
                    f"字节数不完整：已收 {offset}，声明 total={total}", status=400
                )
            try:
                final_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(part_file, final_path)  # 原子落位
            except (FileExistsError, IsADirectoryError, NotADirectoryError):
                raise StoreError("对象键与已有路径冲突（父段是文件，或目标是目录）", status=409) from None
            except FileNotFoundError:
                raise StoreError("分片会话不存在或已过期", status=404) from None
        self.abort(upload_id)  # 清理会话目录（part 已 move，仅剩 meta.json）

        logger.info("分片上传完成: upload_id=%s -> %s (%d bytes)", upload_id, final_path, offset)
        return final_path, offset, session["key"]

    def abort(self, upload_id: str) -> None:
        """删除会话目录（幂等：不存在也不报错）。"""
        sdir = self._session_dir(upload_id)
        if not sdir.exists():
            return
        try:
            shutil.rmtree(sdir)
            logger.info("分片会话已清理: upload_id=%s", upload_id)
        except OSError as e:
            logger.warning("清理分片会话失败: upload_id=%s, error=%s", upload_id, e)

    # ==================== 回收 ====================

    def cleanup_stale(self, max_age_hours: float) -> int:
        """清理超龄未完成的会话，返回清理数量。

        init 之后从不 complete 的会话会永久残留，不清就是磁盘泄漏。
        多 worker 各自启动时调用即可——rmtree 幂等，并发 race 由 try/except 兜住。
        """
        if not self.sessions_dir.exists():
            return 0

        now_ms = int(time.time() * 1000)
        cutoff_ms = max_age_hours * 3600 * 1000
        removed = 0

        for sdir in self.sessions_dir.iterdir():
            if not sdir.is_dir():
                continue
            try:
                if now_ms - self._last_active_ms(sdir) < cutoff_ms:
                    continue
                shutil.rmtree(sdir)
                removed += 1
            except OSError as e:
                logger.warning("清理超龄分片会话失败: %s, error=%s", sdir.name, e)

        if removed:
            logger.info("清理超龄分片会话 %d 个（>%sh）", removed, max_age_hours)
        return removed

    def _last_active_ms(self, sdir: Path) -> int:
        """会话最近一次活动的时刻：创建时间与 part 文件最后写入时间取较晚者。

        只看创建时间会把“仍在慢速上传的大文件”当成陈旧会话：每个 worker 启动时都会跑一次回收，
        重启或扩容一台就把别人正在传的会话整个删掉，客户端续传得到 404。
        """
        created = self._created_ms(sdir)
        try:
            return max(created, int((sdir / "part").stat().st_mtime * 1000))
        except OSError:
            return created

    @staticmethod
    def _created_ms(sdir: Path) -> int:
        """判定会话目录的创建时刻。

        优先读 meta.json 的 created_ms；**meta 缺失或损坏时一律回落目录 mtime**。
        解析异常绝不能冒出去：否则一个坏掉的 meta 就让该目录永远清不掉，
        正好是这套清理机制本该防住的泄漏。
        """
        meta_file = sdir / "meta.json"
        if meta_file.exists():
            try:
                created = json.loads(meta_file.read_text(encoding="utf-8")).get("created_ms")
                if isinstance(created, (int, float)) and created > 0:
                    return int(created)
            except (OSError, ValueError, json.JSONDecodeError):
                logger.warning("会话 meta 损坏，按目录 mtime 判龄: %s", sdir.name)
        return int(sdir.stat().st_mtime * 1000)
