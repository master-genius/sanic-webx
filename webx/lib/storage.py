"""本地对象存储：路径守卫 / 流式落盘 / 原子落位。

零依赖（只用 stdlib），存储根由调用方注入——接线在 ``webx.services.storage_service``。

**本模块全部是无状态操作**：没有会话、没有需要定期清理的中间态。
大文件分片上传是独立可选功能，在 ``webx.lib.chunked_upload``，
不用它的项目完全不会碰到那套会话管理。

**对象键（key）是相对存储根的路径**，如 ``tasks/abc123/results/em.zip``。
键的每一段都要过安全校验，最终路径还要再做一次「解析后仍在根内」的守卫，
两道一起才挡得住 ``..`` 穿越与符号链接逃逸。

两条从生产事故里换来的设计，改动前请先读懂：

1. **流式写入，不缓冲整份文件**。上传走 ``open_writer()`` 逐块落盘，
   峰值内存 = 单个分块，几个并发大文件也不会撑爆内存。
2. **先写 ``.tmp`` 再 ``os.replace`` 原子落位**。否则下载方可能读到半截文件。
   同目录 rename 才是原子的，所以 tmp 必须和目标同目录。
"""

from __future__ import annotations

import logging
import os
import re
import secrets
import shutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# 键的单段安全字符集：字母数字 + 下划线 / 连字符 / 点（禁 '/'、'..'、空）
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9_.-]+$")

# 流式写入的中间态前缀：列表时跳过，避免把半截文件暴露出去
TMP_PREFIX = ".upload."


class StoreError(Exception):
    """存储层异常，携带建议 HTTP 状态码。"""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


def normalize_key(key: str) -> str:
    """校验并归一化对象键，返回 ``a/b/c.txt`` 形式。

    逐段校验：任何一段不合法（含 ``..``、空段、路径分隔符、其他特殊字符）即拒绝。
    这是第一道守卫，``ObjectStore.path_for`` 里还有第二道。
    """
    if not key or not isinstance(key, str):
        raise StoreError("对象键不能为空", status=400)

    segments = [s for s in key.replace("\\", "/").split("/") if s]
    if not segments:
        raise StoreError(f"非法对象键: {key!r}", status=400)

    for seg in segments:
        if seg in (".", "..") or not _SAFE_SEGMENT.match(seg):
            raise StoreError(f"非法对象键片段: {seg!r}", status=400)

    return "/".join(segments)


class ObjectStore:
    """本地对象存储。"""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def ensure_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        logger.info("存储目录已就绪: root=%s", self.root)

    # ==================== 路径 ====================

    def path_for(self, key: str) -> Path:
        """把对象键解析为绝对路径（不要求文件存在）。

        两道守卫：``normalize_key`` 逐段校验 + 解析后必须仍在存储根内
        （后者能挡住符号链接逃逸，前者挡不住）。
        """
        key = normalize_key(key)
        path = self.root / key
        try:
            path.resolve().relative_to(self.root.resolve())
        except ValueError:
            raise StoreError("非法的文件路径", status=403)
        return path

    def exists(self, key: str) -> Optional[Path]:
        """存在则返回路径，否则 None（键非法也返回 None，便于直接当 404 处理）。"""
        try:
            path = self.path_for(key)
        except StoreError:
            return None
        return path if path.is_file() else None

    def size_of(self, key: str) -> int:
        """文件字节数；不存在返回 0。"""
        path = self.exists(key)
        return path.stat().st_size if path else 0

    def list_prefix(self, prefix: str) -> list[str]:
        """列出某个前缀（目录）下的文件名。前缀非法或不存在返回空列表。"""
        try:
            directory = self.path_for(prefix)
        except StoreError:
            return []
        if not directory.is_dir():
            return []
        # 跳过流式写入的中间态，避免把半截文件暴露给调用方
        return sorted(
            f.name for f in directory.iterdir()
            if f.is_file() and not f.name.startswith(TMP_PREFIX)
        )

    def delete(self, key: str) -> bool:
        """删除对象，返回是否真的删掉了（不存在返回 False，不报错）。"""
        path = self.exists(key)
        if not path:
            return False
        try:
            path.unlink()
            logger.info("对象已删除: %s", key)
            return True
        except OSError as e:
            logger.warning("删除对象失败: %s, error=%s", key, e)
            return False

    def delete_prefix(self, prefix: str) -> bool:
        """递归删除整个前缀目录。"""
        try:
            directory = self.path_for(prefix)
        except StoreError:
            return False
        if not directory.is_dir():
            return False
        try:
            shutil.rmtree(directory)
            logger.info("目录已删除: %s", prefix)
            return True
        except OSError as e:
            logger.warning("删除目录失败: %s, error=%s", prefix, e)
            return False

    # ==================== 整传（流式）====================

    def open_writer(self, prefix: str) -> "ObjectWriter":
        """打开流式写入器：逐块直接落盘，峰值内存 = 单块而非整份文件。

        最终文件名延迟到 ``finalize()`` 再定——整传 multipart 的文件名要等
        解析到 file 部分的头才知道。
        """
        return ObjectWriter(self, prefix)

    def save_bytes(self, key: str, data: bytes) -> Path:
        """一次性写入（小文件用）。同样走 .tmp + 原子落位。"""
        key = normalize_key(key)
        prefix, _, filename = key.rpartition("/")
        writer = self.open_writer(prefix or ".")
        try:
            writer.write(data)
            return writer.finalize(filename)
        except Exception:
            writer.abort()
            raise


class ObjectWriter:
    """单个对象的流式写入器。

    先在目标目录下写 ``.upload.<rand>.tmp``，逐块 append；``finalize()`` 时按最终
    文件名 ``os.replace`` 原子落位。tmp 与目标同目录，rename 才是原子的。
    """

    def __init__(self, store: ObjectStore, prefix: str):
        self._store = store
        # 前缀此刻即校验（非法立抛），并确保目录存在
        self._prefix = "" if prefix in (".", "") else normalize_key(prefix)
        self._dir = (store.root / self._prefix) if self._prefix else store.root
        self._dir.mkdir(parents=True, exist_ok=True)

        self._tmp = self._dir / f"{TMP_PREFIX}{secrets.token_hex(8)}.tmp"
        self._fh = open(self._tmp, "wb")
        self._closed = False
        self.size = 0

    def write(self, data: bytes) -> None:
        self._fh.write(data)
        self.size += len(data)

    def finalize(self, filename: str) -> Path:
        """校验文件名并原子落位，返回最终路径。

        文件名先校验再落位——避免把 .tmp move 到非法路径。
        非法文件名抛 ``StoreError``，调用方负责 ``abort()``。
        """
        key = f"{self._prefix}/{filename}" if self._prefix else filename
        final_path = self._store.path_for(key)

        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._closed = True

        os.replace(self._tmp, final_path)  # 同目录 rename，原子
        logger.info("对象已保存(流式): %s (%d bytes)", final_path, self.size)
        return final_path

    def abort(self) -> None:
        """放弃写入并清理 .tmp（幂等）。解析失败 / 超限 / 非法文件名时调用。"""
        try:
            if not self._closed:
                self._fh.close()
                self._closed = True
        finally:
            if self._tmp.exists():
                try:
                    self._tmp.unlink()
                except OSError:
                    pass

    def __enter__(self) -> "ObjectWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """异常退出时自动清理 .tmp（正常退出由调用方 finalize）。"""
        if exc_type is not None:
            self.abort()
