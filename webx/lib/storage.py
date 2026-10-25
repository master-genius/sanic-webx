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

class KeyRule:
    """对象键的单段命名规则。

    所有规则共有的底线（不可配）：不得为空、不得是 ``.`` / ``..``、
    **不得以 ``.`` 开头**（点开头的名字是存储自己的保留区，如 ``.uploads/``、
    ``.staging/``，也避免把隐藏文件当业务对象暴露），不得含 NUL / 控制字符。
    可配的只有字符集与单段长度。
    """

    def __init__(
        self, name: str, pattern: re.Pattern[str], max_bytes: int = 255, max_depth: int = 16
    ):
        self.name = name
        self.pattern = pattern
        self.max_bytes = max_bytes
        #: 键最多几段。不设上限的话，一个请求就能让服务端建出任意深的目录树
        self.max_depth = max_depth

    def check(self, seg: str) -> bool:
        if seg in (".", "..") or seg.startswith("."):
            return False
        if len(seg.encode("utf-8", errors="ignore")) > self.max_bytes:
            return False
        return bool(self.pattern.match(seg))

    def __repr__(self) -> str:
        return f"KeyRule({self.name})"


# ⚠️ 用 \A…\Z 而不是 ^…$：Python 的 $ 会匹配“字符串末尾换行之前”，"a\n" 会被放行，
# 造出带换行的不可见同名文件并污染日志。
#: 严格：字母数字 + ``_`` ``-`` ``.``（默认，适合机器生成的键）
STRICT = KeyRule("strict", re.compile(r"\A[A-Za-z0-9_.-]+\Z"))
#: 宽松：任意可打印字符（含中文、空格、括号），仍排除路径分隔符与控制字符。
#: 适合需要保留用户原始文件名的场景
LENIENT = KeyRule("lenient", re.compile(r"\A[^\x00-\x1f\x7f/\\]+\Z"))

KEY_RULES = {"strict": STRICT, "lenient": LENIENT}


def get_key_rule(name: str | KeyRule | None) -> KeyRule:
    """按名字取规则；未知名字抛 ValueError（配置写错应当启动即暴露）。"""
    if isinstance(name, KeyRule):
        return name
    key = (name or "strict").strip().lower()
    if key not in KEY_RULES:
        raise ValueError(f"未知的对象键规则 {name!r}，可选: {', '.join(KEY_RULES)}")
    return KEY_RULES[key]


# 流式写入的中间态前缀：列表时跳过，避免把半截文件暴露出去
TMP_PREFIX = ".upload."


class StoreError(Exception):
    """存储层异常，携带建议 HTTP 状态码。"""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


def normalize_key(key: str, rule: KeyRule | None = None) -> str:
    """校验并归一化对象键，返回 ``a/b/c.txt`` 形式。

    逐段校验：任何一段不合法（含 ``..``、空段、路径分隔符、点开头、其他特殊字符）
    即拒绝。这是第一道守卫，``ObjectStore.path_for`` 里还有第二道。
    """
    rule = rule or STRICT
    if not key or not isinstance(key, str):
        raise StoreError("对象键不能为空", status=400)

    segments = [s for s in key.replace("\\", "/").split("/") if s]
    if not segments:
        raise StoreError(f"非法对象键: {key!r}", status=400)

    if len(segments) > rule.max_depth:
        raise StoreError(f"对象键层级过深（最多 {rule.max_depth} 段）", status=400)

    for seg in segments:
        if not rule.check(seg):
            raise StoreError(f"非法对象键片段: {seg!r}", status=400)

    return "/".join(segments)


class ObjectStore:
    """本地对象存储。"""

    def __init__(self, root: str | Path, key_rule: str | KeyRule | None = None):
        self.root = Path(root)
        self.key_rule = get_key_rule(key_rule)

    def normalize(self, key: str) -> str:
        """按本存储的键规则归一化对象键。"""
        return normalize_key(key, self.key_rule)

    def ensure_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        logger.info("存储目录已就绪: root=%s", self.root)

    # ==================== 路径 ====================

    def path_for(self, key: str) -> Path:
        """把对象键解析为绝对路径（不要求文件存在）。

        两道守卫：``normalize_key`` 逐段校验 + 解析后必须仍在存储根内
        （后者能挡住符号链接逃逸，前者挡不住）。
        """
        key = self.normalize(key)
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
        except OSError as e:
            logger.warning("删除对象失败: %s, error=%s", key, e)
            return False
        self._prune_empty_parents(path.parent)
        return True

    def _prune_empty_parents(self, directory: Path) -> None:
        """向上回收因删除而变空的目录，止于存储根（根本身永不删）。

        多级键下不回收会留下一整串空目录；rmdir 只删空目录，
        并发写入落进该目录时它自然失败，无需加锁。
        """
        root = self.root.resolve()
        current = directory.resolve()
        while current != root and root in current.parents:
            try:
                current.rmdir()
            except OSError:
                return  # 非空或被并发使用，到此为止
            current = current.parent

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
        key = self.normalize(key)
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
        self._prefix = "" if prefix in (".", "") else store.normalize(prefix)
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
        # filename 是**单个文件名**，不是键：带路径分隔符的会被当成子目录，而 tmp 所在目录里并没有它——
        # 落位时 FileNotFoundError，接口 500。在这里按 400 拒绝
        if not filename or "/" in filename or "\\" in filename:
            raise StoreError(f"非法文件名: {filename!r}", status=400)
        key = f"{self._prefix}/{filename}" if self._prefix else filename
        final_path = self._store.path_for(key)

        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._closed = True

        self._place(final_path)
        logger.info("对象已保存(流式): %s (%d bytes)", final_path, self.size)
        return final_path

    def _place(self, final_path: Path) -> None:
        """tmp → 最终路径的原子落位，并把文件系统层面的失败翻成 ``StoreError``。

        - 键与现有路径冲突（父段是文件 / 目标是目录）是**客户端的键选错了**：409，不是 500。
        - 目标目录刚被并发的 DELETE 修剪掉（``_prune_empty_parents`` 会 rmdir 空目录）：重建后重试一次。
          窗口是 mkdir 到 replace 之间（含大文件的 fsync 时间），不重试上传就白传了。
        失败时 tmp 仍在，由调用方 ``abort()`` 清理（所有调用点都这样做）。
        """
        for attempt in (1, 2):
            try:
                final_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(self._tmp, final_path)
                return
            except FileNotFoundError:
                if attempt == 2:
                    raise StoreError("目标目录被并发删除，请重试", status=409) from None
            except (FileExistsError, IsADirectoryError, NotADirectoryError):
                raise StoreError("对象键与已有路径冲突（父段是文件，或目标是目录）", status=409) from None
            except OSError as e:
                logger.error("对象落位失败: %s -> %s: %s", self._tmp, final_path, e)
                raise StoreError("存储写入失败", status=500) from None

    def finalize_to(self, key: str) -> Path:
        """原子落位到**任意**对象键（目标目录不必是写入器创建时的前缀目录）。

        用于键要等整个请求解析完才知道的场景（如表单字段 ``key`` 排在文件之后）：
        先用根前缀开写入器，解析完再落到真正的键。tmp 与目标同属一个存储根（同一文件系统），
        ``os.replace`` 仍然原子。键非法抛 ``StoreError``，调用方负责 ``abort()``。
        """
        final_path = self._store.path_for(key)

        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._closed = True

        self._place(final_path)
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
