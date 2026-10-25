"""存储服务：接线配置 + 构造下载响应。

``webx.lib.storage`` / ``webx.lib.signer`` 是零配置的纯实现，这一层负责把
``settings`` 接上去，并提供控制器直接可用的下载响应构造。

**下载必须走 ``file_stream``，不要用 ``file()``**——这是原项目上线后才发现的坑：
``file()`` 会把整份文件读进内存再发，几十 MB 的产物就能让事件循环卡住、
并发下载直接把内存打满。``file_stream`` 是边读边发，首字节延迟低、内存恒定。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from sanic.response import file_stream, text

from webx.config import DEFAULT_TOKEN_SECRET, settings
from webx.lib.ranges import UNSATISFIABLE, ByteRange, parse_byte_range
from webx.lib.chunked_upload import ChunkedUpload
from webx.lib.signer import UrlSigner
from webx.lib.storage import ObjectStore, StoreError

logger = logging.getLogger(__name__)

# 下载分块大小：太小则系统调用频繁，太大则首字节延迟上升。1MB 是经验上的平衡点。
DOWNLOAD_CHUNK_SIZE = 1024 * 1024

#: 允许 ``inline`` 展示的 MIME：纯文本与常见位图 / PDF。刻意不含 text/html、image/svg+xml、
#: application/xhtml+xml 等能携带脚本的类型。
SAFE_INLINE_TYPES = frozenset({
    "text/plain", "image/png", "image/jpeg", "image/gif", "image/webp", "application/pdf",
})


class SignerUnavailable(StoreError):
    """签名密钥未配置或不可用。签发回 503；校验一律判无效。"""

    def __init__(self, message: str):
        super().__init__(message, status=503)


class StorageService:
    """对象存储 + 签名 + 下载响应的统一门面。

    延迟初始化：配置由业务在 import 期之后才注册（``configure_settings``），
    所以这里不能在模块 import 时就读 settings。
    """

    def __init__(self) -> None:
        self._store: Optional[ObjectStore] = None
        self._signer: Optional[UrlSigner] = None
        self._chunked: Optional[ChunkedUpload] = None

    @property
    def store(self) -> ObjectStore:
        if self._store is None:
            self._store = ObjectStore(settings.storage_path, settings.STORAGE_KEY_RULE)
        return self._store

    @property
    def signer(self) -> UrlSigner:
        if self._signer is None:
            # 签名密钥独立于用户令牌；未单独配置时回落 TOKEN_SECRET 并告警——
            # 能跑，但两套密钥绑在一起，泄漏面变大。
            secret = settings.STORAGE_SIGN_SECRET
            if not secret:
                secret = settings.TOKEN_SECRET
                if not secret or secret == DEFAULT_TOKEN_SECRET:
                    # fail-closed：回落到空值或源码里公开的默认值，等于任何人都能自己签出有效签名。
                    # 与内部令牌未配置时“拒绝一切”同一原则，宁可不能用，也不能形同虚设
                    raise SignerUnavailable(
                        "未配置签名密钥：请设置 STORAGE_SIGN_SECRET（或把 TOKEN_SECRET 改成非默认值）"
                    )
                logger.warning(
                    "STORAGE_SIGN_SECRET 未配置，回落 TOKEN_SECRET。"
                    "生产环境请单独配置，以便独立轮换。"
                )
            try:
                self._signer = UrlSigner(secret, settings.STORAGE_SIGN_TTL_SECONDS)
            except ValueError as e:  # 密钥过短等：启动后第一次用就明确报错，而不是每个请求都 500
                raise SignerUnavailable(f"签名密钥不可用: {e}") from None
        return self._signer

    @property
    def chunked(self) -> ChunkedUpload:
        """分片上传管理器（可选功能，见 webx.lib.chunked_upload）。"""
        if self._chunked is None:
            self._chunked = ChunkedUpload(self.store)
        return self._chunked

    def ensure_dirs(self) -> None:
        """建好存储目录（挂在应用启动钩子里）。"""
        self.store.ensure_dirs()
        self.chunked.ensure_dirs()

    def cleanup_stale_uploads(self) -> int:
        """清理超龄的分片上传会话（挂在应用启动钩子里，防磁盘泄漏）。"""
        return self.chunked.cleanup_stale(settings.UPLOAD_SESSION_MAX_AGE_HOURS)

    # ==================== 签名 ====================

    def sign(self, object_key: str, ttl_seconds: Optional[int] = None) -> str:
        return self.signer.issue(object_key, ttl_seconds)

    def verify_sign(self, sign: str, object_key: str) -> bool:
        try:
            signer = self.signer
        except SignerUnavailable as e:
            logger.error("拒绝签名校验: %s", e)   # 密钥不可用 = 一律不放行（调用方据此回 404）
            return False
        return signer.verify(sign, object_key)

    # ==================== 下载 ====================

    @staticmethod
    def head(path: Path, *, content_type: Optional[str] = None):
        """HEAD 响应：只回头，不读文件体。

        客户端据此在下载前拿到长度、确认支持 Range、做存在性探测。
        """
        import mimetypes

        size = path.stat().st_size
        mime = content_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        return text(
            "",
            status=200,
            content_type=mime,
            headers={"Content-Length": str(size), "Accept-Ranges": "bytes"},
        )

    async def send(
        self,
        path: Path,
        *,
        download_name: Optional[str] = None,
        range_header: Optional[str] = None,
        inline: bool = False,
        content_type: Optional[str] = None,
        extra_headers: Optional[dict] = None,
    ):
        """构造下载响应：流式 + 断点续传。

        Args:
            path: 已确认存在的文件路径。
            download_name: 呈现给用户的文件名；None 用磁盘上的真实文件名。
                存储文件名通常很简洁（``em.zip``），友好名在这里给。
            range_header: 请求的 ``Range`` 头原值。
            inline: True 则浏览器内联展示（如日志文本），False 触发下载。
            content_type: 覆盖 MIME；None 让 Sanic 按扩展名猜。

        Returns:
            Sanic 响应：全量 200 / 局部 206 / 区间越界 416。
        """
        file_size = path.stat().st_size
        name = download_name or path.name

        # 内联展示只对“浏览器不会当脚本执行”的类型放行。上传者可控内容，把 .html/.svg 内联在存储域名下
        # 就是同源脚本执行（存储域名常与其它应用同源）；其余类型降级为附件下载
        if inline:
            import mimetypes

            mime = (content_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream").split(";")[0]
            if mime not in SAFE_INLINE_TYPES:
                inline = False

        headers = {
            # RFC 5987 编码，兼容中文与特殊字符文件名
            "Content-Disposition": (
                "inline" if inline else f"attachment; filename*=UTF-8''{quote(name)}"
            ),
            # 声明支持断点续传，浏览器/下载器据此分段取
            "Accept-Ranges": "bytes",
            # 禁止浏览器按内容嗅探成别的类型；内联时再加沙箱，即使类型判断有漏也不能执行脚本
            "X-Content-Type-Options": "nosniff",
            **({"Content-Security-Policy": "sandbox"} if inline else {}),
            **(extra_headers or {}),
        }

        parsed = parse_byte_range(range_header, file_size)

        if parsed == UNSATISFIABLE:
            # 按规范回传 Content-Range: bytes */total，客户端据此知道真实长度
            return text(
                "Requested Range Not Satisfiable",
                status=416,
                headers={"Content-Range": f"bytes */{file_size}"},
            )

        if isinstance(parsed, ByteRange):
            # 局部传输：file_stream 会自动写 Content-Range 并置 206
            return await file_stream(
                str(path),
                chunk_size=DOWNLOAD_CHUNK_SIZE,
                mime_type=content_type,
                headers=headers,
                _range=parsed,
            )

        # 全量流式下载（无 Range，或语法非法降级至此）
        headers["Content-Length"] = str(file_size)
        return await file_stream(
            str(path),
            chunk_size=DOWNLOAD_CHUNK_SIZE,
            mime_type=content_type,
            headers=headers,
        )


storage_service = StorageService()
