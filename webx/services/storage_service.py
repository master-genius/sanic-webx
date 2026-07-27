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

from webx.config import settings
from webx.lib.ranges import UNSATISFIABLE, ByteRange, parse_byte_range
from webx.lib.chunked_upload import ChunkedUpload
from webx.lib.signer import UrlSigner
from webx.lib.storage import ObjectStore

logger = logging.getLogger(__name__)

# 下载分块大小：太小则系统调用频繁，太大则首字节延迟上升。1MB 是经验上的平衡点。
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


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
            self._store = ObjectStore(settings.storage_path)
        return self._store

    @property
    def signer(self) -> UrlSigner:
        if self._signer is None:
            # 签名密钥独立于用户令牌；未单独配置时回落 TOKEN_SECRET 并告警——
            # 能跑，但两套密钥绑在一起，泄漏面变大。
            secret = settings.STORAGE_SIGN_SECRET
            if not secret:
                secret = settings.TOKEN_SECRET
                logger.warning(
                    "STORAGE_SIGN_SECRET 未配置，回落 TOKEN_SECRET。"
                    "生产环境请单独配置，以便独立轮换。"
                )
            self._signer = UrlSigner(secret, settings.STORAGE_SIGN_TTL_SECONDS)
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
        return self.signer.verify(sign, object_key)

    # ==================== 下载 ====================

    async def send(
        self,
        path: Path,
        *,
        download_name: Optional[str] = None,
        range_header: Optional[str] = None,
        inline: bool = False,
        content_type: Optional[str] = None,
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

        headers = {
            # RFC 5987 编码，兼容中文与特殊字符文件名
            "Content-Disposition": (
                "inline" if inline else f"attachment; filename*=UTF-8''{quote(name)}"
            ),
            # 声明支持断点续传，浏览器/下载器据此分段取
            "Accept-Ranges": "bytes",
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
