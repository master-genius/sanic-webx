"""限时签名串 —— 给下载链接加"有效期 + 防挪用"。

复用 ``WebxToken``（AES-256-GCM），所以：

  - 签名对外**完全不透明**（是密文，不是 base64 明文），看不出对象键与到期时间
  - 有效期由 WebxToken 内建的时间戳校验兜住，不用自己拼过期逻辑
  - **零新依赖**：底层走 ``_aes_backend``（libcrypto 优先 / 纯 Python 兜底）

两条安全要点：

  1. **签名与对象键绑定**。校验时解出的键必须等于当前请求的键，
     否则一个签名能被挪用到任意文件。
  2. **密钥独立于用户令牌**（``STORAGE_SIGN_SECRET``），可单独轮换。
     用同一把密钥意味着签名泄漏 = 用户令牌体系一起沦陷。
"""

from __future__ import annotations

import logging
from typing import Optional

from webx.lib.token import WebxToken

logger = logging.getLogger(__name__)


class UrlSigner:
    """对象级限时签名器。"""

    def __init__(self, secret: str, default_ttl_seconds: int):
        # 单实例复用：WebxToken 没有每次调用的可变全局状态（make_token 用随机 IV），
        # 协程安全。
        self._default_ttl_ms = max(1, default_ttl_seconds) * 1000
        self._token = WebxToken({
            "key": secret,
            "expires": self._default_ttl_ms,
            "algorithm": "aes-256-gcm",
        })

    def issue(self, object_key: str, ttl_seconds: Optional[int] = None) -> str:
        """签发某对象键的限时签名串。

        Args:
            object_key: 规范对象键，如 ``tasks/abc123/results/em.zip``。
            ttl_seconds: 有效期秒数；None 用默认值。

        Returns:
            形如 ``<riv>.<b64url>`` 的不透明串，URL 安全，可直接作 ``?sign=`` 的值。
        """
        ttl_ms = (max(1, ttl_seconds) * 1000) if ttl_seconds else self._default_ttl_ms
        return self._token.make_token({"p": object_key, "expires": ttl_ms})

    def verify(self, sign: str, object_key: str) -> bool:
        """校验签名对该对象键是否有效且未过期。"""
        if not sign:
            return False
        try:
            result = self._token.verify_token(sign)
        except Exception as e:
            # 解密/解析异常一律视为无效，绝不放行
            logger.debug("签名校验异常: %s", e)
            return False

        if not result.get("ok"):
            return False
        # 对象键必须精确匹配，防止签名被挪用到其他文件
        return (result.get("data") or {}).get("p") == object_key
