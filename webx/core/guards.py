"""路由级守卫装饰器。

与中间件的分工：
  - **中间件**（``webx.middleware``）按目录批量生效，适合"整组接口都要登录"
  - **装饰器**（本模块）按单个路由生效，适合同一目录下混合公开/受限接口
    （典型如 auth 目录：login/register 公开，logout/refresh 需登录）

两者共用同一套令牌校验实现，语义完全一致。
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import Callable, Iterable, Optional

from sanic.request import Request
from sanic.response import json

from webx.config import settings
from webx.i18n import get_lang_from_header, t
from webx.lib.client_ip import get_client_ip
from webx.lib.token import get_token
from webx.middleware.auth import DEFAULT_CLAIMS

__all__ = [
    "require_internal_token",
    "require_token",
    "optional_token",
    "get_client_ip",
    "with_client_ip",
]

logger = logging.getLogger(__name__)


# ==================== 内部服务鉴权 ====================


def require_internal_token(f: Callable) -> Callable:
    """内部回调鉴权：校验 ``X-Internal-Token`` 头。

    ``INTERNAL_TOKEN`` 未配置（空串）时**一律拒绝**——空配置绝不能等价于放行，
    否则一次漏配就把内部接口彻底敞开。
    """

    @wraps(f)
    async def wrapper(request: Request, *args, **kwargs):
        lang = get_lang_from_header(request.headers)
        expected = settings.INTERNAL_TOKEN
        token = request.headers.get("X-Internal-Token")

        if not expected or not token or token != expected:
            return json({"error": t("unauthorized", lang)}, status=401)
        return await f(request, *args, **kwargs)

    return wrapper


# ==================== 令牌鉴权 ====================


def _extract_payload(request: Request) -> tuple[Optional[dict], Optional[str]]:
    """返回 ``(payload, errcode)``：payload 为 None 时 errcode 说明原因。"""
    auth_header = request.headers.get("Authorization") or ""
    if not auth_header.startswith("Bearer "):
        return None, "MISSING"

    result = get_token().verify_token(auth_header[7:])
    if not result.get("ok"):
        return None, result.get("errcode", "FAILED")
    return result.get("data") or {}, None


def require_token(
    f: Callable = None,
    *,
    claims: Iterable[str] = DEFAULT_CLAIMS,
) -> Callable:
    """要求有效令牌，注入 ``request.ctx.user``；无效则 401。

    可直接用 ``@require_token``，也可带参 ``@require_token(claims=[...])``。
    """

    def decorator(func: Callable) -> Callable:
        claim_keys = tuple(claims)

        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            payload, errcode = _extract_payload(request)
            if payload is None:
                lang = get_lang_from_header(request.headers)
                key = "token_expired" if errcode == "TIMEOUT" else "unauthorized"
                return json({"error": t(key, lang), "code": errcode}, status=401)

            request.ctx.token_payload = payload
            request.ctx.user = {k: payload.get(k) for k in claim_keys}
            return await func(request, *args, **kwargs)

        return wrapper

    return decorator(f) if callable(f) else decorator


def optional_token(
    f: Callable = None,
    *,
    claims: Iterable[str] = DEFAULT_CLAIMS,
) -> Callable:
    """令牌可选：有效则注入 ``ctx.user``，无效/缺失则 ``ctx.user = None``，一律放行。

    用于"登录与否都能访问、但登录后返回更多内容"的接口。
    """

    def decorator(func: Callable) -> Callable:
        claim_keys = tuple(claims)

        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            payload, _ = _extract_payload(request)
            request.ctx.token_payload = payload
            request.ctx.user = (
                {k: payload.get(k) for k in claim_keys} if payload is not None else None
            )
            return await func(request, *args, **kwargs)

        return wrapper

    return decorator(f) if callable(f) else decorator


# ==================== 客户端真实 IP（反代感知） ====================

# 解析逻辑本体在 ``webx.lib.client_ip``（中间件与本模块共用，见那里的说明），
# 此处仅原样再导出，老代码 ``from webx.core.guards import get_client_ip`` 不受影响。


def with_client_ip(f: Callable) -> Callable:
    """把真实客户端 IP 注入 ``request.ctx.client_ip`` 的路由级装饰器。

    需要全局生效时改用 ``webx.middleware.client_ip`` 中间件。
    """

    @wraps(f)
    async def wrapper(request: Request, *args, **kwargs):
        request.ctx.client_ip = get_client_ip(request)
        return await f(request, *args, **kwargs)

    return wrapper
