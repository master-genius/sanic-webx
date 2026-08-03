"""阶段 1：认证中间件（只验令牌、注入 ``ctx.user``，不查库）。

令牌本身已携带身份，这一层刻意不碰数据库——查库属于阶段 2
（``webx.middleware.perm.role_perm_middleware``）的职责。
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable

from sanic import Request, response

from webx.i18n import get_lang_from_header, t
from webx.lib.token import get_token

# 从 token payload 透传到 request.ctx.user 的字段。
# ⚠️ 这里的 ``sys_root`` 是**系统级**超管（Admin.is_root），与
# ``role_perm_middleware`` 注入的 ``ctx.is_root``（**组织内**超管，UserOrg.is_root）
# 是两个概念——刻意用不同名字，避免同一个 key 因中间件链不同而含义漂移。
DEFAULT_CLAIMS = ("user_id", "username", "role", "org_id", "sys_root")


def _unauthorized(request: Request, key: str, **extra) -> response.HTTPResponse:
    lang = get_lang_from_header(request.headers)
    return response.json({"error": t(key, lang), **extra}, status=401)


def make_auth_middleware(
    *,
    claims: Iterable[str] = DEFAULT_CLAIMS,
    required_roles: Iterable[str] | None = None,
    forbidden_status: int = 403,
    name: str = "auth_middleware",
) -> Callable[[Request], Awaitable[Any]]:
    """构造一条认证中间件。

    行为：取 Bearer token → 解密校验 → 把 payload 里的 claims 注入
    ``request.ctx.user``。不查库（令牌自带身份，查库交给下游按需做）。

    Args:
        claims: 透传到 ctx.user 的 payload 字段名。
        required_roles: 非空时校验 payload 的 ``role``（大小写/空白不敏感）。
        forbidden_status: 角色不匹配时的状态码。传 ``404`` 可**隐藏接口存在性**
            （403 会泄露"有个你没权限的接口"，内部接口建议用 404）。
        name: 赋给返回函数的 ``__name__``。工厂产物默认都叫 ``middleware``，
            排查中间件链时一片同名毫无信息量——给它一个能认出来的名字。

    Returns:
        Sanic request 中间件。返回 response 即短路，handler 不会执行。
    """
    claim_keys = tuple(claims)
    roles = {r.strip().lower() for r in required_roles} if required_roles else None

    async def middleware(request: Request):
        auth_header = request.headers.get("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            return _unauthorized(request, "unauthorized")

        result = get_token().verify_token(auth_header[7:])
        if not result.get("ok"):
            errcode = result.get("errcode", "FAILED")
            key = "token_expired" if errcode == "TIMEOUT" else "token_invalid"
            return _unauthorized(request, key, code=errcode)

        payload = result.get("data") or {}

        if roles is not None:
            role = str(payload.get("role") or "").strip().lower()
            if role not in roles:
                lang = get_lang_from_header(request.headers)
                key = "not_found" if forbidden_status == 404 else "forbidden"
                return response.json({"error": t(key, lang)}, status=forbidden_status)

        request.ctx.token_payload = payload
        request.ctx.user = {k: payload.get(k) for k in claim_keys}

    middleware.__name__ = name
    return middleware


def make_role_middleware(
    *allowed_roles: str,
    status: int = 403,
    name: str | None = None,
) -> Callable[[Request], Awaitable[Any]]:
    """角色守卫：只放行指定角色，否则按 ``status`` 短路。

    **必须挂在认证中间件之后**——它只读 ``request.ctx.user["role"]``，不自己验令牌
    （令牌无效在上一道即已 401）。

    ``status=404`` 是内部接口的推荐用法：对非授权用户完全不暴露该接口的存在。

    ``name`` 同 ``make_auth_middleware``；默认按角色名生成（如 ``inner_only_middleware``）。
    """
    roles = {r.strip().lower() for r in allowed_roles}

    async def middleware(request: Request):
        user = getattr(request.ctx, "user", None) or {}
        role = str(user.get("role") or "").strip().lower()
        if role not in roles:
            lang = get_lang_from_header(request.headers)
            key = "not_found" if status == 404 else "forbidden"
            return response.json({"error": t(key, lang)}, status=status)

    middleware.__name__ = name or f"{'_'.join(sorted(roles))}_only_middleware"
    return middleware


auth_middleware = make_auth_middleware()
"""标准用户认证：验令牌、注入 ctx.user。"""
