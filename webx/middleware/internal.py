"""服务间鉴权中间件：校验 ``X-Internal-Token``。

挂在 ``controller/internal/__mid__.py`` 上，整组内部接口一次覆盖——比给每个路由各加一个装饰器更不容易漏。
令牌未配置（``INTERNAL_TOKEN`` 为空）时**一律拒绝**：空配置绝不能等价于放行。
常量时间比较，不泄露前缀匹配长度。
"""

from __future__ import annotations

import hmac

from sanic import Request, response

from webx.config import settings


async def internal_auth_middleware(request: Request):
    expected = settings.INTERNAL_TOKEN
    token = request.headers.get("X-Internal-Token") or ""
    if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
        return response.json({"error": "unauthorized"}, status=401)
