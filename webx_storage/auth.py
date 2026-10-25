"""服务间鉴权：``X-Internal-Token``。"""

from __future__ import annotations

import hmac
import logging
from functools import wraps

from sanic.request import Request
from sanic.response import json

from webx.config import settings

logger = logging.getLogger(__name__)


def require_internal_token(f):
    """校验 ``X-Internal-Token`` 等于 ``STORAGE_INTERNAL_TOKEN``。

    - 常量时间比较（``hmac.compare_digest``），不泄露前缀匹配长度。
    - 服务端未配置令牌时**一律拒绝**——空配置绝不能等价于放行。
    """

    @wraps(f)
    async def wrapper(request: Request, *args, **kwargs):
        expected = settings.STORAGE_INTERNAL_TOKEN
        token = request.headers.get("X-Internal-Token") or ""
        if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
            return json({"error": "unauthorized"}, status=401)
        return await f(request, *args, **kwargs)

    return wrapper
