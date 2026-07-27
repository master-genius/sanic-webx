"""示例业务中间件：记录一次访问的追踪信息。

演示如何消费其他中间件注入的上下文（request_id / client_ip / lang），
以及"返回 response 即短路、返回 None 即放行"的约定。
"""

import logging

from sanic import Request

logger = logging.getLogger(__name__)


async def access_note(request: Request) -> None:
    logger.info(
        "[%s] %s %s from %s (lang=%s)",
        getattr(request.ctx, "request_id", "-"),
        request.method,
        request.path,
        getattr(request.ctx, "client_ip", "-"),
        getattr(request.ctx, "lang", "-"),
    )
