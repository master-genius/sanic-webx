"""把真实客户端 IP 注入 ``request.ctx.client_ip``。

控制器只读 ``request.ctx.client_ip``，不必各自拼反代头解析逻辑。
只需按路由生效时，改用 ``webx.core.guards.with_client_ip`` 装饰器。
"""

from sanic import Request

from webx.core.guards import get_client_ip


async def client_ip(request: Request) -> None:
    request.ctx.client_ip = get_client_ip(request)
