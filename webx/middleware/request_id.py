"""请求 ID：贯穿日志的追踪标识。

沿用上游传入的 ``X-Request-Id``（网关/前端已生成时保持同一条链路），
没有则自己生成一个。注入 ``request.ctx.request_id``，业务打日志时带上即可
把一次请求的所有日志串起来。
"""

from sanic import Request

from webx.lib.genid import gen_id

HEADER = "X-Request-Id"


async def request_id(request: Request) -> None:
    incoming = (request.headers.get(HEADER) or "").strip()
    # 限长防止超长头污染日志；上游值不合规就退回自生成
    request.ctx.request_id = incoming[:64] if incoming else gen_id()
