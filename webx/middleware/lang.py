"""把请求语种解析结果注入 ``request.ctx.lang``。

省掉控制器里满地的 ``lang = get_lang_from_header(request.headers)``，
之后直接 ``t("key", request.ctx.lang)``。
"""

from sanic import Request

from webx.i18n import get_lang_from_header


async def lang(request: Request) -> None:
    request.ctx.lang = get_lang_from_header(request.headers)
