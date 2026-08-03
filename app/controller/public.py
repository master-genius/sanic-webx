"""公开接口示例：不需要登录（本目录的 __mid__ 未挂认证中间件）。

这是给你改的业务代码。真正的登录/注册在 ``webx/controller/auth/``，
不要在这里另写一套签发令牌的逻辑。
"""

from sanic import Blueprint
from sanic.response import json

from webx.config import settings
from webx.i18n import t

bp = Blueprint("public", url_prefix="/public")


@bp.get("/welcome")
async def welcome(request):
    """演示根级中间件注入的上下文（lang / request_id / client_ip）。"""
    return json({
        "message": settings.WELCOME_TEXT,
        "lang": request.ctx.lang,
        "request_id": request.ctx.request_id,
        "ok": t("success", request.ctx.lang),
    })
