"""角色定义查询：供后台渲染角色下拉与层级判定。"""

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

from webx.lib.roles import ROLES, role_level, role_name


role_bp = Blueprint("role", url_prefix="/api/admin/role")


@role_bp.get("/")
async def get_roles(request: Request):
    return json(
        {"data": {"roles": ROLES, "role_level": role_level(), "role_name": role_name()}}
    )
