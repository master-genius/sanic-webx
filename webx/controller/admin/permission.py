"""权限定义查询（可按 module 筛选）。"""

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

from webx.models.rbac import Permission


perm_bp = Blueprint("permission", url_prefix="/api/admin/permission")


@perm_bp.get("/")
async def list_permissions(request: Request):
    query = Permission.filter(orgid=request.ctx.orgid)
    module = request.args.get("module")
    if module:
        query = query.filter(module=module)

    perms = await query.order_by("module", "name").values(
        "id", "name", "display_name", "module", "module_name", "detail"
    )
    return json({"data": perms})
