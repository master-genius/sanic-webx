"""角色权限查询 + 当前登录者的权限集合。"""

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

from webx.models.rbac import RolePermission


role_perm_bp = Blueprint("role_permission", url_prefix="/api/admin/role-permission")


@role_perm_bp.get("/")
async def get_current_permissions(request: Request):
    """当前登录者的权限集合（角色权限 + 单独权限），供前端控制菜单显隐。"""
    return json(
        {
            "data": {
                "permissions": getattr(request.ctx, "permissions", {}) or {},
                "user_permissions": getattr(request.ctx, "user_permissions", {}) or {},
                "org_roles": getattr(request.ctx, "org_roles", []) or [],
                "is_root": getattr(request.ctx, "is_root", False),
            }
        }
    )

@role_perm_bp.get("/<role:str>")
async def get_role_permissions(request: Request, role: str):
    """指定角色在当前组织下的权限列表。"""
    perms = await (
        RolePermission.filter(role_name=role, orgid=request.ctx.orgid)
        .order_by("module", "permission_name")
        .values(
            "id", "role_name", "permission_id", "permission_name",
            "display_name", "module", "module_name", "detail",
        )
    )
    return json({"data": perms})
