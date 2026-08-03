"""用户角色管理：查询与批量增删。"""

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import validate
from tortoise.transactions import in_transaction

from webx.middleware import SUPER_ROLE, has_perm
from webx.lib.funcs import now_ms
from webx.lib.genid import gen_id
from webx.lib.roles import ROLES
from webx.models.dtos import UserRolesUpdateRequest
from webx.models.rbac import UserOrg, UserRole


def _forbidden(msg: str = "权限不足"):
    return json({"error": msg}, status=403)


user_role_bp = Blueprint("user_role", url_prefix="/api/admin/user-role")


@user_role_bp.get("/<uid:str>")
async def get_user_roles(request: Request, uid: str):
    """用户在当前组织下的角色。"""
    roles = await UserRole.filter(user_id=uid, orgid=request.ctx.orgid).values(
        "id", "user_id", "role_name", "orgid", "display_name", "is_default"
    )
    return json({"data": roles})

@user_role_bp.put("/<uid:str>")
@validate(json=UserRolesUpdateRequest)
async def update_user_roles(request: Request, uid: str, body: UserRolesUpdateRequest):
    """批量增删用户角色（一个事务）。"""
    if not (has_perm(request, "create-admin") or has_perm(request, "update-admin")):
        return _forbidden()

    if not body.create and not body.delete:
        return json({"error": "请指定要更新的角色"}, status=400)

    # super 不经接口增删：它是无条件放行角色，开放即等于开放提权
    if SUPER_ROLE in body.create:
        return _forbidden("不能通过接口授予 super 角色")
    if SUPER_ROLE in body.delete:
        return _forbidden("不能通过接口移除 super 角色")

    unknown = [r for r in body.create if r not in ROLES]
    if unknown:
        return json({"error": f"未定义的角色: {', '.join(unknown)}"}, status=400)

    orgid = request.ctx.orgid
    if not await UserOrg.filter(orgid=orgid, user_id=uid).exists():
        return json({"error": "用户不存在于当前组织"}, status=404)

    now = now_ms()
    created: list[dict] = []
    deleted = 0

    async with in_transaction():
        if body.delete:
            deleted = await UserRole.filter(
                user_id=uid, orgid=orgid, role_name__in=body.delete
            ).delete()

        for role in body.create:
            exists = await UserRole.filter(
                user_id=uid, orgid=orgid, role_name=role
            ).exists()
            if exists:
                continue
            role_id = gen_id()
            await UserRole.create(
                id=role_id,
                user_id=uid,
                orgid=orgid,
                role_name=role,
                display_name=ROLES[role][1],
                create_time=now,
            )
            created.append({"id": role_id, "role_name": role})

    return json({"data": {"create": created, "delete": deleted}})
