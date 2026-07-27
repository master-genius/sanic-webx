"""用户单独授权：授予 / 撤销 / 查询。"""

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import validate

from webx.core.middleware import has_perm, is_privileged
from webx.models import Admin
from webx.models.dtos import PermissionGrantRequest
from webx.models.rbac import UserPermission
from webx.services.permission_service import user_permission_service


def _forbidden(msg: str = "权限不足"):
    return json({"error": msg}, status=403)


auth_perm_bp = Blueprint("auth_permission", url_prefix="/api/admin/auth-permission")


@auth_perm_bp.get("/")
async def list_user_permissions(request: Request):
    """单独授权记录列表（分页 + 多条件筛选）。"""
    if not has_perm(request, "view-auth-permission"):
        return _forbidden()

    query = UserPermission.filter(orgid=request.ctx.orgid)
    for key in ("user_id", "permission_name", "module"):
        value = request.args.get(key)
        if value:
            query = query.filter(**{key: value})

    # 分页参数容错：非法值退回默认，不让一个坏参数打成 500
    try:
        page = max(1, int(request.args.get("page", 1)))
        size = min(200, max(1, int(request.args.get("size", 20))))
    except (TypeError, ValueError):
        page, size = 1, 20

    total = await query.count()
    rows = await query.order_by("-create_time").offset((page - 1) * size).limit(size).values()
    return json({"data": {"total": total, "list": rows, "page": page, "size": size}})

@auth_perm_bp.get("/<record_id:str>")
async def get_user_permission(request: Request, record_id: str):
    if not has_perm(request, "view-auth-permission"):
        return _forbidden()

    data = await user_permission_service.get_one(record_id, request.ctx.orgid)
    if not data:
        return json({"error": "权限记录不存在"}, status=404)
    return json({"data": data})

@auth_perm_bp.post("/")
@validate(json=PermissionGrantRequest)
async def grant_permission(request: Request, body: PermissionGrantRequest):
    """授予/更新用户单独权限。"""
    if not has_perm(request, "auth-permission"):
        return _forbidden()

    # 不能给自己授权：否则任何有 auth-permission 的人都能自我提权到任意权限
    if body.user_id == request.ctx.user["user_id"]:
        return json({"error": "不能为自己授权"}, status=400)

    # 身份域校验放在这里（rbac service 只按 user_id 关联、不认识 Admin 表）
    target = await Admin.filter(id=body.user_id).first()
    if not target:
        return json({"error": "用户不存在"}, status=400)
    if target.is_root:
        return _forbidden("不能变更超级管理员的权限")

    try:
        result = await user_permission_service.grant(
            user_id=body.user_id,
            perm_name=body.perm_name,
            state=body.state,
            orgid=request.ctx.orgid,
        )
    except ValueError as e:
        return json({"error": str(e)}, status=400)
    return json({"data": result})

@auth_perm_bp.delete("/<record_id:str>")
async def revoke_permission(request: Request, record_id: str):
    """撤销一条单独授权。"""
    if not has_perm(request, "cancel-auth-permission"):
        return _forbidden()

    row = await UserPermission.filter(id=record_id, orgid=request.ctx.orgid).first()
    if not row:
        return json({"error": "权限记录不存在"}, status=404)

    # 治理类权限只有超管能撤，防止互相解除对方的管理能力
    if row.permission_name in ("auth-permission", "cancel-auth-permission"):
        if not is_privileged(request):
            return _forbidden("只有超级管理员才能撤销此权限")

    try:
        result = await user_permission_service.revoke(record_id, request.ctx.orgid)
    except ValueError as e:
        return json({"error": str(e)}, status=400)
    return json({"data": result})
