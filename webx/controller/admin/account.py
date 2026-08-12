"""管理端账号 CRUD。

权限判定统一走 ``has_perm``，超管（is_root / super 角色）自动放行。
中间件由本目录的 ``__mid__.py`` 声明：``admin_auth_middleware + role_perm_middleware``。
"""

from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import validate
from tortoise.transactions import in_transaction

from webx.models.dtos import (
    AccountCreateRequest,
    AccountResetPasswdRequest,
    AccountUpdateRequest,
)
from webx.models import Admin
from webx.lib.roles import ROLES, default_role
from webx.services.admin_service import admin_service
from webx.middleware import has_perm, is_privileged
from webx.models import UserOrg, UserRole
from webx.lib.funcs import now_ms
from webx.lib.genid import gen_id

# 对外暴露的字段白名单：绝不包含 passwd
FIELDS = (
    "id", "username", "realname", "sex", "email", "mobile",
    "email_state", "mobile_state", "info", "forbid", "is_root",
    "create_time", "update_time",
)


def _view(admin: Admin) -> dict:
    return {f: getattr(admin, f) for f in FIELDS}


def _forbidden(msg: str = "权限不足"):
    return json({"error": msg}, status=403)

account_bp = Blueprint("webx_account", url_prefix="/api/admin/account")


@account_bp.get("/")
async def list_accounts(request: Request):
    """当前组织下的账号列表。"""
    if not has_perm(request, "list-admin"):
        return _forbidden()

    orgid = request.ctx.orgid
    uids = [u.user_id for u in await UserOrg.filter(orgid=orgid)]
    if not uids:
        return json({"data": {"total": 0, "list": []}})

    admins = await Admin.filter(id__in=uids).order_by("-is_root", "-create_time")
    return json({"data": {"total": len(uids), "list": [_view(a) for a in admins]}})

@account_bp.get("/<account_id:str>")
async def get_account(request: Request, account_id: str):
    """账号详情。看别人的需要 get-admin 权限，看自己的永远允许。"""
    if account_id != request.ctx.user["user_id"] and not has_perm(
        request, "get-admin"
    ):
        return _forbidden()

    # 限定在当前组织内，防跨组织读取
    if not await UserOrg.filter(orgid=request.ctx.orgid, user_id=account_id).exists():
        return json({"error": "用户不存在于当前组织"}, status=404)

    admin = await Admin.filter(id=account_id).first()
    if not admin:
        return json({"error": "用户不存在"}, status=404)
    return json({"data": _view(admin)})

@account_bp.post("/")
@validate(json=AccountCreateRequest)
async def create_account(request: Request, body: AccountCreateRequest):
    """创建账号，同时写入组织关系与默认角色（三张表一个事务）。"""
    if not has_perm(request, "create-admin"):
        return _forbidden()

    role = default_role()
    if role not in ROLES:
        return json({"error": f"默认角色未定义: {role}"}, status=500)
    # super 只能由 DB 直接铺设，不开放接口创建——否则等于开了提权后门
    if role == "super":
        return json({"error": "不能直接创建 super 用户"}, status=403)

    if await admin_service.get_by_username(body.username):
        return json({"error": "用户名已存在"}, status=409)

    orgid = request.ctx.orgid
    now = now_ms()

    async with in_transaction():
        result = await admin_service.create(
            body.username,
            body.password,
            email=body.email,
            mobile=body.mobile,
            realname=body.realname,
        )
        if not result["ok"]:
            return json({"error": result["errmsg"]}, status=409)
        admin_id = result["data"]["id"]

        await UserRole.create(
            id=gen_id(),
            user_id=admin_id,
            role_name=role,
            orgid=orgid,
            display_name=ROLES[role][1],
            create_time=now,
        )
        await UserOrg.create(
            id=gen_id(),
            user_id=admin_id,
            orgid=orgid,
            create_time=now,
            update_time=now,
        )

    admin = await Admin.filter(id=admin_id).first()
    return json({"data": _view(admin)}, status=201)

@account_bp.put("/<account_id:str>")
@validate(json=AccountUpdateRequest)
async def update_account(request: Request, account_id: str, body: AccountUpdateRequest):
    """更新账号资料。改自己永远允许，改别人需要 update-admin。"""
    if account_id != request.ctx.user["user_id"] and not has_perm(
        request, "update-admin"
    ):
        return _forbidden()

    if not await UserOrg.filter(orgid=request.ctx.orgid, user_id=account_id).exists():
        return json({"error": "用户不存在于当前组织"}, status=404)

    # exclude_unset：只更新前端确实传了的字段，未传的保持原值
    data = body.model_dump(exclude_unset=True)
    if not data:
        return json({"error": "没有需要更新的数据"}, status=400)

    await admin_service.update_profile(account_id, **data)
    admin = await Admin.filter(id=account_id).first()
    return json({"data": _view(admin)})

@account_bp.post("/<account_id:str>/passwd")
@validate(json=AccountResetPasswdRequest)
async def reset_passwd(request: Request, account_id: str, body: AccountResetPasswdRequest):
    """重置密码。改自己永远允许，改别人需要 update-admin-passwd。"""
    if account_id != request.ctx.user["user_id"] and not has_perm(
        request, "update-admin-passwd"
    ):
        return _forbidden()

    if not await UserOrg.filter(orgid=request.ctx.orgid, user_id=account_id).exists():
        return json({"error": "用户不存在于当前组织"}, status=404)

    # 非超管不得重置超管密码——否则低权限者可夺取超管账号
    if not is_privileged(request):
        target = await Admin.filter(id=account_id).first()
        if target and target.is_root:
            return _forbidden("不能重置超级管理员的密码")

    await admin_service.update_passwd(account_id, body.passwd)
    return json({"data": {"count": 1}})

@account_bp.delete("/<account_id:str>")
async def delete_account(request: Request, account_id: str):
    """删除账号，连同其组织与角色关系（一个事务）。"""
    if not has_perm(request, "delete-admin"):
        return _forbidden()

    if account_id == request.ctx.user["user_id"]:
        return _forbidden("不能删除自己")

    if not await UserOrg.filter(orgid=request.ctx.orgid, user_id=account_id).exists():
        return json({"error": "用户不存在于当前组织"}, status=404)

    target = await Admin.filter(id=account_id).first()
    if target and target.is_root:
        return _forbidden("不能删除超级管理员")

    async with in_transaction():
        await Admin.filter(id=account_id).delete()
        await UserRole.filter(user_id=account_id).delete()
        await UserOrg.filter(user_id=account_id).delete()

    return json({"data": {"message": "删除成功"}})
