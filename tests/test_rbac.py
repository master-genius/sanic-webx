"""RBAC 单元测试（不依赖数据库）。

覆盖权限判定的三处纯逻辑：权限优先级、超管放行、空注册表保护。
涉及查库的部分见 test_rbac_db.py。
"""

import asyncio
import types

from webx.middleware import is_privileged, make_has_permission, require_permission
from webx.services.init_permission import init_permissions
from webx.lib.permissions import PERMISSIONS, clear, register_module, register_permissions


def _fake_request(**ctx):
    """构造一个只带 ctx 与 headers 的假请求（够本模块的判定逻辑用）。"""
    req = types.SimpleNamespace()
    req.ctx = types.SimpleNamespace(**ctx)
    req.headers = {}
    return req


# ── 权限优先级 ────────────────────────────────────────────────


def test_user_permission_overrides_role():
    """用户单独权限优先于角色权限——可显式 deny 掉角色自带的权限。"""
    checker = make_has_permission(
        role_permissions={"view-order": True, "edit-order": True},
        user_permissions={"edit-order": {"state": "deny"}},
    )
    assert checker("view-order") is True   # 仅角色赋予 → 通过
    assert checker("edit-order") is False  # 角色给了但用户被单独 deny → 拒绝


def test_user_permission_grants_beyond_role():
    """角色没有、但单独授予的权限同样生效。"""
    checker = make_has_permission(
        role_permissions={},
        user_permissions={"special": {"state": "allow"}},
    )
    assert checker("special") is True
    assert checker("unknown") is False


# ── 超管放行 ──────────────────────────────────────────────────


def test_is_privileged_by_root_flag():
    """UserOrg.is_root 的用户即便一个角色都没有也放行。"""
    assert is_privileged(_fake_request(is_root=True, org_roles=[])) is True


def test_is_privileged_by_super_role():
    assert is_privileged(_fake_request(is_root=False, org_roles=["super"])) is True


def test_not_privileged_for_plain_user():
    assert is_privileged(_fake_request(is_root=False, org_roles=["viewer"])) is False
    # ctx 上什么都没注入时不得误判为超管
    assert is_privileged(_fake_request()) is False


# ── require_permission 装饰器 ─────────────────────────────────


def _call(handler, request):
    return asyncio.run(handler(request))


def test_require_permission_allows_holder():
    @require_permission("view-order")
    async def handler(request):
        return "ok"

    checker = make_has_permission({"view-order": True}, {})
    req = _fake_request(
        user={"user_id": "u1", "has_permission": checker},
        org_roles=["viewer"],
        is_root=False,
    )
    assert _call(handler, req) == "ok"


def test_require_permission_rejects_non_holder():
    @require_permission("view-order")
    async def handler(request):
        return "ok"

    req = _fake_request(
        user={"user_id": "u1", "has_permission": make_has_permission({}, {})},
        org_roles=["viewer"],
        is_root=False,
    )
    assert _call(handler, req).status == 403


def test_require_permission_reports_missing_middleware():
    """漏挂 role_perm_middleware 时必须明确报错，绝不静默放行。"""

    @require_permission("view-order")
    async def handler(request):
        return "ok"

    req = _fake_request(user={"user_id": "u1"}, org_roles=[], is_root=False)
    resp = _call(handler, req)
    assert resp.status == 403
    assert b"permissions_not_loaded" in resp.body


def test_require_permission_lets_root_through():
    """超管无条件放行，不看 has_permission。"""

    @require_permission("anything")
    async def handler(request):
        return "ok"

    req = _fake_request(user={"user_id": "u1"}, org_roles=[], is_root=True)
    assert _call(handler, req) == "ok"


# ── 注册表与空表保护 ──────────────────────────────────────────


def test_registry_normalizes_entries():
    clear()
    register_module("order", "订单管理")
    register_permissions({"view-order": {"display_name": "查看订单", "module": "order"}})
    entry = PERMISSIONS["view-order"]
    assert entry["detail"] == ""     # 缺省字段补齐
    assert entry["role"] == []
    clear()


def test_init_permissions_skips_when_registry_empty():
    """空注册表必须直接返回——否则会把整张权限表当作"已废弃"删光。

    本用例不连库：若保护失效，函数会走到 Permission.filter(...) 而抛异常，
    测试即失败。这正是我们要挡住的那条破坏性路径。
    """
    clear()
    asyncio.run(init_permissions(orgid=""))  # 不抛异常 = 提前返回成功
