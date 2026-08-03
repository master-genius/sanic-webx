"""权限体系的查库路径测试（sqlite 内存库）。

覆盖 ``role_perm_middleware`` 三张表的实际查询、``init_permissions`` 的
同步与清理、单独授权的授予/撤销。
"""

import types

import pytest

from webx.middleware import ORG_HEADER, is_privileged, role_perm_middleware
from webx.services.init_permission import init_permissions
from webx.lib.permissions import clear, register_module, register_permissions
from webx.models.rbac import (
    Permission,
    RolePermission,
    UserOrg,
    UserPermission,
    UserRole,
)
from webx.services.permission_service import (
    MAX_USER_PERMS,
    user_permission_service,
    user_role_service,
)
from webx.lib.genid import gen_id


def _request(user_id: str, headers: dict | None = None):
    """构造一个够 role_perm_middleware 用的假请求。"""
    req = types.SimpleNamespace()
    req.ctx = types.SimpleNamespace(user={"user_id": user_id, "role": "admin"})
    req.headers = headers or {}
    return req


@pytest.fixture
def perms():
    """注册一小组权限定义，用完清干净。"""
    clear()
    register_module("order", "订单管理")
    register_permissions(
        {
            "view-order": {"display_name": "查看订单", "module": "order", "role": ["manager"]},
            "edit-order": {"display_name": "编辑订单", "module": "order", "role": ["manager"]},
            "drop-order": {"display_name": "删除订单", "module": "order", "role": ["super"]},
        }
    )
    yield
    clear()


# ==================== 权限同步 ====================


async def test_init_permissions_syncs_and_prunes(db, perms):
    await init_permissions(orgid="")

    assert await Permission.all().count() == 3
    # manager 自带两条，super 自带一条
    assert await RolePermission.filter(role_name="manager").count() == 2
    assert await RolePermission.filter(role_name="super").count() == 1

    # 从注册表移除一条后重同步 → DB 应随之删除（连同角色关联）
    clear()
    register_permissions({"view-order": {"display_name": "查看订单", "module": "order", "role": ["manager"]}})
    await init_permissions(orgid="")

    assert await Permission.all().count() == 1
    assert await RolePermission.filter(permission_name="edit-order").count() == 0


async def test_init_permissions_skips_empty_registry(db, perms):
    """空注册表绝不能把整张表当"已废弃"删光。"""
    await init_permissions(orgid="")
    assert await Permission.all().count() == 3

    clear()
    await init_permissions(orgid="")
    assert await Permission.all().count() == 3, "空注册表必须跳过清理"


# ==================== 中间件查库 ====================


async def _seed_membership(user_id: str, orgid: str, role: str, is_root: int = 0):
    await UserOrg.create(id=gen_id(), user_id=user_id, orgid=orgid, is_root=is_root, is_default=1)
    await UserRole.create(id=gen_id(), user_id=user_id, orgid=orgid, role_name=role, is_default=1)


async def test_role_perm_middleware_loads_permissions(db, perms):
    await init_permissions(orgid="org1")
    await _seed_membership("u1", "org1", "manager")

    req = _request("u1")
    await role_perm_middleware(req)

    assert req.ctx.orgid == "org1"
    assert req.ctx.org_roles == ["manager"]
    assert req.ctx.has_permission("view-order") is True
    assert req.ctx.has_permission("drop-order") is False, "super 专属权限不该给 manager"
    assert req.ctx.is_root is False


async def test_user_permission_overrides_role_in_db(db, perms):
    await init_permissions(orgid="org1")
    await _seed_membership("u2", "org1", "manager")

    perm = await Permission.filter(name="view-order", orgid="org1").first()
    await UserPermission.create(
        id=gen_id(), orgid="org1", user_id="u2",
        permission_id=perm.id, permission_name="view-order", state="deny",
    )

    req = _request("u2")
    await role_perm_middleware(req)
    assert req.ctx.has_permission("view-order") is False, "单独 deny 必须压过角色授予"


async def test_is_root_loaded_from_user_org(db, perms):
    await _seed_membership("u3", "org1", "watcher", is_root=1)

    req = _request("u3")
    await role_perm_middleware(req)

    assert req.ctx.is_root is True
    assert is_privileged(req) is True, "组织超管应无条件放行"


async def test_requested_org_must_be_owned(db, perms):
    """请求头指定的组织必须是用户确实所属的，否则回落默认组织（防越权切组织）。"""
    await init_permissions(orgid="org1")
    await _seed_membership("u4", "org1", "manager")

    req = _request("u4", headers={ORG_HEADER: "org-not-mine"})
    await role_perm_middleware(req)

    assert req.ctx.orgid == "org1", "不属于的组织不得被采纳"


async def test_middleware_no_roles_yields_empty_permissions(db, perms):
    """用户没有任何角色记录时安全降级为无权限，而不是报错。"""
    req = _request("nobody")
    await role_perm_middleware(req)

    assert req.ctx.org_roles == []
    assert req.ctx.has_permission("view-order") is False


# ==================== 单独授权 ====================


async def test_grant_and_revoke(db, perms):
    await init_permissions(orgid="org1")

    granted = await user_permission_service.grant(
        user_id="u5", perm_name="view-order", state="allow", orgid="org1"
    )
    assert granted["permission_name"] == "view-order"

    # 同状态重复授予视为无意义操作
    with pytest.raises(ValueError):
        await user_permission_service.grant(
            user_id="u5", perm_name="view-order", state="allow", orgid="org1"
        )

    # 改状态则更新而非新增
    await user_permission_service.grant(
        user_id="u5", perm_name="view-order", state="deny", orgid="org1"
    )
    assert await UserPermission.filter(user_id="u5").count() == 1

    await user_permission_service.revoke(granted["id"], "org1")
    assert await UserPermission.filter(user_id="u5").count() == 0


async def test_core_perms_cannot_be_granted(db, perms):
    """治理权限自身不可被当作普通权限授予，否则可自我提权。"""
    await init_permissions(orgid="org1")
    with pytest.raises(ValueError, match="核心权限"):
        await user_permission_service.grant(
            user_id="u6", perm_name="auth-permission", state="allow", orgid="org1"
        )


async def test_grant_requires_existing_permission_definition(db, perms):
    await init_permissions(orgid="org1")
    with pytest.raises(ValueError, match="不存在的权限定义"):
        await user_permission_service.grant(
            user_id="u7", perm_name="no-such-perm", state="allow", orgid="org1"
        )


async def test_grant_respects_record_cap(db):
    """单用户单独授权条数上限。"""
    clear()
    register_permissions(
        {f"perm-{i}": {"display_name": f"权限{i}", "module": "test"} for i in range(MAX_USER_PERMS + 1)}
    )
    await init_permissions(orgid="org1")

    for i in range(MAX_USER_PERMS):
        await user_permission_service.grant(
            user_id="u8", perm_name=f"perm-{i}", state="allow", orgid="org1"
        )

    with pytest.raises(ValueError, match="最多允许"):
        await user_permission_service.grant(
            user_id="u8", perm_name=f"perm-{MAX_USER_PERMS}", state="allow", orgid="org1"
        )
    clear()


# ==================== 默认组织 ====================


async def test_set_default_org_flips_both_tables(db, perms):
    """两张表的 is_default 必须同步翻转，否则两处看到不同的"默认组织"。"""
    await _seed_membership("u9", "org1", "manager")
    await UserOrg.create(id=gen_id(), user_id="u9", orgid="org2", is_default=0)
    await UserRole.create(id=gen_id(), user_id="u9", orgid="org2", role_name="watcher", is_default=0)

    await user_role_service.set_default_org("u9", "org2")

    assert (await UserOrg.filter(user_id="u9", orgid="org2").first()).is_default == 1
    assert (await UserRole.filter(user_id="u9", orgid="org2").first()).is_default == 1
    assert (await UserOrg.filter(user_id="u9", orgid="org1").first()).is_default == 0
    assert (await UserRole.filter(user_id="u9", orgid="org1").first()).is_default == 0


async def test_set_default_org_rejects_foreign_org(db, perms):
    await _seed_membership("u10", "org1", "manager")
    with pytest.raises(ValueError):
        await user_role_service.set_default_org("u10", "org-not-mine")


async def test_get_roles_structure(db, perms):
    await _seed_membership("u11", "org1", "manager")
    await UserRole.create(id=gen_id(), user_id="u11", orgid="org2", role_name="watcher")

    obj, rows = await user_role_service.get_roles("u11")

    assert obj["default_orgid"] == "org1"
    assert set(obj["orgs"]) == {"org1", "org2"}
    assert obj["org"]["org1"] == ["manager"]
    assert len(rows) == 2


# ==================== 身份语义（系统级 vs 组织级）====================


async def test_sys_root_claim_is_not_overwritten_by_org_flag(db, perms):
    """token 的 ``sys_root``（系统级）与 ctx.is_root（组织级）必须互不干扰。

    历史缺陷：两者曾共用 ``is_root`` 这一个 key，``role_perm_middleware`` 会把
    token 带来的系统级标记覆写成组织级值——同一个 key 的含义随中间件链漂移。
    """
    req = _request("sysroot")
    req.ctx.user["sys_root"] = 1  # auth_middleware 从 token 注入

    await role_perm_middleware(req)

    assert req.ctx.user["sys_root"] == 1, "系统级标记不得被组织级覆写"
    assert req.ctx.is_root is False, "该用户在此组织没有 UserOrg 记录"
    assert "is_root" not in req.ctx.user, "组织级标记的唯一出处是 ctx.is_root"


async def test_is_privileged_ignores_sys_root(db, perms):
    """``is_privileged`` 只回答"在当前组织内"，刻意不认系统超管。"""
    req = _request("sysroot")
    req.ctx.user["sys_root"] = 1

    await role_perm_middleware(req)

    assert is_privileged(req) is False, (
        "系统超管不属于该组织时不应越过其权限体系；"
        "需要跨组织通吃请显式判 ctx.user['sys_root']"
    )
