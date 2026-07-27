"""CLI 测试（sqlite 内存库，真跑 ORM）。

CLI 是最容易腐坏的一类代码——平时没人跑，出问题往往是运维在深夜发现的。
所以这里直接调各 ``cmd_*`` 协程，绕开 argparse 与连库外壳，验证行为本身。

覆盖搬运时修掉的三个原件缺陷：
  - 建账号用 ORM 且三张表落在一个事务里（原件是裸 SQL、无事务）
  - ``setlevel`` 写入真实 epoch 毫秒（原件误用事件循环单调时钟）
  - ``init-perm`` 连库后再查组织（原件在 Tortoise.init 之前就查，必崩）
"""

import argparse
import pathlib
import sys

import pytest

# tools/ 不是包，按路径加载（脚本本身可 python tools/xxx.py 直接跑）
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "tools"))

import admin as admin_cli  # noqa: E402
import org as org_cli  # noqa: E402
from webx.lib.funcs import now_ms
from webx.lib.permissions import clear, register_permissions
from webx.models import Admin, Organization, Permission, UserOrg, UserRole


def _args(**kw) -> argparse.Namespace:
    """构造子命令参数，未给的键一律取空值。"""
    defaults = dict(
        name="", password="", super=False, orgid="", org="", role="watcher",
        user_id="", username="", level=2, detail="", limit=200, default_org=False,
    )
    defaults.update(kw)
    return argparse.Namespace(**defaults)


@pytest.fixture
async def org(db):
    """一个可用的组织。"""
    return await Organization.create(
        id="org0001", name="研发中心", orgcode="RD000001",
        create_time=now_ms(), update_time=now_ms(),
    )


# ==================== 组织 ====================


async def test_org_create(db):
    await org_cli.cmd_create(_args(name="研发中心"))

    created = await Organization.filter(name="研发中心").first()
    assert created is not None
    assert len(created.orgcode) == 8


async def test_org_create_rejects_duplicate(db, org):
    with pytest.raises(SystemExit):
        await org_cli.cmd_create(_args(name="研发中心"))


async def test_org_setlevel_writes_real_epoch_ms(db, org):
    """等级有效期必须是真实 epoch 毫秒——原件用的是事件循环单调时钟。"""
    before = now_ms()
    await org_cli.cmd_setlevel(_args(orgid=org.id, level=3))
    after = now_ms()

    updated = await Organization.filter(id=org.id).first()
    assert updated.level == 3
    # 落在调用前后的真实时间窗内，说明是 epoch 而非单调时钟（后者是个极小的数）
    assert before <= updated.level_start <= after
    assert updated.level_end == updated.level_start + org_cli.LEVEL_DURATION_MS


async def test_org_resolve_by_name_rejects_ambiguous(db):
    """同名模糊匹配到多个时必须拒绝，而不是随便挑一个。"""
    for i in range(2):
        await Organization.create(id=f"o{i}", name=f"研发中心{i}", create_time=now_ms())

    with pytest.raises(SystemExit):
        await org_cli.cmd_setlevel(_args(name="研发中心", level=1))


async def test_org_init_perm(db, org):
    """init-perm 在连库之后查组织并同步权限（原件在 init 之前查，必崩）。"""
    clear()
    register_permissions(
        {"view-order": {"display_name": "查看订单", "module": "order", "role": ["manager"]}}
    )
    try:
        await org_cli.cmd_init_perm(_args(org="研发中心"))
        assert await Permission.filter(orgid=org.id, name="view-order").exists()
    finally:
        clear()


async def test_org_init_perm_requires_target(db):
    with pytest.raises(SystemExit):
        await org_cli.cmd_init_perm(_args())


# ==================== 管理员 ====================


async def test_admin_create_writes_three_tables(db, org):
    await admin_cli.cmd_create(
        _args(name="alice", password="secret123", orgid=org.id, role="manager")
    )

    created = await Admin.filter(username="alice").first()
    assert created is not None
    assert created.passwd and created.passwd != "secret123", "密码必须是哈希"

    assert await UserRole.filter(user_id=created.id, role_name="manager").exists()
    assert await UserOrg.filter(user_id=created.id, orgid=org.id).exists()


async def test_admin_create_super_sets_root_flags(db, org):
    await admin_cli.cmd_create(
        _args(name="rootadmin", password="secret123", orgid=org.id, super=True)
    )

    created = await Admin.filter(username="rootadmin").first()
    assert created.is_root == 1
    assert (await UserOrg.filter(user_id=created.id).first()).is_root == 1
    assert await UserRole.filter(user_id=created.id, role_name="super").exists()


async def test_admin_create_with_email_username(db, org):
    """用邮箱当标识时，email 落 email 字段，username 另取随机短串。"""
    await admin_cli.cmd_create(
        _args(name="a@example.com", password="secret123", orgid=org.id)
    )

    created = await Admin.filter(email="a@example.com").first()
    assert created is not None
    assert created.username != "a@example.com"


async def test_admin_create_rejects_bad_role(db, org):
    """名字合法、只有角色非法——确保拒绝原因确实是角色。"""
    with pytest.raises(SystemExit):
        await admin_cli.cmd_create(
            _args(name="bobbyx", password="secret123", orgid=org.id, role="no-such-role")
        )


async def test_admin_create_rejects_unknown_org(db):
    """名字与角色都合法，只有组织不存在。"""
    with pytest.raises(SystemExit):
        await admin_cli.cmd_create(
            _args(name="bobbyx", password="secret123", orgid="nope")
        )


async def test_admin_create_is_atomic(db, org, monkeypatch):
    """建账号中途失败必须整体回滚，不留"有账号没角色"的残缺状态。"""
    async def boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(UserRole, "create", boom)

    with pytest.raises(RuntimeError):
        await admin_cli.cmd_create(
            _args(name="ghost", password="secret123", orgid=org.id)
        )

    assert not await Admin.filter(username="ghost").exists(), "事务未回滚"


async def test_admin_passwd(db, org):
    await admin_cli.cmd_create(_args(name="carol", password="secret123", orgid=org.id))
    old = (await Admin.filter(username="carol").first()).passwd

    await admin_cli.cmd_passwd(_args(username="carol", password="newsecret1"))

    new = (await Admin.filter(username="carol").first()).passwd
    assert new != old

    from webx.services.admin_service import admin_service
    assert admin_service.verify_passwd_hash("newsecret1", new)


async def test_admin_delete_cleans_relations(db, org):
    await admin_cli.cmd_create(_args(name="davidx", password="secret123", orgid=org.id))
    created = await Admin.filter(username="davidx").first()

    await admin_cli.cmd_delete(_args(username="davidx"))

    assert not await Admin.filter(id=created.id).exists()
    assert not await UserRole.filter(user_id=created.id).exists()
    assert not await UserOrg.filter(user_id=created.id).exists()


async def test_admin_delete_refuses_root(db, org):
    await admin_cli.cmd_create(
        _args(name="root2", password="secret123", orgid=org.id, super=True)
    )
    with pytest.raises(SystemExit):
        await admin_cli.cmd_delete(_args(username="root2"))


async def test_admin_addrole(db, org):
    await admin_cli.cmd_create(_args(name="erinxx", password="secret123", orgid=org.id))
    created = await Admin.filter(username="erinxx").first()

    await admin_cli.cmd_addrole(_args(username="erinxx", orgid=org.id, role="manager"))

    roles = await UserRole.filter(user_id=created.id, orgid=org.id).values_list(
        "role_name", flat=True
    )
    assert set(roles) == {"watcher", "manager"}


async def test_admin_addrole_rejects_duplicate(db, org):
    await admin_cli.cmd_create(_args(name="frank", password="secret123", orgid=org.id))
    with pytest.raises(SystemExit):
        await admin_cli.cmd_addrole(_args(username="frank", orgid=org.id, role="watcher"))


async def test_admin_addrole_creates_missing_org_link(db, org):
    """跨组织授权时，缺失的组织关系应自动补上。"""
    await admin_cli.cmd_create(_args(name="grace", password="secret123", orgid=org.id))
    other = await Organization.create(id="org0002", name="市场部", create_time=now_ms())
    created = await Admin.filter(username="grace").first()

    await admin_cli.cmd_addrole(_args(username="grace", orgid=other.id, role="manager"))

    assert await UserOrg.filter(user_id=created.id, orgid=other.id).exists()


async def test_admin_lookup_requires_identifier(db):
    with pytest.raises(SystemExit):
        await admin_cli.cmd_passwd(_args(password="x"))


async def test_admin_create_rejects_short_username(db, org):
    """用户名须 5-36 字符（NAME_RE，沿用原项目规则）。"""
    with pytest.raises(SystemExit):
        await admin_cli.cmd_create(
            _args(name="bob", password="secret123", orgid=org.id)
        )
