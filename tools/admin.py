"""管理员账号命令行工具。

    python tools/admin.py show
    python tools/admin.py create -n admin -p 'secret123' --super --org 默认组织
    python tools/admin.py passwd -u admin -p 'newsecret'
    python tools/admin.py addrole -u alice -r manager --org 默认组织
    python tools/admin.py delete -u alice

相对原项目脚本的三处修正：
  - **改用 ORM 而非裸 SQL**。原脚本用 ``INSERT INTO superadmin (...)`` 手写字段列表 +
    PostgreSQL 专用的 ``$1`` 占位符：模型加一个 NOT NULL 字段它就崩，且锁死了数据库。
  - 建账号的三张表（superadmin / user_role / user_org）**放进事务**，
    避免中途失败留下"有账号没角色"的残缺状态。
  - 密码走 ``admin_service.create``，不再手工拼 hash。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 先把项目根塞进 sys.path，才能 import webx / app（本仓库不装成包）
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _bootstrap  # noqa: E402
from datetime import datetime

from tortoise.transactions import in_transaction

from webx.lib.funcs import now_ms
from webx.lib.genid import gen_id
from webx.lib.regex_rules import CNAME_RE, EMAIL_RE, NAME_RE
from webx.lib.roles import ROLES, default_role
from webx.models import Admin, Organization, UserOrg, UserRole
from webx.services.admin_service import admin_service


def _timestr(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if ms else "-"


async def _resolve_org(orgid: str, org_name: str) -> str:
    """按 ``--orgid`` 或 ``--org``（模糊匹配）定位组织，返回其 ID。"""
    if orgid:
        if not await Organization.filter(id=orgid).exists():
            _bootstrap.fail(f"组织 ID {orgid} 不存在")
        return orgid

    if org_name:
        orgs = await Organization.filter(name__icontains=org_name)
        if not orgs:
            _bootstrap.fail(f"未找到名为 '{org_name}' 的组织")
        if len(orgs) > 1:
            listing = "\n".join(f"    {o.id}  {o.name}" for o in orgs)
            _bootstrap.fail(f"'{org_name}' 匹配到多个组织，请用 --orgid 精确指定:\n{listing}")
        return orgs[0].id

    _bootstrap.fail("必须提供组织 ID (--orgid) 或组织名称 (--org)")
    return ""  # 不可达，仅为类型完整


async def _find_admin(args) -> Admin:
    """按 ``--user-id`` 或 ``--username`` 定位管理员。"""
    if args.user_id:
        admin = await Admin.filter(id=args.user_id).first()
    elif args.username:
        admin = await Admin.filter(username=args.username).first()
    else:
        _bootstrap.fail("必须提供用户 ID (--user-id) 或用户名 (-u/--username)")
        return None  # 不可达

    if not admin:
        _bootstrap.fail("未找到该管理员")
    return admin


# ==================== 子命令 ====================


async def cmd_show(args) -> None:
    """管理员列表。"""
    rows = await Admin.all().order_by("-is_root", "-create_time").limit(args.limit).values(
        "id", "username", "realname", "mobile", "email", "forbid", "is_root", "create_time"
    )
    if not rows:
        print("暂无管理员数据")
        return

    print(f"共 {len(rows)} 个管理员:\n")
    print(f"{'ID':<18}{'用户名':<22}{'手机':<16}{'邮箱':<30}{'超管':<6}{'禁用':<6}创建时间")
    print("-" * 120)
    for a in rows:
        print(
            f"{a['id']:<18}{a['username']:<22}{a['mobile'] or '-':<16}"
            f"{a['email'] or '-':<30}{'是' if a['is_root'] else '否':<6}"
            f"{'是' if a['forbid'] else '否':<6}{_timestr(a['create_time'])}"
        )


async def cmd_create(args) -> None:
    """创建管理员，同时写入组织关系与角色。"""
    name = args.name
    if not (NAME_RE.match(name) or CNAME_RE.match(name) or EMAIL_RE.match(name)):
        _bootstrap.fail(f"用户名 '{name}' 格式不符合要求")

    role = "super" if args.super else args.role
    if role not in ROLES:
        _bootstrap.fail(f"无效的角色 '{role}'，可选: {', '.join(ROLES)}")

    orgid = await _resolve_org(args.orgid, args.org)

    # 用邮箱当用户名时，username 另取一个随机短串，避免两处语义混淆
    is_email = bool(EMAIL_RE.match(name))
    username = gen_id()[:8] if is_email else name
    email = name if is_email else ""

    if await Admin.filter(**({"email": name} if is_email else {"username": name})).exists():
        _bootstrap.fail(f"'{name}' 已存在")

    now = now_ms()
    # 三张表一个事务：中途失败不该留下"有账号没角色"的残缺状态
    async with in_transaction():
        result = await admin_service.create(
            username, args.password, is_root=1 if args.super else 0, email=email
        )
        if not result["ok"]:
            _bootstrap.fail(result["errmsg"])
        admin_id = result["data"]["id"]

        await UserRole.create(
            id=gen_id(), user_id=admin_id, role_name=role, orgid=orgid,
            display_name=ROLES[role][1], create_time=now, is_default=1,
        )
        await UserOrg.create(
            id=gen_id(), user_id=admin_id, orgid=orgid,
            is_root=1 if args.super else 0, is_default=1,
            create_time=now, update_time=now,
        )

    print(f"✅ 管理员 '{name}' 创建成功  ID={admin_id}  角色={role}  组织={orgid}")


async def cmd_delete(args) -> None:
    """删除管理员及其组织/角色关系。"""
    admin = await _find_admin(args)
    if admin.is_root:
        _bootstrap.fail(f"不能删除超级管理员 {admin.username}")

    async with in_transaction():
        await UserRole.filter(user_id=admin.id).delete()
        await UserOrg.filter(user_id=admin.id).delete()
        await Admin.filter(id=admin.id).delete()

    print(f"✅ 管理员 '{admin.username}' (ID={admin.id}) 已删除")


async def cmd_passwd(args) -> None:
    """重置管理员密码。"""
    admin = await _find_admin(args)
    await admin_service.update_passwd(admin.id, args.password)
    print(f"✅ 管理员 '{admin.username}' (ID={admin.id}) 密码已更新")


async def cmd_addrole(args) -> None:
    """为管理员在某组织下追加角色。

    ⚠️ 与 ``create --super`` 不同：本命令**不会**授予组织超管
    （``UserOrg.is_root`` 保持 0）。跨组织授权默认只给角色，
    需要在新组织也当超管请直接改库或另行开命令。
    """
    if args.role not in ROLES:
        _bootstrap.fail(f"无效的角色 '{args.role}'，可选: {', '.join(ROLES)}")

    admin = await _find_admin(args)
    orgid = await _resolve_org(args.orgid, args.org)

    if await UserRole.filter(user_id=admin.id, orgid=orgid, role_name=args.role).exists():
        _bootstrap.fail(f"'{admin.username}' 在组织 {orgid} 已有 '{args.role}' 角色")

    # 组织关系可能还不存在（跨组织授权），一并补上
    if not await UserOrg.filter(user_id=admin.id, orgid=orgid).exists():
        now = now_ms()
        await UserOrg.create(
            id=gen_id(), user_id=admin.id, orgid=orgid,
            create_time=now, update_time=now,
        )

    await UserRole.create(
        id=gen_id(), user_id=admin.id, role_name=args.role, orgid=orgid,
        display_name=ROLES[args.role][1], create_time=now_ms(),
    )
    print(f"✅ 已为 '{admin.username}' 在组织 {orgid} 添加 '{args.role}' 角色")


# ==================== 入口 ====================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="webx 管理员账号管理")
    sub = parser.add_subparsers(dest="command")

    p_show = sub.add_parser("show", help="管理员列表")
    p_show.add_argument("--limit", type=int, default=200, help="最多显示条数")

    p_create = sub.add_parser("create", help="创建管理员")
    p_create.add_argument(
        "-n", "--name", required=True,
        help="用户名（5-36 字符，字母/数字/中文）或邮箱",
    )
    p_create.add_argument("-p", "--password", required=True, help="密码")
    p_create.add_argument("-s", "--super", action="store_true", help="设为超级管理员")
    p_create.add_argument("--orgid", default="", help="组织 ID")
    p_create.add_argument("--org", default="", help="组织名称（模糊匹配）")
    p_create.add_argument("-r", "--role", default=default_role(), help="角色名")

    p_del = sub.add_parser("delete", help="删除管理员")
    p_del.add_argument("--user-id", default="", help="用户 ID")
    p_del.add_argument("-u", "--username", default="", help="用户名")

    p_pwd = sub.add_parser("passwd", help="重置密码")
    p_pwd.add_argument("--user-id", default="", help="用户 ID")
    p_pwd.add_argument("-u", "--username", default="", help="用户名")
    p_pwd.add_argument("-p", "--password", required=True, help="新密码")

    p_role = sub.add_parser(
        "addrole", help="追加角色（不授予组织超管，见 cmd_addrole 说明）"
    )
    p_role.add_argument("--user-id", default="", help="用户 ID")
    p_role.add_argument("-u", "--username", default="", help="用户名")
    p_role.add_argument("--orgid", default="", help="组织 ID")
    p_role.add_argument("--org", default="", help="组织名称（模糊匹配）")
    p_role.add_argument("-r", "--role", required=True, help="角色名")

    return parser


HANDLERS = {
    "show": cmd_show,
    "create": cmd_create,
    "delete": cmd_delete,
    "passwd": cmd_passwd,
    "addrole": cmd_addrole,
}


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    _bootstrap.run(args, HANDLERS[args.command])


if __name__ == "__main__":
    main()
