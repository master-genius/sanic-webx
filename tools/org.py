"""组织命令行工具。

    python tools/org.py list
    python tools/org.py create -n "研发中心"
    python tools/org.py setlevel -n "研发中心" -l 2
    python tools/org.py init-perm --org "研发中心"

把原项目分散在 ``admin_cmd``（showorg / addorg）与 ``setauth_cmd``、
``init_permission_cmd`` 三个脚本里的组织操作**按资源归拢到一处**——
它们操作的是同一张表，分散只会让人记不住命令在哪。

两处修正：
  - ``setlevel`` 原先用 ``asyncio.get_event_loop().time()`` 当毫秒时间戳写进
    level_start/level_end。那是**单调时钟、原点任意**（通常是进程启动后的秒数），
    写进库就是个无意义的小数字，有效期判断必然出错。改用真实的 epoch 毫秒。
  - ``init-perm`` 原先在 ``Tortoise.init()`` **之前**就查 Organization，
    必然抛异常。现在连库统一由 ``_bootstrap.database()`` 先行完成。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 先把项目根塞进 sys.path，才能 import webx / app（本仓库不装成包）
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _bootstrap  # noqa: E402
from datetime import datetime

from webx.lib.funcs import now_ms
from webx.lib.genid import gen_id
from webx.lib.regex_rules import CNAME_RE, NAME_RE
from webx.models import Organization
from webx.services.init_permission import init_permissions

# 认证等级默认有效期：一年
LEVEL_DURATION_MS = 365 * 24 * 60 * 60 * 1000


def _timestr(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if ms else "-"


async def _resolve_org(orgid: str, org_name: str) -> Organization:
    """按 ID 或名称（模糊匹配）定位唯一组织。"""
    if orgid:
        org = await Organization.filter(id=orgid).first()
        if not org:
            _bootstrap.fail(f"组织 ID {orgid} 不存在")
        return org

    if org_name:
        orgs = await Organization.filter(name__icontains=org_name)
        if not orgs:
            _bootstrap.fail(f"未找到名为 '{org_name}' 的组织")
        if len(orgs) > 1:
            listing = "\n".join(f"    {o.id}  {o.name}" for o in orgs)
            _bootstrap.fail(f"'{org_name}' 匹配到多个组织，请用 --orgid 精确指定:\n{listing}")
        return orgs[0]

    _bootstrap.fail("必须提供组织 ID (--orgid) 或组织名称 (-n/--name)")
    return None  # 不可达


# ==================== 子命令 ====================


async def cmd_list(args) -> None:
    """组织列表。"""
    rows = await Organization.all().order_by("-create_time").limit(args.limit).values(
        "id", "name", "orgcode", "mobile", "email", "level", "create_time"
    )
    if not rows:
        print("暂无组织数据")
        return

    print(f"共 {len(rows)} 个组织:\n")
    print(f"{'ID':<18}{'名称':<32}{'组织代码':<12}{'等级':<6}{'手机':<16}创建时间")
    print("-" * 110)
    for o in rows:
        print(
            f"{o['id']:<18}{o['name']:<32}{o['orgcode'] or '-':<12}"
            f"{o['level']:<6}{o['mobile'] or '-':<16}{_timestr(o['create_time'])}"
        )


async def cmd_create(args) -> None:
    """创建组织。"""
    name = args.name
    if not (NAME_RE.match(name) or CNAME_RE.match(name)):
        _bootstrap.fail(f"组织名称 '{name}' 格式不符合要求")

    if await Organization.filter(name=name).exists():
        _bootstrap.fail(f"组织名称 '{name}' 已存在")

    now = now_ms()
    org = await Organization.create(
        id=gen_id(),
        name=name,
        orgcode=Organization.make_org_code(),
        detail=args.detail or f"命令行创建：{name}",
        create_time=now,
        update_time=now,
    )
    print(f"✅ 组织 '{name}' 创建成功  ID={org.id}  组织代码={org.orgcode}")


async def cmd_setlevel(args) -> None:
    """设置组织认证等级，有效期一年。"""
    org = await _resolve_org(args.orgid, args.name)

    # ⚠️ 必须是真实 epoch 毫秒。原脚本用事件循环的单调时钟，写进去是个
    # 无意义的小数字，等级有效期判断会全错。
    now = now_ms()
    await Organization.filter(id=org.id).update(
        level=args.level,
        level_start=now,
        level_end=now + LEVEL_DURATION_MS,
        update_time=now,
    )
    print(
        f"✅ 组织 '{org.name}' 认证等级设为 {args.level}"
        f"（有效期至 {_timestr(now + LEVEL_DURATION_MS)}）"
    )


async def cmd_init_perm(args) -> None:
    """把权限注册表同步进该组织。

    注册表内容来自代码（``register_permissions``），所以要先 import 到业务的
    权限声明模块。CLI 这条路只会 import ``app.config``（``_bootstrap.load_settings``），
    所以业务得保证自己的 ``register_permissions(...)`` 挂在那条 import 链上。

    ⚠️ 内置 admin 组的权限定义**不在**这条链上：``register_admin_permissions()``
    由 ``create_app`` 在服务进程里按 ``builtin_controller`` 调用，CLI 不装配应用、
    自然不会执行它。故本命令同步的只是业务自己声明的那部分。
    注册表为空时 ``init_permissions`` 整体跳过，不会误删库里已有的记录。
    """
    orgid = args.orgid
    if not orgid and args.org:
        orgid = (await _resolve_org("", args.org)).id
    if not orgid and not args.default_org:
        _bootstrap.fail("必须提供 --orgid / --org，或用 --default-org 初始化默认组织（orgid=\"\"）")

    await init_permissions(orgid)
    print(f"✅ 组织 {orgid or '(默认)'} 权限初始化完成")


# ==================== 入口 ====================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="webx 组织管理")
    sub = parser.add_subparsers(dest="command")

    p_list = sub.add_parser("list", help="组织列表")
    p_list.add_argument("--limit", type=int, default=200, help="最多显示条数")

    p_create = sub.add_parser("create", help="创建组织")
    p_create.add_argument("-n", "--name", required=True, help="组织名称")
    p_create.add_argument("--detail", default="", help="组织说明")

    p_level = sub.add_parser("setlevel", help="设置认证等级")
    p_level.add_argument("-n", "--name", default="", help="组织名称（模糊匹配）")
    p_level.add_argument("--orgid", default="", help="组织 ID")
    p_level.add_argument(
        "-l", "--level", type=int, default=2, choices=range(10), metavar="0-9",
        help="认证等级 0-9",
    )

    p_perm = sub.add_parser("init-perm", help="同步权限定义到该组织")
    p_perm.add_argument("--orgid", default="", help="组织 ID")
    p_perm.add_argument("--org", default="", help="组织名称（模糊匹配）")
    p_perm.add_argument(
        "--default-org", action="store_true",
        help='初始化默认组织（orgid=""），单组织项目用这个',
    )

    return parser


HANDLERS = {
    "list": cmd_list,
    "create": cmd_create,
    "setlevel": cmd_setlevel,
    "init-perm": cmd_init_perm,
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
