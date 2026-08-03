"""阶段 2/3：权限加载中间件 + 路由级校验装饰器 + 公共判权工具。

三者同处一个模块是「一个中间件一个模块」的**唯一例外**，理由是数据契约同源：

  - ``role_perm_middleware`` 负责**写** ``ctx.permissions`` / ``user_permissions`` /
    ``has_permission`` / ``org_roles`` / ``is_root``；
  - ``require_permission`` 与 ``has_perm`` / ``is_privileged`` 负责**读**同一批字段。

拆成两个文件只会让二者互相 import，且契约变更时容易只改一半。

⚠️ 必须挂在认证中间件之后：本模块只读 ``ctx.user["user_id"]``，
令牌无效在上一道即已 401。
"""

from __future__ import annotations

from functools import wraps
from typing import Callable

from sanic import Request
from sanic.response import json

from webx.i18n import get_lang_from_header, t
from webx.models.rbac import RolePermission, UserOrg, UserPermission, UserRole

# 拥有该角色即无条件放行（组织内最高权限）
SUPER_ROLE = "super"

# 指定组织的请求头。多组织项目由前端切换，单组织项目忽略即可。
ORG_HEADER = "x-orgid"


# ══════════════════ 阶段 2：角色权限加载（查库）══════════════════


def make_has_permission(
    role_permissions: dict[str, bool],
    user_permissions: dict[str, dict],
) -> Callable[[str], bool]:
    """构建权限判定闭包。

    优先级：用户单独权限 > 角色权限。用户在 user_permission 表里有记录就以其
    ``state`` 为准（可显式 deny 掉角色自带权限），否则看角色权限。
    """

    def has_permission(perm_name: str) -> bool:
        if perm_name in user_permissions:
            return user_permissions[perm_name]["state"] == "allow"
        return bool(role_permissions.get(perm_name))

    return has_permission


async def role_perm_middleware(request: Request) -> None:
    """加载当前用户在当前组织下的权限集合，注入 request.ctx。

    注入的上下文：
      - ``ctx.orgid`` 解析后的组织 ID
      - ``ctx.org_roles`` 该组织内的角色名列表
      - ``ctx.permissions`` 角色权限 {name: True}
      - ``ctx.user_permissions`` 用户单独权限 {name: row}
      - ``ctx.has_permission(name)`` 判定函数（同时挂到 ctx.user 上）
    """
    user_info = getattr(request.ctx, "user", None)
    if not user_info:
        # 防御性分支：正常流程下认证中间件已拦截无用户请求
        return

    uid = user_info.get("user_id") or ""
    if not uid:
        return

    # 组织-角色映射：按 是否默认 → 组织序 → 显示序 排序，首条 is_default 即默认组织
    rows = (
        await UserRole.filter(user_id=uid)
        .order_by("-is_default", "org_order", "display_order")
        .values("orgid", "role_name", "is_default")
    )

    org_roles_map: dict[str, list[str]] = {}
    default_orgid = None
    for r in rows:
        if r["is_default"] and not default_orgid:
            default_orgid = r["orgid"]
        org_roles_map.setdefault(r["orgid"], []).append(r["role_name"])

    # 请求头指定的组织必须是用户确实所属的组织，否则回落默认组织（防越权切组织）
    requested = request.headers.get(ORG_HEADER, "")
    orgid = requested if requested in org_roles_map else (default_orgid or "")
    org_roles = org_roles_map.get(orgid, [])

    role_permissions: dict[str, bool] = {}
    if org_roles:
        rp_rows = await RolePermission.filter(
            role_name__in=org_roles, orgid=orgid
        ).values("permission_name")
        for row in rp_rows:
            role_permissions[row["permission_name"]] = True

    up_rows = await UserPermission.filter(user_id=uid, orgid=orgid).values(
        "permission_name", "state"
    )
    user_permissions = {r["permission_name"]: r for r in up_rows}

    # 组织超管：UserOrg.is_root=1 的用户在该组织内无条件放行，与角色体系并行
    # （角色可能一个都没配，但超管必须能进）
    membership = await UserOrg.filter(user_id=uid, orgid=orgid).values("is_root")
    is_root = bool(membership and membership[0]["is_root"])

    checker = make_has_permission(role_permissions, user_permissions)

    request.ctx.orgid = orgid
    request.ctx.org_roles = org_roles
    request.ctx.is_root = is_root
    request.ctx.permissions = role_permissions
    request.ctx.user_permissions = user_permissions
    request.ctx.has_permission = checker

    # 同步进 ctx.user，方便控制器与装饰器统一从一处取。
    # 刻意**不**写 user_info["is_root"]：token 里的 sys_root 是系统级、
    # 这里的 is_root 是组织级，混进同一个 dict 迟早被误读。
    # 组织级标记的唯一出处是 ctx.is_root。
    user_info["orgid"] = orgid
    user_info["org_roles"] = org_roles
    user_info["has_permission"] = checker


# ══════════════════ 公共判权工具 ══════════════════


def is_privileged(request: Request) -> bool:
    """在**当前组织内**是否为无条件放行身份：组织超管或 ``super`` 角色。

    ⚠️ 刻意**不认**系统级超管（``Admin.is_root`` / token 的 ``sys_root``）。
    系统超管若不属于该组织，就不该越过该组织的权限体系——这是多租户下想要的行为，
    不是缺陷。确实需要跨组织通吃时，请显式判 ``ctx.user["sys_root"]``，
    不要指望这个函数。
    """
    if getattr(request.ctx, "is_root", False):
        return True
    return SUPER_ROLE in (getattr(request.ctx, "org_roles", None) or [])


def has_perm(request: Request, perm_name: str) -> bool:
    """在控制器里直接判权限的工具函数（超管 / super 角色自动通过）。"""
    if is_privileged(request):
        return True
    checker = getattr(request.ctx, "has_permission", None)
    return bool(checker and checker(perm_name))


# ══════════════════ 阶段 3：权限装饰器 ══════════════════


def require_permission(*perm_names: str) -> Callable:
    """声明路由所需权限。多个权限之间是 OR 关系（拥有任一即放行）。

    放行顺序：组织超管/super 角色 → 未声明权限（仅需登录）→ 逐个判定。
    权限名会绑定到 wrapper 的 ``__required_perms__``，便于生成文档/调试。
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            lang = get_lang_from_header(request.headers)
            user_info = getattr(request.ctx, "user", None)
            if not user_info:
                return json({"error": t("unauthorized", lang)}, status=401)

            # 组织超管 / super 角色无条件放行
            if is_privileged(request):
                return await func(request, *args, **kwargs)

            # 未声明具体权限 → 通过认证即可
            if not perm_names:
                return await func(request, *args, **kwargs)

            checker = user_info.get("has_permission")
            if not checker:
                # 只可能是漏挂 role_perm_middleware——明确报错，别静默放行
                return json(
                    {"error": t("forbidden", lang), "reason": "permissions_not_loaded"},
                    status=403,
                )

            if not any(checker(p) for p in perm_names):
                return json(
                    {"error": t("forbidden", lang), "perms": list(perm_names)},
                    status=403,
                )

            return await func(request, *args, **kwargs)

        wrapper.__required_perms__ = perm_names  # type: ignore[attr-defined]
        return wrapper

    return decorator
