"""管理端角色定义（角色名 → 层级 + 显示名）。

层级用于「谁能管谁」：高层级可操作低层级，同级不可互改。框架给一套通用默认，
业务可整体替换或增补——和权限注册表一样，**不改框架文件**。

    from webx.lib.roles import register_roles

    register_roles({
        "reviewer": (4, "审核员"),
    })

注意与 C 端 ``User.role``（user / vip 等粗粒度标识）无关，那是另一维。
``super`` 是 rbac 的无条件放行角色（见 `webx.middleware.SUPER_ROLE`），
删掉它会让 rbac 的超管放行失效。
"""

from __future__ import annotations

# 角色名 -> (层级, 显示名)
ROLES: dict[str, tuple[int, str]] = {
    "super": (9, "超级管理员"),
    "senior": (7, "高级管理员"),
    "manager": (5, "管理员"),
    "operator": (2, "运营"),
    "watcher": (1, "观察员"),
}

# 新账号未指定角色时的默认角色（取层级最低者，最小权限原则）
DEFAULT_ROLE = "watcher"


def register_roles(items: dict[str, tuple[int, str]], replace: bool = False) -> None:
    """增补或整体替换角色表。

    Args:
        items: 角色名 → (层级, 显示名)
        replace: True 则先清空默认角色表（慎用：会移除 ``super``）
    """
    if replace:
        ROLES.clear()
    ROLES.update(items)


def set_default_role(role: str) -> None:
    """设置新账号的默认角色。"""
    global DEFAULT_ROLE
    DEFAULT_ROLE = role


def default_role() -> str:
    return DEFAULT_ROLE


def role_level() -> dict[str, int]:
    return {k: v[0] for k, v in ROLES.items()}


def role_name() -> dict[str, str]:
    return {k: v[1] for k, v in ROLES.items()}


def can_manage(actor_role: str, target_role: str) -> bool:
    """actor 是否有权管理 target（严格高层级才可以，同级不可互改）。

    未知角色层级视为 0，因此不认识的角色既管不了别人、也不会被误判为高权限。
    """
    levels = role_level()
    return levels.get(actor_role, 0) > levels.get(target_role, 0)
