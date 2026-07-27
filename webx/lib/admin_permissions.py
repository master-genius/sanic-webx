"""管理端自带权限定义。

``controller/admin/`` 下的端点用 ``has_perm(request, "create-admin")`` 之类做判定，
这些权限必须存在于注册表里，否则永远判为无权。业务启动时调一次::

    from webx.lib.admin_permissions import register_admin_permissions
    from webx.services.init_permission import init_permissions

    register_admin_permissions()          # 写入注册表（纯内存）
    await init_permissions(orgid="")      # 同步进数据库

``role`` 列表是"默认哪些角色自带该权限"，角色名对应 `webx.lib.roles.ROLES`。
"""

from __future__ import annotations

from webx.lib.permissions import register_module, register_permissions

# 权限名 -> (显示名, 说明, 默认角色)
_ADMIN_PERMISSIONS: dict[str, tuple[str, str, list[str]]] = {
    "list-admin": ("查看账号列表", "", ["super", "senior", "manager", "operator"]),
    "get-admin": ("查看账号详情", "查看其他账号的详细信息", ["super", "senior", "manager", "operator", "watcher"]),
    "create-admin": ("创建账号", "", ["super", "senior"]),
    "update-admin": ("更新他人账号", "每个人都可改自己的资料，改他人需要本权限", ["super", "senior"]),
    "update-admin-passwd": ("重置他人密码", "每个人都可改自己的密码，改他人需要本权限", ["super", "senior"]),
    "delete-admin": ("删除账号", "", ["super"]),
    "view-auth-permission": ("查看单独授权", "查看组织内用户的单独权限记录", ["super", "senior", "manager"]),
    "auth-permission": ("授予单独权限", "为组织内用户授予单独权限", ["super", "senior"]),
    "cancel-auth-permission": ("撤销单独权限", "撤销组织内用户的单独权限", ["super", "senior"]),
}


def register_admin_permissions(module: str = "admin", module_display: str = "账号管理") -> None:
    """把管理端权限写进权限注册表（幂等）。"""
    register_module(module, module_display)
    register_permissions(
        {
            name: {
                "display_name": display,
                "module": module,
                "detail": detail,
                "role": roles,
            }
            for name, (display, detail, roles) in _ADMIN_PERMISSIONS.items()
        }
    )
