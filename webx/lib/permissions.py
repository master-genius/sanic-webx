"""权限注册表。

原项目把全部权限硬编码在 ``lib/permissions.py``（"创建订单""查看订单统计"…），
框架化后**框架不预置任何业务权限**——只提供注册接口，业务在自己的模块里声明::

    # app/permissions.py
    from webx.lib.permissions import register_module, register_permissions

    register_module("order", "订单管理")

    register_permissions({
        "create-order": {
            "display_name": "创建订单", "module": "order", "detail": "",
            "role": ["super", "manager", "salesman"],
        },
        "view-order": {
            "display_name": "查看订单", "module": "order",
            "role": ["super", "manager", "accountant", "salesman"],
        },
    })

声明里的 ``role`` 列表会在 ``init_permissions()`` 时自动写入 role_permission，
即"这些角色默认拥有该权限"。
"""

from __future__ import annotations

# name -> {display_name, module, detail, role}
PERMISSIONS: dict[str, dict] = {}

# module -> 模块中文名（用于后台权限树分组展示）
MODULE_NAMES: dict[str, str] = {}


def register_permissions(items: dict[str, dict], override: bool = True) -> None:
    """注册一批权限定义。

    Args:
        items: name -> {display_name, module, detail?, role?}
        override: 同名是否覆盖（默认覆盖，便于业务改写）
    """
    for name, meta in items.items():
        if not override and name in PERMISSIONS:
            continue
        PERMISSIONS[name] = {
            "display_name": meta.get("display_name", name),
            "module": meta.get("module", "common"),
            "detail": meta.get("detail", ""),
            "role": list(meta.get("role") or []),
        }


def register_module(module: str, display_name: str) -> None:
    """注册模块显示名。"""
    MODULE_NAMES[module] = display_name


def clear() -> None:
    """清空注册表（测试用）。"""
    PERMISSIONS.clear()
    MODULE_NAMES.clear()
