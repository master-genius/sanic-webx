"""把注册表中的权限定义同步进数据库（幂等，可安全重复执行）。

放 services 而非 lib：它要写库。lib 是零 ORM 依赖的工具层，
让它 import models 会造成 lib ⇄ models 的包级循环。

典型接法：在应用启动钩子里调一次，代码即权威、DB 自动跟随。

    from webx.services.init_permission import init_permissions

    async def sync_perms(app):
        await init_permissions(orgid="")

    app = create_app("app.controller", on_startup=[sync_perms])
"""

from __future__ import annotations

import logging
import time

from webx.models import Permission, RolePermission
from webx.lib.permissions import MODULE_NAMES, PERMISSIONS
from webx.lib.genid import gen_id

logger = logging.getLogger(__name__)


async def _sync_one(name: str, perm: dict, orgid: str) -> None:
    now = int(time.time() * 1000)
    module_display = MODULE_NAMES.get(perm["module"], "")

    existing = await Permission.filter(name=name, orgid=orgid).first()
    if not existing:
        perm_id = gen_id()
        await Permission.create(
            id=perm_id,
            orgid=orgid,
            name=name,
            module=perm["module"],
            display_name=perm["display_name"],
            detail=perm.get("detail", ""),
            module_name=module_display,
            create_time=now,
            update_time=now,
        )
    else:
        perm_id = existing.id
        await Permission.filter(name=name, orgid=orgid).update(
            display_name=perm["display_name"],
            detail=perm.get("detail", ""),
            module=perm["module"],
            module_name=module_display,
            update_time=now,
        )

    # 角色默认权限：冗余字段一并写入/刷新，避免列表接口反复 join
    for role in perm.get("role") or []:
        payload = {
            "permission_name": name,
            "display_name": perm["display_name"],
            "detail": perm.get("detail", ""),
            "module": perm["module"],
            "module_name": module_display,
        }
        rp = await RolePermission.filter(
            role_name=role, orgid=orgid, permission_id=perm_id
        ).first()
        if rp:
            await RolePermission.filter(id=rp.id).update(**payload)
        else:
            await RolePermission.create(
                id=gen_id(), orgid=orgid, role_name=role,
                permission_id=perm_id, **payload,
            )


async def init_permissions(orgid: str = "", prune: bool = True) -> None:
    """同步全部权限定义，并清理注册表中已删除的权限。

    Args:
        orgid: 目标组织，单组织项目传 ""。
        prune: 是否删除注册表里已不存在的权限。

    ⚠️ **空注册表一律跳过清理**。清理语义是"代码里删掉的权限"，而空注册表
    表达的是"还没注册"——业务漏 import 权限声明模块（注册靠 import 副作用），
    或 import 排在启动钩子之后，都会让注册表在这一刻为空。若不拦，一次漏 import
    就会静默删光整张权限表及其角色关联。
    """
    if not PERMISSIONS:
        logger.warning(
            "权限注册表为空，跳过同步与清理。"
            "请确认已 import 业务的权限声明模块（register_permissions 靠 import 副作用生效）"
        )
        return

    existing = await Permission.filter(orgid=orgid).values("id", "name")

    for name, perm in PERMISSIONS.items():
        await _sync_one(name, perm, orgid)

    if not prune:
        return

    # 代码里删掉的权限，DB 里也删掉（连同其 role_permission 关联）
    stale = [p["name"] for p in existing if p["name"] not in PERMISSIONS]
    if stale:
        logger.info("清理已废弃权限 %d 个: %s", len(stale), ", ".join(stale))
        await RolePermission.filter(permission_name__in=stale, orgid=orgid).delete()
        await Permission.filter(name__in=stale, orgid=orgid).delete()
