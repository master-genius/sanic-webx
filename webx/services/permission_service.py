"""权限服务：用户单独权限的授予/撤销、用户角色与默认组织。

**刻意不 import Admin 模型**：本层只按 ``user_id`` 字符串关联，
"被授权者是否存在 / 是不是超管"这类校验属于身份域，由控制器在调用前完成。
"""

from __future__ import annotations

from tortoise.transactions import in_transaction

from webx.models import Permission, UserOrg, UserPermission, UserRole
from webx.lib.funcs import now_ms
from webx.lib.genid import gen_id

# 治理权限自身的权限：不允许被当作普通权限授予/撤销，否则可自我提权
CORE_PERMS = {"auth-permission", "cancel-auth-permission"}

# 单用户单独授权记录数上限：单独权限是例外机制，泛滥即说明角色划分有问题
MAX_USER_PERMS = 20


class UserPermissionService:
    """用户单独权限（优先级高于角色权限）。"""

    async def grant(
        self, *, user_id: str, perm_name: str, state: str, orgid: str
    ) -> dict:
        """授予或更新一条用户单独权限。

        Raises:
            ValueError: 核心权限 / 权限定义不存在 / 超出条数上限 / 状态未变化
        """
        if perm_name in CORE_PERMS:
            raise ValueError("禁止操作此核心权限")

        perm_def = await Permission.filter(name=perm_name, orgid=orgid).first()
        if not perm_def:
            raise ValueError("不存在的权限定义")

        rows = await UserPermission.filter(orgid=orgid, user_id=user_id).values(
            "id", "permission_name", "state"
        )
        existing = next((p for p in rows if p["permission_name"] == perm_name), None)

        if not existing and len(rows) >= MAX_USER_PERMS:
            raise ValueError(f"每个用户最多允许 {MAX_USER_PERMS} 条授权记录")

        now = now_ms()
        if existing:
            if existing["state"] == state:
                raise ValueError("已存在相同的授权状态，无需重复操作")
            await UserPermission.filter(id=existing["id"]).update(
                state=state, update_time=now
            )
            record_id = existing["id"]
        else:
            record_id = gen_id()
            await UserPermission.create(
                id=record_id,
                orgid=orgid,
                user_id=user_id,
                permission_id=perm_def.id,
                permission_name=perm_name,
                state=state,
                display_name=perm_def.display_name,
                module=perm_def.module,
                module_name=perm_def.module_name,
                create_time=now,
                update_time=now,
            )

        # 注意：不能写 .values().first()——ValuesQuery 没有 first()（Tortoise 1.x）
        rows = await UserPermission.filter(id=record_id).values()
        return rows[0] if rows else {}

    async def revoke(self, record_id: str, orgid: str) -> dict:
        """撤销一条授权记录，返回被删除的内容。"""
        row = await UserPermission.filter(id=record_id, orgid=orgid).first()
        if not row:
            raise ValueError("未找到指定权限记录或无权删除")
        data = {f: getattr(row, f) for f in row._meta.fields_map}
        await row.delete()
        return data

    async def get_one(self, record_id: str, orgid: str) -> dict | None:
        row = await UserPermission.filter(id=record_id, orgid=orgid).first()
        return {f: getattr(row, f) for f in row._meta.fields_map} if row else None


class UserRoleService:
    """用户角色与默认组织。"""

    async def get_roles(self, user_id: str) -> tuple[dict, list]:
        """取用户全部组织与角色。

        Returns:
            ``(结构化对象, 原始行列表)``。结构化对象含 orgs / org / role /
            default_orgid，供前端一次渲染组织切换器与角色标签。
        """
        rows = await (
            UserRole.filter(user_id=user_id)
            .order_by("-is_default", "org_order", "display_order")
            .values(
                "user_id", "orgid", "role_name", "display_name",
                "display_order", "is_default",
            )
        )

        obj: dict = {"orgs": [], "org": {}, "role": {}, "default_orgid": None}
        for r in rows:
            if r["is_default"] and not obj["default_orgid"]:
                obj["default_orgid"] = r["orgid"]
            obj["role"].setdefault(r["role_name"], []).append(r["orgid"])
            if r["orgid"] not in obj["org"]:
                obj["org"][r["orgid"]] = []
                obj["orgs"].append(r["orgid"])
            obj["org"][r["orgid"]].append(r["role_name"])

        return obj, rows

    async def set_default_org(self, user_id: str, orgid: str) -> bool:
        """设置用户默认组织。

        UserRole 与 UserOrg 两张表的 is_default 必须同步翻转——中间件解析默认组织
        读的是 UserRole，账号列表等读的是 UserOrg，只改一边会让两处看到不同的"默认"。
        """
        if not await UserOrg.filter(user_id=user_id, orgid=orgid).exists():
            raise ValueError(f"用户 {user_id} 不属于组织 {orgid}")

        async with in_transaction():
            await UserRole.filter(user_id=user_id).update(is_default=0)
            await UserRole.filter(user_id=user_id, orgid=orgid).update(is_default=1)
            await UserOrg.filter(user_id=user_id).update(is_default=0)
            await UserOrg.filter(user_id=user_id, orgid=orgid).update(is_default=1)
        return True


user_permission_service = UserPermissionService()
user_role_service = UserRoleService()
