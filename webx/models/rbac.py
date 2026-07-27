"""RBAC 模型：组织 / 角色 / 权限三层关系。

启用方式：把 ``"webx.models"`` 加进业务的 ``MODELS_MODULES``，
再跑 ``python -m webx.cli.dbsync`` 建表。不需要 RBAC 的项目完全不受影响。

权限判定优先级：**用户单独权限 > 角色权限**——``UserPermission`` 里有记录就以其
``state`` 为准（可显式 deny 掉角色自带的权限），没有才回落 ``RolePermission``。

注意本模块**不含 User 表**：用户表字段因项目而异（有的要手机号、有的要微信
openid），强行统一只会到处打补丁。RBAC 只按 ``user_id`` 字符串关联，业务用
什么用户表都行。
"""

import random
import string

from tortoise import fields
from tortoise.models import Model

from webx.lib.genid import gen_id


class Organization(Model):
    """组织/租户。单组织项目留一条 orgid="" 的默认记录即可。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    name = fields.CharField(max_length=128)
    orgcode = fields.CharField(max_length=24, default="")
    detail = fields.TextField(default="")
    contact = fields.CharField(max_length=64, default="")
    mobile = fields.CharField(max_length=20, default="")
    email = fields.CharField(max_length=100, default="")
    website = fields.CharField(max_length=200, default="")
    logo = fields.CharField(max_length=200, default="")
    is_forbid = fields.SmallIntField(default=0)
    forbid_detail = fields.CharField(max_length=200, default="")
    # 认证等级与有效期（与 User.level 对称；不用则一直是 0，不影响任何逻辑）
    level = fields.SmallIntField(default=0)
    level_start = fields.BigIntField(default=0)
    level_end = fields.BigIntField(default=0)
    create_time = fields.BigIntField(default=0)
    update_time = fields.BigIntField(default=0)

    class Meta:
        table = "organization"

    @staticmethod
    def make_org_code(prefix: str = "") -> str:
        random_part = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        return f"{prefix}{random_part}"


class UserOrg(Model):
    """用户-组织关系。``is_root`` 标记组织内的超管（权限判定直接放行）。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    user_id = fields.CharField(max_length=16)
    orgid = fields.CharField(max_length=16)
    is_root = fields.SmallIntField(default=0)
    is_default = fields.SmallIntField(default=0)
    create_time = fields.BigIntField(default=0)
    update_time = fields.BigIntField(default=0)

    class Meta:
        table = "user_org"
        indexes = (("user_id",), ("orgid",), ("create_time",))
        unique_together = (("user_id", "orgid"),)


class UserRole(Model):
    """用户在某组织内担任的角色（一个用户在一个组织内可有多个角色）。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    user_id = fields.CharField(max_length=16, default="")
    role_name = fields.CharField(max_length=40)
    display_name = fields.CharField(max_length=50, default="")
    orgid = fields.CharField(max_length=16, default="")
    create_time = fields.BigIntField(default=0)
    # 排序三元组：解析"默认组织"时按 -is_default, org_order, display_order 取首条
    display_order = fields.SmallIntField(default=0)
    org_order = fields.SmallIntField(default=0)
    is_default = fields.SmallIntField(default=0)

    class Meta:
        table = "user_role"
        indexes = (("user_id",), ("orgid",))
        unique_together = (("user_id", "orgid", "role_name"),)


class Permission(Model):
    """权限定义表。由 ``registry.PERMISSIONS`` 经 ``init_permissions()`` 同步而来。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    name = fields.CharField(max_length=50, default="unknown")
    display_name = fields.CharField(max_length=200, default="")
    detail = fields.CharField(max_length=200, default="")
    module = fields.CharField(max_length=30, default="common")
    module_name = fields.CharField(max_length=40, default="")
    orgid = fields.CharField(max_length=16, default="")
    create_time = fields.BigIntField(default=0)
    update_time = fields.BigIntField(default=0)

    class Meta:
        table = "permission"
        indexes = (("orgid",), ("module",), ("create_time",))
        unique_together = (("name", "orgid"),)


class RolePermission(Model):
    """角色自动携带的权限（冗余 name/display_name 以免列表接口反复 join）。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    role_name = fields.CharField(max_length=50, default="")
    permission_id = fields.CharField(max_length=16)
    permission_name = fields.CharField(max_length=100, default="")
    display_name = fields.CharField(max_length=100, default="")
    module = fields.CharField(max_length=30, default="")
    module_name = fields.CharField(max_length=40, default="")
    detail = fields.CharField(max_length=200, default="")
    orgid = fields.CharField(max_length=16, default="")

    class Meta:
        table = "role_permission"
        indexes = (("role_name",), ("permission_name",), ("orgid",))
        unique_together = (("role_name", "permission_id"),)


class UserPermission(Model):
    """用户单独权限（``state`` 为 allow/deny，优先级高于角色权限）。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    permission_id = fields.CharField(max_length=16)
    permission_name = fields.CharField(max_length=50, default="")
    display_name = fields.CharField(max_length=100, default="")
    module = fields.CharField(max_length=30, default="")
    module_name = fields.CharField(max_length=40, default="")
    user_id = fields.CharField(max_length=16, default="")
    state = fields.CharField(max_length=30, default="allow")
    orgid = fields.CharField(max_length=16)
    create_time = fields.BigIntField(default=0)
    update_time = fields.BigIntField(default=0)

    class Meta:
        table = "user_permission"
        indexes = (("user_id",), ("permission_id",), ("orgid",))
        unique_together = (("user_id", "permission_id", "orgid"),)
