"""管理后台账号表。

与 C 端 ``user`` 表完全分离：管理端与 C 端是两套身份，不共表、不互认，
令牌里用 ``role="admin"`` 区分。配合 ``webx.models.rbac`` 的组织/角色/权限体系。
"""

from tortoise import fields
from tortoise.models import Model

from webx.lib.genid import gen_id


class Admin(Model):
    """管理后台账号。表名 ``superadmin``，与 C 端 ``user`` 表完全分离。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    username = fields.CharField(max_length=50, unique=True)
    mobile = fields.CharField(max_length=16, default="")
    mobile_state = fields.SmallIntField(default=0)
    email = fields.CharField(max_length=60, default="")
    email_state = fields.SmallIntField(default=0)
    realname = fields.CharField(max_length=50, default="")
    sex = fields.SmallIntField(default=0)
    info = fields.TextField(default="")
    passwd = fields.CharField(max_length=240)
    create_time = fields.BigIntField(default=0)
    update_time = fields.BigIntField(default=0)
    failed_count = fields.IntField(default=0)
    failed_time = fields.BigIntField(default=0)
    # 手动封禁；登录失败自动锁定不动此字段
    forbid = fields.SmallIntField(default=0)
    # 系统级超管：跨组织无条件放行（与 rbac 的 UserOrg.is_root 是组织内超管，层级不同）
    is_root = fields.SmallIntField(default=0)
    openid = fields.CharField(max_length=64, default="")
    unionid = fields.CharField(max_length=64, default="")
    extra = fields.TextField(default="{}")

    class Meta:
        table = "superadmin"
        indexes = (
            ("create_time",),
            ("is_root",),
            ("openid",),
            ("unionid",),
        )
