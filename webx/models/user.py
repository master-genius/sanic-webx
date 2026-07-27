"""C 端用户表。

**刻意不做 AbstractUser + AUTH_USER_MODEL 那套可替换机制**：实测 Tortoise 的
``Meta`` 是普通类属性，子类 ``Meta`` 会直接遮蔽父类的——抽象基类上声明的
``indexes`` / ``unique_together`` 在子类表上会**静默消失**，是最危险的那类失败。
所以这里给具体模型，字段取通用集合；项目要加字段就建关联表，
或用 ``extra`` 这个 JSON 文本字段兜住零散的扩展需求。
"""


from tortoise import fields
from tortoise.models import Model

from webx.lib.genid import gen_id


class User(Model):
    """C 端用户。"""

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    username = fields.CharField(max_length=50, unique=True, default="")
    email = fields.CharField(max_length=60, default="")
    mobile = fields.CharField(max_length=16, default="")
    passwd = fields.CharField(max_length=240, default="")
    # 是否已由用户主动设置过密码（0=未设置/系统随机密码，1=用户自设）。
    # 验证码自动注册的用户 passwd 是随机串、本人并不知晓，故 passwd_set=0；
    # 前端凭此区分「设置密码」（无需旧密码）与「修改密码」（需旧密码）两种 UI。
    passwd_set = fields.SmallIntField(default=0)
    nickname = fields.CharField(max_length=50, default="")
    avatar = fields.CharField(max_length=300, default="")
    sex = fields.SmallIntField(default=0)
    province = fields.CharField(max_length=30, default="")
    city = fields.CharField(max_length=50, default="")
    country = fields.CharField(max_length=30, default="")
    # 管理员手动封禁标记（登录失败自动锁定走 failed_count/failed_time，不动此字段）
    forbid = fields.SmallIntField(default=0)
    # 粗粒度身份标识（user / vip / inner 等），与数值型 level 互补、各管一维
    role = fields.CharField(max_length=32, default="user")
    failed_count = fields.IntField(default=0)
    failed_time = fields.BigIntField(default=0)
    # 通用积分/等级位（不需要就一直是 0，不影响任何逻辑）
    points = fields.IntField(default=0)
    total_points = fields.IntField(default=0)
    level = fields.SmallIntField(default=0)
    # 等级有效期（毫秒时间戳，0=未设置）：仅当 level>0 时才需校验该时间窗
    level_start = fields.BigIntField(default=0)
    level_end = fields.BigIntField(default=0)
    create_time = fields.BigIntField(default=0)
    update_time = fields.BigIntField(default=0)
    last_login_time = fields.BigIntField(default=0)
    last_login_ip = fields.CharField(max_length=45, default="")
    # 第三方登录标识（微信/OAuth 等），不用则留空
    openid = fields.CharField(max_length=64, null=True, unique=True)
    unionid = fields.CharField(max_length=64, default="")
    # 业务扩展位：塞不进上面字段的零散信息放这里（JSON 文本），避免为几个字段改框架
    extra = fields.TextField(default="{}")

    class Meta:
        table = "user"
        indexes = (
            ("create_time",),
            ("update_time",),
            ("unionid",),
            ("points",),
            ("level",),
        )
