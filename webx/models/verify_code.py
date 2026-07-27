"""验证码表与其枚举。"""

import time

from tortoise import fields
from tortoise.models import Model

from webx.lib.genid import gen_id


class VerifyCodeState:
    """验证码状态。单向流转，不可逆。"""

    PENDING = "pending"     # 待验证
    VERIFIED = "verified"   # 已验证（成功使用）
    EXPIRED = "expired"     # 已过期（超时或重试过多）


class VerifyCodeBizType:
    """验证码业务场景。落库时记录，校验时精确匹配，防跨场景串用。"""

    LOGIN = "login"         # 验证码登录（默认），用户不存在则自动注册
    REGISTER = "register"   # 纯注册（用户已存在则报错）
    CHANGE = "change"       # 变更验证（仅验证身份，不登录/注册；兼作找回密码）
    BIND = "bind"           # 绑定/换绑邮箱或手机（码发往待绑定的新标识，证明其所有权）


class VerifyCodeChannel:
    """发送渠道。与 ``senders`` 注册表的 key 对应。"""

    EMAIL = "email"
    MOBILE = "mobile"


class VerifyCodeReason:
    """失败原因码（机器可读）。

    让 service 用结构化 reason 表达失败语义，控制器据此查表映射 HTTP 状态码，
    彻底解耦「协议状态码」与「展示文案」——改文案 / 切 i18n 都不影响状态码。
    注释里的状态码即 ``services.verifycode.VC_REASON_STATUS`` 的映射目标。
    """

    INVALID_FORMAT = "invalid_format"           # 邮箱/手机号格式错      → 400
    UNSUPPORTED_CHANNEL = "unsupported_channel" # 渠道未开通             → 400
    RATE_LIMITED = "rate_limited"               # IP 维度限流            → 429
    COOLDOWN = "cooldown"                       # 发送冷却中             → 429
    DAILY_LIMITED = "daily_limited"             # 当日发送已达上限        → 429
    LOCKED = "locked"                           # 标识已被锁定           → 429
    SEND_FAILED = "send_failed"                 # 投递失败（上游故障）    → 502
    NOT_FOUND = "not_found"                     # 无可用验证码           → 404
    EXPIRED = "expired"                         # 验证码已过期           → 410
    INVALID_CODE = "invalid_code"               # 验证码错误             → 400
    CONFLICT = "conflict"                       # 账号已存在（纯注册）     → 409
    REGISTER_FAILED = "register_failed"         # 注册/自动注册失败       → 500


class VerifyCode(Model):
    """验证码记录。

    明文存储 code：有效期以分钟计、且有重试熔断，hash 的收益不抵可读性成本。
    """

    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    code = fields.CharField(max_length=6, description="明文验证码")
    channel_type = fields.CharField(max_length=20, index=True)  # email / mobile
    identifier = fields.CharField(max_length=60, index=True, description="邮箱或手机号")
    user_id = fields.CharField(
        max_length=16, default="", description="登录态发码的归属用户 ID（公开发码为空）"
    )
    biz_type = fields.CharField(max_length=20, default=VerifyCodeBizType.LOGIN)
    state = fields.CharField(max_length=10, default=VerifyCodeState.PENDING, index=True)
    created_at = fields.BigIntField(description="发送时间戳（毫秒）")
    verified_at = fields.BigIntField(null=True, description="验证成功时间戳（毫秒）")
    expires_at = fields.BigIntField(description="过期时间戳（毫秒）")
    ip = fields.CharField(max_length=45, default="", description="请求来源 IP")
    retry_count = fields.IntField(default=0, description="验证失败重试次数")
    max_retries = fields.IntField(default=5, description="最大验证失败次数")

    class Meta:
        table = "verify_code"
        indexes = (
            ("identifier", "state"),
            ("channel_type", "state"),
            ("expires_at",),
            ("created_at",),
            ("ip", "created_at"),       # IP 维度限流窗口统计
            ("user_id", "created_at"),  # 登录态 bind 发码的每用户每日计数
        )

    @property
    def is_expired(self) -> bool:
        return int(time.time() * 1000) > self.expires_at

    @property
    def is_valid(self) -> bool:
        """未过期 + 状态 pending + 未超重试上限。"""
        now = int(time.time() * 1000)
        return (
            self.state == VerifyCodeState.PENDING
            and now <= self.expires_at
            and self.retry_count < self.max_retries
        )

    def __repr__(self) -> str:
        return (
            f"<VerifyCode id={self.id} channel={self.channel_type} "
            f"identifier={self.identifier[:8]}... state={self.state}>"
        )
