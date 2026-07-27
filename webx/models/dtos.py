"""auth 组件的请求/响应 DTO（Pydantic v2）。

由 `sanic_ext.validate` 在进入 handler 前完成校验，不合规直接 400——
控制器里因此不必再写一遍长度/必填判断。
"""

from typing import Optional

from pydantic import BaseModel, Field, model_validator

# ==================== 通用 ====================


class SuccessResponse(BaseModel):
    message: str = Field(default="操作成功", description="消息")
    data: Optional[dict] = Field(None, description="数据")


class ErrorResponse(BaseModel):
    error: str = Field(..., description="错误信息")
    code: Optional[str] = Field(None, description="错误代码")


# ==================== 登录 / 注册 ====================


class LoginRequest(BaseModel):
    """账密登录。"""

    username: str = Field(..., min_length=1, description="用户名、邮箱或手机号")
    # min_length=1：仅「字段存在」不够，空串也算存在；在 DTO 层即以 400 拒绝空密码，
    # 不依赖后续哈希比对失败兜底。
    password: str = Field(..., min_length=1, description="密码")


class RegisterRequest(BaseModel):
    """账密注册。

    身份策略：username 必填，email 与 mobile **恰好二选一**，所选渠道必须配验证码。
    后续「绑定/换绑」再补另一渠道。
    """

    username: str = Field(..., min_length=3, max_length=50, description="用户名（3-50 字符）")
    password: str = Field(..., min_length=6, max_length=128, description="密码（6-128 字符）")
    email: Optional[str] = Field(None, max_length=60, description="邮箱（与 mobile 二选一）")
    mobile: Optional[str] = Field(None, max_length=16, description="手机号（与 email 二选一）")
    code: str = Field(..., min_length=6, max_length=6, description="所选渠道的验证码")
    nickname: Optional[str] = Field(None, max_length=50, description="昵称（可选）")

    @model_validator(mode="after")
    def _exactly_one_channel(self):
        # 同填或同空均非法。放在 DTO 层，控制器拿到的一定是合法组合。
        if bool(self.email) == bool(self.mobile):
            raise ValueError("email 与 mobile 必须恰好提供一个")
        return self


# ==================== 验证码 ====================


class VerifyCodeSendRequest(BaseModel):
    identifier: str = Field(..., max_length=60, description="邮箱地址或手机号")
    channel_type: str = Field(..., description="发送渠道: email / mobile")
    biz_type: str = Field(default="login", description="业务类型: login / register / change")


class VerifyCodeCheckRequest(BaseModel):
    identifier: str = Field(..., max_length=60, description="邮箱地址或手机号")
    code: str = Field(..., min_length=6, max_length=6, description="6 位验证码")
    channel_type: str = Field(default="", description="发送渠道（传入时精确匹配）")


# ==================== 个人资料 ====================


class UserPasswdUpdateRequest(BaseModel):
    """设置 / 修改密码。

    ``old_password`` 可选：仅当用户已自设过密码（passwd_set=1）时必填用于身份校验；
    未自设密码的用户（验证码自动注册）首次设密留空即可。放行判定在服务端。
    """

    old_password: Optional[str] = Field(
        None, min_length=1, max_length=128, description="旧密码（已设密码者必填）"
    )
    new_password: str = Field(..., min_length=6, max_length=128, description="新密码")


class UsernameUpdateRequest(BaseModel):
    """修改用户名（需当前密码验证身份）。"""

    password: str = Field(..., min_length=1, max_length=128, description="当前密码")
    new_username: str = Field(..., min_length=3, max_length=50, description="新用户名")


class AccountChangeByCodeRequest(BaseModel):
    """凭「邮箱/手机 + 验证码」改名/改密（无需登录，兼作找回密码）。

    身份渠道 email 与 mobile **恰好二选一**（与注册一致）；
    验证码须发往所选渠道（biz_type=change）。
    短信是否可用属于运行期状态，在 handler 里 gate，DTO 管不到。
    """

    email: Optional[str] = Field(None, max_length=60, description="账户邮箱（与 mobile 二选一）")
    mobile: Optional[str] = Field(None, max_length=16, description="账户手机号（与 email 二选一）")
    code: str = Field(..., min_length=6, max_length=6, description="6 位验证码")
    new_username: Optional[str] = Field(None, min_length=3, max_length=50, description="新用户名")
    new_password: Optional[str] = Field(None, min_length=6, max_length=128, description="新密码")

    @model_validator(mode="after")
    def _exactly_one_channel(self):
        # 同填或同空均非法，与 RegisterRequest 同款约束
        if bool(self.email) == bool(self.mobile):
            raise ValueError("email 与 mobile 必须恰好提供一个")
        return self

    @model_validator(mode="after")
    def _at_least_one_change(self):
        if self.new_username is None and self.new_password is None:
            raise ValueError("new_username 与 new_password 至少提供一个")
        return self


class EmailBindCodeRequest(BaseModel):
    email: str = Field(..., max_length=60, description="待绑定的新邮箱")


class EmailBindRequest(BaseModel):
    email: str = Field(..., max_length=60, description="待绑定的新邮箱")
    code: str = Field(..., min_length=6, max_length=6, description="发往新邮箱的验证码")


class MobileBindCodeRequest(BaseModel):
    mobile: str = Field(..., max_length=16, description="待绑定的新手机号")


class MobileBindRequest(BaseModel):
    mobile: str = Field(..., max_length=16, description="待绑定的新手机号")
    code: str = Field(..., min_length=6, max_length=6, description="发往新手机号的验证码")


class ContactUnbindRequest(BaseModel):
    """解绑邮箱/手机（用当前密码认证，而非验证码——待解绑的标识可能已不可达）。"""

    password: str = Field(..., min_length=1, max_length=128, description="当前密码")


class UserInfoResponse(BaseModel):
    id: str
    username: str
    email: Optional[str] = None
    mobile: Optional[str] = None
    nickname: Optional[str] = None
    avatar: Optional[str] = None
    sex: int = 0
    province: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    points: int = 0
    total_points: int = 0
    level: int = 0
    role: str = "user"
    passwd_set: int = 1
    last_login_time: Optional[int] = None
    create_time: Optional[int] = None


# ==================== 管理端账号 ====================


class AccountCreateRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    password: str = Field(..., min_length=6, max_length=128, description="密码")
    email: Optional[str] = Field(None, max_length=60, description="邮箱")
    mobile: Optional[str] = Field(None, max_length=16, description="手机号")
    realname: Optional[str] = Field(None, max_length=50, description="真实姓名")


class AccountUpdateRequest(BaseModel):
    email: Optional[str] = Field(None, max_length=60)
    mobile: Optional[str] = Field(None, max_length=16)
    realname: Optional[str] = Field(None, max_length=50)
    forbid: Optional[int] = Field(None, description="是否禁用 (0/1)")


class AccountResetPasswdRequest(BaseModel):
    passwd: str = Field(..., min_length=6, max_length=128, description="新密码")


class AccountResponse(BaseModel):
    id: str
    username: str
    email: Optional[str] = None
    mobile: Optional[str] = None
    realname: Optional[str] = None
    is_root: int = 0
    forbid: int = 0
    create_time: Optional[int] = None
    update_time: Optional[int] = None


# ==================== 权限管理 ====================


class PermissionGrantRequest(BaseModel):
    user_id: str = Field(..., description="用户 ID")
    perm_name: str = Field(..., description="权限名称")
    state: str = Field(default="allow", description="状态: allow / deny")


class UserRolesUpdateRequest(BaseModel):
    create: list[str] = Field(default=[], description="要添加的角色")
    delete: list[str] = Field(default=[], description="要删除的角色")
