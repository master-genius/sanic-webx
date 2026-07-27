"""C 端用户服务：注册、登录、改密改名、联系方式绑定。"""

from __future__ import annotations

from tortoise.expressions import F

from webx.models import User
from webx.config import settings
from webx.lib.funcs import now_ms
from webx.lib.genid import gen_id
from webx.lib.hash_password import HashPassword
from webx.lib.regex_rules import EMAIL_RE, MOBILE_RE

# 模块级单例：共享同一套 hash 配置（scrypt）
_hp = HashPassword()


class UserService:
    """用户服务。返回统一的 ``{"ok": bool, ...}`` 结构，不抛异常。"""

    # ── 密码哈希 ──────────────────────────────────────────────

    def hash_passwd(self, passwd: str) -> str:
        """对明文密码做哈希，返回 ``salt.hash`` 格式字符串。"""
        return _hp.make(passwd)

    def verify_passwd_hash(self, plain: str, hashed: str) -> bool:
        return _hp.verify(plain, hashed)["ok"]

    # ── 查询 ──────────────────────────────────────────────────

    async def get_by_id(self, user_id: str) -> User | None:
        return await User.filter(id=user_id).first()

    async def get_by_username(self, username: str) -> User | None:
        return await User.filter(username=username).first()

    async def get_by_email(self, email: str) -> User | None:
        return await User.filter(email=email).first()

    async def get_by_mobile(self, mobile: str) -> User | None:
        return await User.filter(mobile=mobile).first()

    # ── 注册 ──────────────────────────────────────────────────

    async def register(
        self, username: str, passwd: str, *, passwd_set: int = 1, **kwargs
    ) -> dict:
        """注册新用户。

        Args:
            username: 用户名（必填，全局唯一）
            passwd: 明文密码
            passwd_set: 是否标记为「用户自设密码」。账密注册=1；验证码自动注册
                传的是随机密码、用户并不知晓，应传 0，以便其登录后首次设密。
            **kwargs: 可选字段（email / mobile / nickname / role 等）

        Returns:
            ``{"ok": True, "data": user_dict}`` 或 ``{"ok": False, "errmsg": ...}``
        """
        if await self.get_by_username(username):
            return {"ok": False, "errmsg": "用户名已存在"}

        email = kwargs.get("email", "")
        if email and await self.get_by_email(email):
            return {"ok": False, "errmsg": "邮箱已被注册"}

        mobile = kwargs.get("mobile", "")
        if mobile and await self.get_by_mobile(mobile):
            return {"ok": False, "errmsg": "手机号已被注册"}

        now = now_ms()
        user = await User.create(
            id=gen_id(),
            username=username,
            passwd=self.hash_passwd(passwd),
            passwd_set=passwd_set,
            email=email or "",
            mobile=mobile or "",
            nickname=kwargs.get("nickname", ""),
            role=kwargs.get("role", "user"),
            create_time=now,
            update_time=now,
            last_login_time=now,
        )
        return {"ok": True, "data": self.to_dict(user)}

    # ── 登录 ──────────────────────────────────────────────────

    async def login_verify(self, username: str, passwd: str) -> dict:
        """校验「用户名/邮箱/手机号 + 密码」并完成登录。

        登录标识按格式自动判定字段，无需前端区分。
        """
        if MOBILE_RE.match(username):
            field = "mobile"
        elif EMAIL_RE.match(username):
            field = "email"
        else:
            field = "username"

        user = await User.filter(**{field: username}).first()
        if not user:
            return {"ok": False, "errmsg": "用户名不存在"}

        if user.forbid:
            return {"ok": False, "errmsg": "账户已被禁用"}

        if self.is_locked(user):
            return {"ok": False, "errmsg": "账户已被锁定，请稍后再试"}

        # passwd_set=0（验证码自动注册的随机密码）禁止密码登录——用户本就不知道那串密码，
        # 应走验证码登录或登录后先「设置密码」
        if (
            not user.passwd_set
            or not user.passwd
            or not self.verify_passwd_hash(passwd, user.passwd)
        ):
            await self.record_failed_login(user)
            return {"ok": False, "errmsg": "用户名或密码错误"}

        now = now_ms()
        await User.filter(id=user.id).update(
            failed_count=0, failed_time=0, last_login_time=now, update_time=now
        )
        return {"ok": True, "data": self.to_dict(user)}

    # ── 修改 ──────────────────────────────────────────────────

    async def update_passwd(self, user_id: str, new_passwd: str) -> int:
        """更新密码。无论改密还是首次设密，统一置 passwd_set=1。"""
        return await User.filter(id=user_id).update(
            passwd=self.hash_passwd(new_passwd), passwd_set=1, update_time=now_ms()
        )

    async def update_username(self, user_id: str, new_username: str) -> int:
        """更新用户名。

        查重由调用方负责（需区分「被他人占用」与「本就是自己」），
        本方法只写库，保持服务层职责单一。
        """
        return await User.filter(id=user_id).update(
            username=new_username, update_time=now_ms()
        )

    async def set_contact(self, user_id: str, channel: str, value: str) -> int:
        """绑定/解绑联系方式，单条 UPDATE 直接覆盖。

        Args:
            channel: ``email`` 或 ``mobile``
            value: 新标识（绑定/换绑）或空串（解绑清空）

        全局唯一性校验与验证码/密码鉴权由调用方负责，本方法只写库。
        """
        if channel not in ("email", "mobile"):
            raise ValueError(f"unsupported channel: {channel}")
        return await User.filter(id=user_id).update(
            **{channel: value}, update_time=now_ms()
        )

    async def update_account(
        self,
        user_id: str,
        *,
        username: str | None = None,
        new_passwd: str | None = None,
    ) -> int:
        """一次性更新用户名/密码，单条 UPDATE 原子落库。

        用于「验证码改名改密」：避免分两条 update 出现中间态。
        两者皆 None 时不写库直接返回 0。用户名查重仍由调用方负责。
        """
        fields: dict = {}
        if username is not None:
            fields["username"] = username
        if new_passwd is not None:
            fields["passwd"] = self.hash_passwd(new_passwd)
            fields["passwd_set"] = 1  # 经此路径改密即视为用户自设
        if not fields:
            return 0
        fields["update_time"] = now_ms()
        return await User.filter(id=user_id).update(**fields)

    # ── 工具 ──────────────────────────────────────────────────

    @staticmethod
    def to_dict(user: User) -> dict:
        """转 dict 并**剔除密码字段**。所有对外返回一律经此方法。"""
        return {f: getattr(user, f) for f in user._meta.fields_map if f != "passwd"}

    @staticmethod
    def is_locked(user: User) -> bool:
        """是否处于登录失败锁定窗口内。"""
        if (user.failed_count or 0) < settings.LOGIN_MAX_FAILED:
            return False
        if not user.failed_time:
            return False
        return user.failed_time > now_ms() - settings.LOGIN_LOCK_MINUTES * 60 * 1000

    @staticmethod
    async def record_failed_login(user: User) -> None:
        """记录一次登录失败。

        用 F 表达式原子递增避免并发竞态；不动 forbid（那是管理员手动封禁位）。
        """
        now = now_ms()
        await User.filter(id=user.id).update(
            failed_count=F("failed_count") + 1, failed_time=now, update_time=now
        )


user_service = UserService()
