"""管理后台账号服务。

与 UserService 结构对称，但作用于独立的 ``superadmin`` 表：管理端与 C 端是
两套身份，不共表、不互认。
"""

from __future__ import annotations

from tortoise.expressions import F

from webx.models import Admin
from webx.lib.roles import ROLES, default_role, role_level, role_name
from webx.config import settings
from webx.lib.funcs import now_ms
from webx.lib.genid import gen_id
from webx.lib.hash_password import HashPassword
from webx.lib.regex_rules import EMAIL_RE, MOBILE_RE

_hp = HashPassword()


class AdminService:
    """管理员服务。"""

    @property
    def roles(self) -> dict:
        return ROLES

    @property
    def default_role(self) -> str:
        return default_role()

    def get_role_levels(self) -> dict:
        """角色元数据：供后台渲染角色下拉与层级判定。"""
        return {"roles": ROLES, "role_level": role_level(), "role_name": role_name()}

    # ── 密码 ──────────────────────────────────────────────────

    def hash_passwd(self, passwd: str) -> str:
        return _hp.make(passwd)

    def verify_passwd_hash(self, plain: str, hashed: str) -> bool:
        return _hp.verify(plain, hashed)["ok"]

    async def verify_passwd(self, admin_id: str, passwd: str) -> bool:
        row = await Admin.filter(id=admin_id).only("passwd").first()
        return bool(row) and self.verify_passwd_hash(passwd, row.passwd)

    # ── 查询 ──────────────────────────────────────────────────

    async def get_by_id(self, admin_id: str) -> Admin | None:
        return await Admin.filter(id=admin_id).first()

    async def get_by_username(self, username: str) -> Admin | None:
        return await Admin.filter(username=username).first()

    # ── 创建 ──────────────────────────────────────────────────

    async def create(
        self, username: str, passwd: str, *, is_root: int = 0, **kwargs
    ) -> dict:
        """创建管理员账号。"""
        if await self.get_by_username(username):
            return {"ok": False, "errmsg": "用户名已存在"}

        now = now_ms()
        admin = await Admin.create(
            id=gen_id(),
            username=username,
            passwd=self.hash_passwd(passwd),
            email=kwargs.get("email", "") or "",
            mobile=kwargs.get("mobile", "") or "",
            realname=kwargs.get("realname", "") or "",
            is_root=is_root,
            create_time=now,
            update_time=now,
        )
        return {"ok": True, "data": self.to_dict(admin)}

    # ── 登录 ──────────────────────────────────────────────────

    async def login_verify(self, username: str, passwd: str) -> dict:
        """校验「用户名/邮箱/手机号 + 密码」。"""
        if MOBILE_RE.match(username):
            field = "mobile"
        elif EMAIL_RE.match(username):
            field = "email"
        else:
            field = "username"

        admin = await Admin.filter(**{field: username}).first()
        if not admin:
            return {"ok": False, "errmsg": "用户名不存在"}

        if admin.forbid:
            return {"ok": False, "errmsg": "账户已被禁用"}

        # 锁定窗口：连续失败达上限且仍在窗口内
        if (admin.failed_count or 0) >= settings.LOGIN_MAX_FAILED and (
            admin.failed_time > now_ms() - settings.LOGIN_LOCK_MINUTES * 60 * 1000
        ):
            return {"ok": False, "errmsg": "账户已被锁定，请稍后再试"}

        if not self.verify_passwd_hash(passwd, admin.passwd):
            now = now_ms()
            # F 表达式原子递增；不动 forbid（那是手动封禁位）
            await Admin.filter(id=admin.id).update(
                failed_count=F("failed_count") + 1, failed_time=now, update_time=now
            )
            return {"ok": False, "errmsg": "用户名或密码错误"}

        now = now_ms()
        await Admin.filter(id=admin.id).update(
            failed_count=0, failed_time=0, update_time=now
        )
        return {"ok": True, "data": self.to_dict(admin)}

    # ── 修改 ──────────────────────────────────────────────────

    async def update_passwd(self, admin_id: str, passwd: str) -> int:
        return await Admin.filter(id=admin_id).update(
            passwd=self.hash_passwd(passwd), update_time=now_ms()
        )

    async def update_profile(self, admin_id: str, **fields) -> int:
        """更新资料（email / mobile / realname / forbid）。None 值一律忽略。"""
        allowed = {"email", "mobile", "realname", "forbid"}
        payload = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not payload:
            return 0
        payload["update_time"] = now_ms()
        return await Admin.filter(id=admin_id).update(**payload)

    @staticmethod
    def to_dict(admin: Admin) -> dict:
        """转 dict 并剔除密码字段。"""
        return {f: getattr(admin, f) for f in admin._meta.fields_map if f != "passwd"}


admin_service = AdminService()
