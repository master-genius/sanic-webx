"""验证码服务。

职责：
  1. 生成 + 明文落库（短期失效 + 重试熔断，hash 的收益不抵可读性成本）
  2. 三层防刷：IP 维度限流 / 单标识冷却 / 单标识每日上限
  3. 校验与单向状态流转（pending → verified / expired）
  4. 失败熔断锁定

发送通道走 `webx.lib.senders` 注册表，本模块**不 import 任何具体通道**
（短信是可选依赖，硬 import 会让没装 httpx 的项目直接崩在 import 期）。
"""

from __future__ import annotations

import logging
import secrets
import string
import time
from datetime import datetime, timezone

from webx.models import (
    VerifyCode,
    VerifyCodeBizType,
    VerifyCodeChannel,
    VerifyCodeReason,
    VerifyCodeState,
)
from webx.lib.senders import get_sender
from webx.config import settings
from webx.lib.funcs import mask_mobile, now_ms
from webx.lib.genid import gen_id
from webx.lib.regex_rules import EMAIL_RE, MOBILE_RE

logger = logging.getLogger(__name__)


# 失败原因码 → HTTP 状态码。协议语义集中、显式，取代「按中文文案子串嗅探」。
# 单一来源：注册/改资料/绑定等多个控制器共用，避免各处重复同一张表导致漂移。
VC_REASON_STATUS: dict[str, int] = {
    VerifyCodeReason.INVALID_FORMAT: 400,
    VerifyCodeReason.UNSUPPORTED_CHANNEL: 400,
    VerifyCodeReason.INVALID_CODE: 400,
    VerifyCodeReason.NOT_FOUND: 404,
    VerifyCodeReason.CONFLICT: 409,
    VerifyCodeReason.EXPIRED: 410,
    VerifyCodeReason.RATE_LIMITED: 429,
    VerifyCodeReason.COOLDOWN: 429,
    VerifyCodeReason.DAILY_LIMITED: 429,
    VerifyCodeReason.LOCKED: 429,
    VerifyCodeReason.REGISTER_FAILED: 500,
    VerifyCodeReason.SEND_FAILED: 502,
}


def vc_status(result: dict, default: int = 400) -> int:
    """按 service 返回的 reason 映射 HTTP 状态码；缺省/未知 → default。"""
    return VC_REASON_STATUS.get(result.get("reason"), default)


def _fail(reason: str, errmsg: str) -> dict:
    return {"ok": False, "reason": reason, "errmsg": errmsg}


def _today_start_ms() -> int:
    """当日零点（UTC）的毫秒时间戳，用于每日额度统计。"""
    now = datetime.now(tz=timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000)


class VerifyCodeService:
    """验证码核心服务。

    配置在**每次访问时**从 settings 读取（属性而非 __init__ 快照）：
    实例是模块级单例，若在构造时快照，业务后注册的 Settings 就永远生效不了。
    """

    @property
    def cooldown_seconds(self) -> int:
        return settings.VERIFY_CODE_COOLDOWN_SECONDS

    @property
    def valid_minutes(self) -> int:
        return settings.VERIFY_CODE_VALID_MINUTES

    @property
    def max_retries(self) -> int:
        return settings.VERIFY_CODE_MAX_RETRIES

    @property
    def lock_minutes(self) -> int:
        return settings.VERIFY_CODE_LOCK_MINUTES

    def _daily_limit(self, channel_type: str) -> int:
        return (
            settings.VERIFY_CODE_DAILY_LIMIT_EMAIL
            if channel_type == VerifyCodeChannel.EMAIL
            else settings.VERIFY_CODE_DAILY_LIMIT_MOBILE
        )

    # ==================== 发送 ====================

    async def send_code(
        self,
        identifier: str,
        channel_type: str,
        biz_type: str = VerifyCodeBizType.LOGIN,
        ip: str = "",
        user_id: str = "",
    ) -> dict:
        """发送验证码（前置限流 → 落库 → 投递）。

        Args:
            user_id: 登录态发码（如 bind）的归属用户。传入时该码带 user_id 落库，
                并对 ``biz_type=bind`` 施加「每用户每日上限」（不依赖 IP 通用限流）。

        Returns:
            ``{"ok": True, "expires_in": 300}``
            或 ``{"ok": False, "reason": <VerifyCodeReason>, "errmsg": ...}``
        """
        now = now_ms()

        # ── 0. 格式校验 ──
        if channel_type == VerifyCodeChannel.EMAIL:
            if not EMAIL_RE.match(identifier):
                return _fail(VerifyCodeReason.INVALID_FORMAT, "邮箱格式不正确")
        elif channel_type == VerifyCodeChannel.MOBILE:
            if not MOBILE_RE.match(identifier):
                return _fail(VerifyCodeReason.INVALID_FORMAT, "手机号格式不正确")

        # ── 0.2 渠道可用性前置闸 ──
        # 刻意放在限流统计与落库【之前】：通道没开通时不该白写一行验证码、
        # 白占掉该标识的当日额度与 IP 窗口。
        sender = get_sender(channel_type)
        if sender is None:
            return _fail(
                VerifyCodeReason.UNSUPPORTED_CHANNEL,
                f"{channel_type} 验证码暂未开通，请更换验证方式",
            )

        # ── 0.5 IP 维度限流（防单 IP 轮换轰炸大量目标）──
        # 复用本表既有行（每次发码插一行，带 ip + created_at）做窗口计数，无需额外存储。
        # fail-open：IP 为空或统计异常一律放行，绝不误伤正常用户。
        if ip:
            try:
                cnt_hour = await VerifyCode.filter(
                    ip=ip, created_at__gte=now - 3600 * 1000
                ).count()
                cnt_min = await VerifyCode.filter(
                    ip=ip, created_at__gte=now - 60 * 1000
                ).count()
                if (
                    cnt_hour >= settings.IP_RATE_LIMIT_PER_HOUR
                    or cnt_min >= settings.IP_RATE_LIMIT_PER_MIN
                ):
                    logger.warning(
                        "[验证码] IP 限流触发: ip=%s min=%d/%d hour=%d/%d",
                        ip, cnt_min, settings.IP_RATE_LIMIT_PER_MIN,
                        cnt_hour, settings.IP_RATE_LIMIT_PER_HOUR,
                    )
                    return _fail(VerifyCodeReason.RATE_LIMITED, "请求过于频繁，请稍后再试")
            except Exception as e:
                # 统计失败属基础设施异常，降级放行，不阻断正常用户发码
                logger.error("[验证码] IP 限流统计异常，降级放行: %s", e)

        # ── 1. 冷却检查 ──
        last = (
            await VerifyCode.filter(
                identifier=identifier,
                channel_type=channel_type,
                state=VerifyCodeState.PENDING,
            )
            .order_by("-created_at")
            .first()
        )
        if last:
            elapsed = (now - last.created_at) / 1000
            if elapsed < self.cooldown_seconds:
                remaining = int(self.cooldown_seconds - elapsed)
                return _fail(VerifyCodeReason.COOLDOWN, f"请等待 {remaining} 秒后再试")

        # ── 2. 锁定检查 ──
        if await self._is_locked(identifier, channel_type):
            return _fail(VerifyCodeReason.LOCKED, "该标识已被锁定，请稍后再试")

        # ── 3. 每日上限 ──
        today_start = _today_start_ms()
        sent_today = await VerifyCode.filter(
            identifier=identifier, channel_type=channel_type, created_at__gte=today_start
        ).count()
        if sent_today >= self._daily_limit(channel_type):
            return _fail(
                VerifyCodeReason.DAILY_LIMITED, "该标识今日发送已达上限，请明日再试"
            )

        # ── 3.5 登录态 bind：按 user_id 的每日上限（登录用户身份明确，取代 IP 限流）──
        if biz_type == VerifyCodeBizType.BIND and user_id:
            bind_today = await VerifyCode.filter(
                user_id=user_id,
                biz_type=VerifyCodeBizType.BIND,
                created_at__gte=today_start,
            ).count()
            if bind_today >= settings.VERIFY_CODE_DAILY_LIMIT_BIND:
                return _fail(
                    VerifyCodeReason.DAILY_LIMITED,
                    f"今日绑定验证码已达上限（{settings.VERIFY_CODE_DAILY_LIMIT_BIND} 个/天）",
                )

        # ── 4. 生成 + 落库 ──
        code = f"{secrets.randbelow(900000) + 100000}"
        vc = await VerifyCode.create(
            id=gen_id(),
            code=code,
            channel_type=channel_type,
            identifier=identifier,
            biz_type=biz_type,
            state=VerifyCodeState.PENDING,
            created_at=now,
            expires_at=now + self.valid_minutes * 60 * 1000,
            ip=ip,
            user_id=user_id,
            max_retries=self.max_retries,
        )

        # ── 5. 投递 ──
        # 失败即把该行烧成 EXPIRED：否则留下 PENDING 行会让用户被自己的冷却期挡住
        # （60 秒内无法重发），体验上等于"发不出去还不让重试"。
        try:
            await sender(
                identifier,
                code,
                valid_minutes=self.valid_minutes,
                biz_type=biz_type,
            )
        except Exception as e:
            masked = (
                mask_mobile(identifier)
                if channel_type == VerifyCodeChannel.MOBILE
                else identifier[:10] + "..."
            )
            logger.error("[验证码] 发送失败: %s %s", masked, e)
            await VerifyCode.filter(id=vc.id).update(state=VerifyCodeState.EXPIRED)
            return _fail(VerifyCodeReason.SEND_FAILED, "验证码发送失败，请稍后再试")

        logger.info(
            "[验证码] 发送成功: %s=%s... expires_in=%dmin",
            channel_type, identifier[:10], self.valid_minutes,
        )
        return {"ok": True, "expires_in": self.valid_minutes * 60}

    # ==================== 校验 ====================

    async def _consume(
        self, identifier: str, code: str, channel_type: str = "", biz_type: str = ""
    ) -> dict:
        """校验并消费验证码的公共前半段。

        Returns:
            ``{"ok": True, "vc": <VerifyCode>}``（**尚未**标记 VERIFIED，由调用方
            在业务动作成功后再标记——业务失败不该消耗用户的验证码）
            或 ``{"ok": False, "reason": ..., "errmsg": ...}``
        """
        query = VerifyCode.filter(identifier=identifier, state=VerifyCodeState.PENDING)
        if channel_type:
            query = query.filter(channel_type=channel_type)
        if biz_type:
            query = query.filter(biz_type=biz_type)
        vc = await query.order_by("-created_at").first()

        if not vc:
            return _fail(VerifyCodeReason.NOT_FOUND, "验证码不存在")

        if vc.is_expired:
            await VerifyCode.filter(id=vc.id).update(state=VerifyCodeState.EXPIRED)
            return _fail(VerifyCodeReason.EXPIRED, "验证码已过期")

        if vc.retry_count >= vc.max_retries:
            await VerifyCode.filter(id=vc.id).update(state=VerifyCodeState.EXPIRED)
            return _fail(VerifyCodeReason.LOCKED, "验证码失败次数过多，已锁定")

        # 明文比对。Python 的 != 是短路比较，理论上有计时攻击风险；
        # 但验证码仅 6 位数字且有重试熔断，攻击面极小，可接受。
        if code != vc.code:
            new_count = vc.retry_count + 1
            update: dict = {"retry_count": new_count}
            if new_count >= vc.max_retries:
                update["state"] = VerifyCodeState.EXPIRED
                logger.warning("[验证码] %s 验证失败过多，已锁定", vc.identifier)
            await VerifyCode.filter(id=vc.id).update(**update)
            return _fail(VerifyCodeReason.INVALID_CODE, "验证码错误")

        return {"ok": True, "vc": vc}

    async def _mark_verified(self, vc: VerifyCode) -> None:
        await VerifyCode.filter(id=vc.id).update(
            state=VerifyCodeState.VERIFIED, verified_at=now_ms()
        )

    async def verify_only(
        self,
        identifier: str,
        code: str,
        channel_type: str = "",
        biz_type: str = "",
    ) -> dict:
        """只回答「这个码对不对」，不做任何用户动作。

        与 ``verify_code`` 的差异：后者校验通过会自动登录或注册；本方法职责单一，
        后续业务动作（账密注册、改名改密、绑定邮箱…）全交调用方落库，可被多处复用。

        Args:
            biz_type: 传入时精确匹配，防止「登录码被拿去注册/改密」等跨场景串用。
        """
        result = await self._consume(identifier, code, channel_type, biz_type)
        if not result["ok"]:
            return result

        vc = result["vc"]
        await self._mark_verified(vc)
        logger.info("[验证码] %s 仅校验成功 (biz=%s)", vc.identifier, vc.biz_type)
        return {"ok": True, "identifier": vc.identifier, "channel_type": vc.channel_type}

    async def verify_code(
        self, identifier: str, code: str, channel_type: str = "", ip: str = ""
    ) -> dict:
        """校验并按 biz_type 分流：登录 / 注册 / 仅验证身份。

        Returns:
            ``{"ok": True, "action": "login"|"register"|"change", "user": ..., "token": ...}``
            或失败结构。注册成功不返回 token（用户需先设置密码）。
        """
        from webx.models import User
        from webx.services.user_service import user_service
        from webx.lib.token import get_token

        result = await self._consume(identifier, code, channel_type)
        if not result["ok"]:
            return result
        vc = result["vc"]

        field = "email" if vc.channel_type == VerifyCodeChannel.EMAIL else "mobile"

        # ── change：仅验证身份 ──
        if vc.biz_type == VerifyCodeBizType.CHANGE:
            await self._mark_verified(vc)
            logger.info("[验证码] %s 验证成功 (change)", vc.identifier)
            return {
                "ok": True,
                "action": "change",
                "token": None,
                "user": None,
                "identifier": vc.identifier,
                "channel_type": vc.channel_type,
            }

        existing = await User.filter(**{field: identifier}).first()

        # ── register：纯注册，已存在即冲突 ──
        if vc.biz_type == VerifyCodeBizType.REGISTER:
            if existing:
                return _fail(VerifyCodeReason.CONFLICT, "该账号已注册，请直接登录")
            reg = await self._register_with_retry(user_service, field, identifier)
            if not reg["ok"]:
                return _fail(
                    VerifyCodeReason.REGISTER_FAILED, f"注册失败: {reg['errmsg']}"
                )
            user_data = reg["data"]
            if ip:
                await User.filter(id=user_data["id"]).update(last_login_ip=ip)
            await self._mark_verified(vc)
            logger.info("[验证码] 注册成功: %s", user_data["username"])
            # 不返回 token：验证码注册的用户密码是随机串，需先设置密码
            return {
                "ok": True,
                "action": "register",
                "token": None,
                "user": self._user_view(user_data),
            }

        # ── login：默认场景，用户不存在则自动注册 ──
        action = "login"
        if not existing:
            reg = await self._register_with_retry(user_service, field, identifier)
            if not reg["ok"]:
                return _fail(
                    VerifyCodeReason.REGISTER_FAILED, f"自动注册失败: {reg['errmsg']}"
                )
            user_data = reg["data"]
            if ip:
                await User.filter(id=user_data["id"]).update(last_login_ip=ip)
            action = "register"
            logger.info("[验证码] 新用户自动注册: %s", user_data["username"])
        else:
            if existing.forbid:
                return _fail(VerifyCodeReason.REGISTER_FAILED, "账户已被禁用")
            user_data = user_service.to_dict(existing)
            update: dict = {"last_login_time": now_ms()}
            if ip:
                update["last_login_ip"] = ip
            await User.filter(id=existing.id).update(**update)

        await self._mark_verified(vc)

        token = get_token().make_token(
            {
                "user_id": user_data["id"],
                "username": user_data["username"],
                "role": user_data.get("role", "user"),
            },
            id_="user",
        )
        return {
            "ok": True,
            "action": action,
            "token": token,
            "user": self._user_view(user_data),
        }

    # ==================== 清理 ====================

    async def cleanup_expired(self) -> int:
        """删除 expired 状态的记录，返回删除数量（可挂定时任务）。"""
        count = await VerifyCode.filter(state=VerifyCodeState.EXPIRED).delete()
        if count:
            logger.info("[验证码] 清理过期记录: 删除 %d 条", count)
        return count

    # ==================== 内部 ====================

    async def _is_locked(self, identifier: str, channel_type: str) -> bool:
        """该标识是否仍在锁定窗口内。

        不额外建锁表：`state=EXPIRED 且 retry_count 达上限` 的最近一行即锁定证据，
        其 created_at + lock_minutes 就是解锁时间。
        """
        recent = (
            await VerifyCode.filter(
                identifier=identifier,
                channel_type=channel_type,
                state=VerifyCodeState.EXPIRED,
                retry_count__gte=self.max_retries,
            )
            .order_by("-created_at")
            .first()
        )
        if not recent:
            return False
        return now_ms() < recent.created_at + self.lock_minutes * 60 * 1000

    @staticmethod
    def _user_view(user_data: dict) -> dict:
        """对外的用户视图。

        邮箱/手机**脱敏**：前端凭「非空」即可判定是否已绑定，无需拿到完整值。
        ``passwd_set`` 一并下发，前端据此决定是否弹「设置密码」引导。
        """
        from webx.lib.funcs import mask_email, mask_mobile as _mm

        return {
            "user_id": user_data["id"],
            "username": user_data["username"],
            "role": user_data.get("role", "user"),
            "email": mask_email(user_data.get("email", "")),
            "mobile": _mm(user_data.get("mobile", "")),
            "passwd_set": user_data.get("passwd_set", 1),
        }

    @staticmethod
    def _generate_username() -> str:
        """随机用户名：u + 时间戳末 4 位 + 3 位数字 + 2 位字母。"""
        tm = str(int(time.time() * 1000))
        n = secrets.randbelow(900) + 100
        letters = "".join(secrets.choice(string.ascii_lowercase) for _ in range(2))
        return f"u{tm[-4:]}{n}{letters}"

    async def _register_with_retry(
        self, user_service, field: str, identifier: str, max_retries: int = 5
    ) -> dict:
        """注册新用户，用户名随机冲突时自动重试。

        passwd_set=0：随机密码本人不知晓，登录后可经「设置密码」首次设密。
        """
        kwargs = {field: identifier}
        for attempt in range(max_retries):
            username = self._generate_username()
            reg = await user_service.register(
                username, secrets.token_urlsafe(32), passwd_set=0, **kwargs
            )
            if reg["ok"]:
                return reg
            # 只有用户名冲突才值得重试，其他错误（邮箱已注册等）直接返回
            if "用户名已存在" not in reg.get("errmsg", ""):
                return reg
            logger.warning(
                "[验证码] 用户名冲突重试: %s (%d/%d)", username, attempt + 1, max_retries
            )
        return {"ok": False, "errmsg": "注册失败：用户名多次冲突，请稍后再试"}


verifycode_service = VerifyCodeService()
