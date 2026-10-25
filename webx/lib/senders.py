"""验证码发送通道注册表。

两个内置通道都**默认注册**：

- ``email``  走 stdlib `smtplib`，零依赖。
- ``mobile`` 走短信适配层 `webx.lib.sms`（服务商由 ``SMS_PROVIDER`` 选择）。
  注册时带了「是否可用」判断 ``is_ready()``（``SMS_ENABLED`` 且驱动凭证齐备），
  不可用时 ``get_sender("mobile")`` 返回 None，所有手机号入口据此回
  UNSUPPORTED_CHANNEL / 「暂未开通」。所以**配好 .env 即可用，不需要任何接线代码**；
  要关掉就把 ``SMS_ENABLED`` 置 false。

注册表仍然开放：``register_sender`` 可覆盖任一通道（例如换成自己的短信实现，
或加一个新渠道）。不带 ``enabled`` 的注册视为「始终可用」。

sender 契约：``async (identifier, code, *, valid_minutes, biz_type) -> None``，
**失败即抛异常**（不要吞错、不要返回 False）——验证码服务据此把该行烧成 EXPIRED，
避免留下 PENDING 行让用户被自己的冷却期挡住。
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from webx.config import settings
from webx.lib.sms import SmsScene, is_ready, send_scene_sms

logger = logging.getLogger(__name__)

# identifier, code, valid_minutes, biz_type → None（失败抛异常）
Sender = Callable[..., Awaitable[None]]

_SENDERS: dict[str, Sender] = {}
# 渠道 → 「当前是否可用」。没登记的渠道视为始终可用
_ENABLED: dict[str, Callable[[], bool]] = {}


def register_sender(
    channel: str, sender: Sender, *, enabled: Callable[[], bool] | None = None
) -> None:
    """注册某渠道的发送实现（重复注册即覆盖）。

    Args:
        enabled: 返回该渠道当前是否可用。每次 ``get_sender`` 都会求值，因此配置
            变化即时生效。不传 = 始终可用（覆盖注册时会清掉此前登记的判断）。
    """
    _SENDERS[channel] = sender
    if enabled is None:
        _ENABLED.pop(channel, None)
    else:
        _ENABLED[channel] = enabled
    logger.info("[验证码] 注册发送通道: %s", channel)


def get_sender(channel: str) -> Sender | None:
    """取渠道的发送实现；未注册或当前不可用返回 None。

    调用方据此回 UNSUPPORTED_CHANNEL——它是前置闸，必须在限流统计与落库之前判断，
    否则通道没开通时会白写一行验证码、白占该标识的当日额度。
    """
    sender = _SENDERS.get(channel)
    if sender is None:
        return None
    ready = _ENABLED.get(channel)
    if ready is not None and not ready():
        return None
    return sender


def available_channels() -> list[str]:
    return [c for c in _SENDERS if get_sender(c) is not None]


def unregister_sender(channel: str) -> None:
    """注销渠道（测试用，或运行时临时下线某通道）。"""
    _SENDERS.pop(channel, None)
    _ENABLED.pop(channel, None)


# ── 默认邮件通道 ──────────────────────────────────────────────────

_DEFAULT_TEMPLATE = """\
<html>
<body style="font-family: Arial, sans-serif; padding: 20px;">
    <h2 style="color: #333;">{app_name}</h2>
    <p>您好，您的验证码为：</p>
    <p style="font-size: 32px; font-weight: bold; color: #1a73e8; margin: 20px 0;">
        <span style="background: #f5f5f5; padding: 8px 16px; border-radius: 4px;">{code}</span>
    </p>
    <p style="color: #666;">此验证码 {valid_minutes} 分钟内有效，请勿泄露给他人。</p>
    <hr style="border: none; border-top: 1px solid #eee; margin-top: 30px;">
    <p style="color: #999; font-size: 12px;">此邮件为系统自动发送，请勿直接回复。</p>
</body>
</html>
"""


def set_email_template(template: str) -> None:
    """替换验证码邮件模板。

    占位符：``{app_name}`` ``{code}`` ``{valid_minutes}``。
    正文用 HTML 而非纯文本，是为降低被反垃圾系统拦截的概率。
    """
    global _DEFAULT_TEMPLATE
    _DEFAULT_TEMPLATE = template


async def send_email_code(
    identifier: str, code: str, *, valid_minutes: int, biz_type: str = "", **_
) -> None:
    """默认邮件发送实现（stdlib smtplib，零第三方依赖）。"""
    from webx.lib.mail import send_mail

    app_name = settings.APP_DISPLAY_NAME or settings.APP_NAME
    await send_mail(
        to_addr=identifier,
        subject=f"{app_name} 验证码",
        body=_DEFAULT_TEMPLATE.format(
            app_name=app_name, code=code, valid_minutes=valid_minutes
        ),
        content_type="html",
    )


register_sender("email", send_email_code)


# ── 默认短信通道 ──────────────────────────────────────────────────


async def send_sms_code(
    identifier: str, code: str, *, valid_minutes: int, biz_type: str = "", **_
) -> None:
    """默认短信发送实现：经适配层发出，服务商由 SMS_PROVIDER 决定。

    模板契约：服务商控制台报备的模板变量名必须是 ``${code}``，
    且 ``SMS_TEMPLATES`` 里要登记 ``verify_code`` 场景的模板码。
    """
    await send_scene_sms(
        SmsScene.VERIFY_CODE,
        identifier,
        {"code": code},
        # 与我方有效期对齐，避免服务商侧与本系统两套时限不一致
        validity_period=valid_minutes * 60,
    )


register_sender("mobile", send_sms_code, enabled=is_ready)
