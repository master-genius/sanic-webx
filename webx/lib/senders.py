"""验证码发送通道注册表。

为什么要这层：邮件走 stdlib（`smtplib`，零依赖，永远可用），短信走
`webx.lib.sms`（阿里云 + httpx，是**可选依赖** `webx[sms]`）。若在验证码
服务里直接 `from webx.lib.sms import ...`，没装 httpx 的项目连 import 都过不去。

所以做成注册表：邮件通道默认注册；短信通道**只在业务显式注册时才存在**。
未注册的通道给出明确的 UNSUPPORTED_CHANNEL 错误，而不是 ImportError。

启用短信（业务项目启动时执行一次）::

    from webx.lib.senders import register_sender
    from webx.lib.sms import SmsScene, send_scene_sms

    async def send_sms_code(identifier, code, *, valid_minutes, **kw):
        await send_scene_sms(
            scene=SmsScene.VERIFY_CODE,
            to=identifier,
            params={"code": code},
            validity_period=valid_minutes * 60,   # 与我方有效期对齐
        )

    register_sender("mobile", send_sms_code)

sender 契约：``async (identifier, code, *, valid_minutes, biz_type) -> None``，
**失败即抛异常**（不要吞错、不要返回 False）——验证码服务据此把该行烧成 EXPIRED，
避免留下 PENDING 行让用户被自己的冷却期挡住。
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from webx.config import settings

logger = logging.getLogger(__name__)

# identifier, code, valid_minutes, biz_type → None（失败抛异常）
Sender = Callable[..., Awaitable[None]]

_SENDERS: dict[str, Sender] = {}


def register_sender(channel: str, sender: Sender) -> None:
    """注册某渠道的发送实现（重复注册即覆盖）。"""
    _SENDERS[channel] = sender
    logger.info("[验证码] 注册发送通道: %s", channel)


def get_sender(channel: str) -> Sender | None:
    """取渠道的发送实现；未注册返回 None（调用方据此回 UNSUPPORTED_CHANNEL）。"""
    return _SENDERS.get(channel)


def available_channels() -> list[str]:
    return list(_SENDERS.keys())


def unregister_sender(channel: str) -> None:
    """注销渠道（测试用，或运行时临时下线某通道）。"""
    _SENDERS.pop(channel, None)


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
