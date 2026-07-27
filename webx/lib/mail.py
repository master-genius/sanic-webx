"""邮件发送工具函数。

使用 Python 内置 smtplib，零依赖。
通过 asyncio.to_thread() 包装，避免阻塞事件循环。

用法:
    from webx.lib.mail import send_mail
    await send_mail("user@example.com", "验证码", "您的验证码是：123456")
"""

import asyncio
import logging
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

from webx.config import settings

logger = logging.getLogger(__name__)


def _send_mail_sync(to_addr: str, subject: str, body: str, content_type: str = "plain") -> None:
    """同步发送邮件（在线程池中执行）。

    Args:
        to_addr: 收件人邮箱
        subject: 邮件主题
        body: 邮件正文
        content_type: 内容类型 ("plain" 或 "html")
    """
    sender = settings.MAIL_DEFAULT_SENDER or settings.MAIL_USERNAME
    if not sender:
        raise ValueError("MAIL_DEFAULT_SENDER 或 MAIL_USERNAME 未配置")

    # 发件人名称使用完整品牌名，避免被反垃圾系统拦截
    from_name = "DeepMice 科学计算平台"
    msg = MIMEText(body, content_type, "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr((from_name, sender))
    msg["To"] = to_addr

    # 两种加密形态**不能混用**，构造的类都不同：
    #   · 隐式 TLS / SMTPS（典型 465，腾讯企业邮 smtp.exmail.qq.com）：
    #     连接建立后立刻 TLS 握手，必须用 SMTP_SSL。若误用明文 SMTP 连 465，
    #     客户端等服务器的 220 欢迎语、服务器等客户端的 TLS ClientHello
    #     → 双方互等 → 连接超时（不是网络问题，是协议不匹配）。
    #   · 显式 TLS / STARTTLS（典型 587）：先明文建连，再用 STARTTLS 升级。
    # SSL 优先级高于 TLS：两者都为 true 时按 SSL 处理并告警（配置自相矛盾）。
    server: smtplib.SMTP | None = None
    try:
        if settings.MAIL_USE_SSL:
            if settings.MAIL_USE_TLS:
                logger.warning(
                    "[邮件] MAIL_USE_SSL 与 MAIL_USE_TLS 同时为 true，"
                    "按 SSL(隐式 TLS) 处理；请检查配置"
                )
            server = smtplib.SMTP_SSL(settings.MAIL_SERVER, settings.MAIL_PORT, timeout=10)
        elif settings.MAIL_USE_TLS:
            server = smtplib.SMTP(settings.MAIL_SERVER, settings.MAIL_PORT, timeout=10)
            server.ehlo()
            server.starttls()
            server.ehlo()          # 升级后须重新 EHLO，能力集会变化
        else:
            server = smtplib.SMTP(settings.MAIL_SERVER, settings.MAIL_PORT, timeout=10)

        server.login(settings.MAIL_USERNAME, settings.MAIL_PASSWORD)
        server.sendmail(sender, [to_addr], msg.as_string())
        logger.info(f"[邮件] 发送成功: {to_addr}, subject={subject}")
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"[邮件] 认证失败: {e}")
        raise
    except smtplib.SMTPException as e:
        logger.error(f"[邮件] 发送失败: {e}")
        raise
    except Exception as e:
        logger.error(f"[邮件] 异常: {e}")
        raise
    finally:
        # quit() 原本在 try 内，中途抛错就漏连接；改由 finally 兜底
        if server is not None:
            try:
                server.quit()
            except Exception:
                pass


async def send_mail(
    to_addr: str,
    subject: str,
    body: str,
    content_type: str = "plain",
) -> None:
    """异步发送邮件（非阻塞）。

    Args:
        to_addr: 收件人邮箱
        subject: 邮件主题
        body: 邮件正文
        content_type: 内容类型 ("plain" 或 "html")

    Raises:
        ValueError: 配置不完整
        smtplib.SMTPException: 发送失败
    """
    if not settings.MAIL_SERVER:
        raise ValueError("MAIL_SERVER 未配置，无法发送邮件")
    if not settings.MAIL_USERNAME or not settings.MAIL_PASSWORD:
        raise ValueError("MAIL_USERNAME 或 MAIL_PASSWORD 未配置，无法发送邮件")

    await asyncio.to_thread(_send_mail_sync, to_addr, subject, body, content_type)
