"""邮件发送：发件人显示名来自配置，而不是写死的产品名。"""

import smtplib
from email.header import decode_header

import pytest

from webx.lib import mail


class _FakeSMTP:
    sent: list = []

    def __init__(self, *a, **k):
        pass

    def login(self, *a):
        pass

    def sendmail(self, sender, to, raw):
        _FakeSMTP.sent.append((sender, to, raw))

    def quit(self):
        pass


@pytest.fixture
def fake_smtp(monkeypatch):
    _FakeSMTP.sent = []
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    monkeypatch.setattr(smtplib, "SMTP_SSL", _FakeSMTP)
    monkeypatch.setattr(mail.settings, "MAIL_DEFAULT_SENDER", "noreply@example.com", raising=False)
    monkeypatch.setattr(mail.settings, "MAIL_USE_SSL", False, raising=False)
    monkeypatch.setattr(mail.settings, "MAIL_USE_TLS", False, raising=False)
    return _FakeSMTP


def _from_name(raw: str) -> str:
    import email

    msg = email.message_from_string(raw)
    name, _ = email.utils.parseaddr(msg["From"])
    part, enc = decode_header(name)[0]
    return part.decode(enc or "utf-8") if isinstance(part, bytes) else part


def test_sender_name_prefers_display_name_then_app_name(fake_smtp, monkeypatch):
    monkeypatch.setattr(mail.settings, "APP_DISPLAY_NAME", "星河平台", raising=False)
    mail._send_mail_sync("u@example.com", "s", "b")
    assert _from_name(fake_smtp.sent[-1][2]) == "星河平台"

    monkeypatch.setattr(mail.settings, "APP_DISPLAY_NAME", "", raising=False)
    monkeypatch.setattr(mail.settings, "APP_NAME", "myapp", raising=False)
    mail._send_mail_sync("u@example.com", "s", "b")
    assert _from_name(fake_smtp.sent[-1][2]) == "myapp"


def test_no_hardcoded_product_name_in_sender():
    import inspect

    assert "DeepMice" not in inspect.getsource(mail)
