"""邮件发送：发件人显示名来自配置，不是写死的品牌名；SSL / STARTTLS / 明文三种连接形态各走各的构造器。"""

import inspect
import smtplib
from email import message_from_string
from email.header import decode_header, make_header

import pytest

from webx.config import get_settings
from webx.lib import mail


class _FakeSMTP:
    instances: list = []

    def __init__(self, host, port, timeout=None):
        self.host, self.port = host, port
        self.sent: list = []
        self.calls: list = []
        _FakeSMTP.instances.append(self)

    def ehlo(self):
        self.calls.append("ehlo")

    def starttls(self):
        self.calls.append("starttls")

    def login(self, user, password):
        self.calls.append("login")

    def sendmail(self, sender, rcpts, raw):
        self.sent.append((sender, rcpts, raw))

    def quit(self):
        self.calls.append("quit")


class _FakeSMTPSSL(_FakeSMTP):
    pass


@pytest.fixture
def smtp(monkeypatch):
    _FakeSMTP.instances.clear()
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    monkeypatch.setattr(smtplib, "SMTP_SSL", _FakeSMTPSSL)
    s = get_settings()
    saved = {k: getattr(s, k) for k in (
        "MAIL_SERVER", "MAIL_PORT", "MAIL_USERNAME", "MAIL_PASSWORD", "MAIL_DEFAULT_SENDER",
        "MAIL_USE_SSL", "MAIL_USE_TLS", "APP_NAME", "APP_DISPLAY_NAME")}
    s.MAIL_SERVER, s.MAIL_PORT, s.MAIL_USERNAME, s.MAIL_PASSWORD = "smtp.test", 465, "bot@test", "pw"
    s.MAIL_DEFAULT_SENDER, s.MAIL_USE_SSL, s.MAIL_USE_TLS = "noreply@test", True, False
    yield s
    for k, v in saved.items():
        setattr(s, k, v)


def _from_name(raw: str) -> str:
    msg = message_from_string(raw)
    name = str(make_header(decode_header(msg["From"].rsplit("<", 1)[0].strip())))
    return name.strip('" ')


def test_sender_display_name_comes_from_config_not_a_hardcoded_brand(smtp):
    smtp.APP_NAME, smtp.APP_DISPLAY_NAME = "acme-api", "Acme 数据平台"
    mail._send_mail_sync("u@x.com", "subj", "body")
    srv = _FakeSMTP.instances[0]
    assert _from_name(srv.sent[0][2]) == "Acme 数据平台"
    assert "DeepMice" not in srv.sent[0][2]


def test_sender_display_name_falls_back_to_app_name(smtp):
    smtp.APP_NAME, smtp.APP_DISPLAY_NAME = "acme-api", ""
    mail._send_mail_sync("u@x.com", "subj", "body")
    assert _from_name(_FakeSMTP.instances[0].sent[0][2]) == "acme-api"


def test_no_hardcoded_product_name_in_the_module_source():
    assert "DeepMice" not in inspect.getsource(mail)


def test_connection_form_follows_ssl_tls_settings(smtp):
    smtp.MAIL_USE_SSL, smtp.MAIL_USE_TLS = True, False
    mail._send_mail_sync("u@x.com", "s", "b")
    assert isinstance(_FakeSMTP.instances[-1], _FakeSMTPSSL)

    smtp.MAIL_USE_SSL, smtp.MAIL_USE_TLS = False, True
    mail._send_mail_sync("u@x.com", "s", "b")
    srv = _FakeSMTP.instances[-1]
    assert not isinstance(srv, _FakeSMTPSSL)
    assert srv.calls[:3] == ["ehlo", "starttls", "ehlo"], "STARTTLS 升级后必须重新 EHLO"

    smtp.MAIL_USE_SSL, smtp.MAIL_USE_TLS = False, False
    mail._send_mail_sync("u@x.com", "s", "b")
    assert "starttls" not in _FakeSMTP.instances[-1].calls
    assert all("quit" in i.calls for i in _FakeSMTP.instances), "每次都必须 quit，不漏连接"


def test_missing_sender_config_raises(smtp):
    smtp.MAIL_DEFAULT_SENDER = smtp.MAIL_USERNAME = ""
    with pytest.raises(ValueError):
        mail._send_mail_sync("u@x.com", "s", "b")
