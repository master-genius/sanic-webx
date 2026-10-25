"""短信适配层：驱动（阿里云）、网关、配置。

全部用 ``httpx.MockTransport`` 或假驱动，**不走真实网络**。因此这里验证的是
「请求构造 / 签名结构 / 响应判定 / 驱动选择」是否正确，不能证明阿里云会接受这份签名——
那一步只有拿真实 AK/SK 联调才能确认。
"""

import hashlib
import hmac
import json
import logging
from urllib.parse import parse_qsl, urlsplit

import httpx
import pytest

from webx.config import BaseSettings, get_settings
from webx.lib.sms import (
    SmsDriver,
    SmsError,
    SmsScene,
    SmsSettingsMixin,
    gateway,
    is_ready,
    register_driver,
    send_scene_sms,
    send_sms,
)
from webx.lib.sms import aliyun
from webx.lib.sms.aliyun import AliyunSmsDriver


@pytest.fixture
def sms_cfg(monkeypatch):
    """把全局配置设为「阿里云已配齐」，用例结束自动还原。"""
    cfg = get_settings()
    for key, value in {
        "SMS_ENABLED": True,
        "SMS_PROVIDER": "aliyun",
        "SMS_ACCESS_KEY_ID": "test-ak",
        "SMS_ACCESS_KEY_SECRET": "test-sk",
        "SMS_SIGN_NAME": "测试签名",
        "SMS_ENDPOINT": "",
        "SMS_DEFAULT_COUNTRY_CODE": "86",
        "SMS_TEMPLATES": {"verify_code": "SMS_100"},
    }.items():
        monkeypatch.setattr(cfg, key, value)
    return cfg


def _driver(handler) -> AliyunSmsDriver:
    return AliyunSmsDriver(transport=httpx.MockTransport(handler))


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"Code": "OK", "BizId": "BIZ1", "RequestId": "REQ1"})


# ==================== 签名（纯函数）====================


def test_canonical_query_sorts_encodes_and_skips_none():
    q = aliyun._canonical_query({"b": "a b", "a": "x~y", "c": None, "d": "中"})
    assert q == "a=x~y&b=a%20b&d=%E4%B8%AD"


def test_authorization_matches_independent_recomputation():
    """按 V3 规范在测试里独立算一遍，与实现逐字对比。"""
    headers = {"Host": "h.example", "x-acs-action": "SendSms", "x-acs-date": "2026-01-01T00:00:00Z"}
    auth, signed = aliyun._build_authorization(
        method="POST", canonical_uri="/", canonical_query="a=1",
        headers=headers, hashed_payload=aliyun._EMPTY_PAYLOAD_SHA256,
        access_key_id="AK", access_key_secret="SK",
    )
    assert signed == "host;x-acs-action;x-acs-date"

    canonical_request = (
        "POST\n/\na=1\n"
        "host:h.example\nx-acs-action:SendSms\nx-acs-date:2026-01-01T00:00:00Z\n"
        "\nhost;x-acs-action;x-acs-date\n" + aliyun._EMPTY_PAYLOAD_SHA256
    )
    sts = "ACS3-HMAC-SHA256\n" + hashlib.sha256(canonical_request.encode()).hexdigest()
    sig = hmac.new(b"SK", sts.encode(), hashlib.sha256).hexdigest()
    assert auth == f"ACS3-HMAC-SHA256 Credential=AK,SignedHeaders={signed},Signature={sig}"


# ==================== 阿里云驱动：请求构造 ====================


async def test_aliyun_request_shape(sms_cfg):
    seen: list[httpx.Request] = []

    def handler(request):
        seen.append(request)
        return _ok(request)

    message_id = await _driver(handler).send(
        "13800138000", "SMS_100", {"code": "123456"}, validity_period=300
    )
    assert message_id == "BIZ1"

    (req,) = seen
    assert req.method == "POST"
    assert req.url.host == "dysmsapi.aliyuncs.com"
    assert req.content == b"", "本接口所有业务参数走 query，请求体必须为空"

    query = dict(parse_qsl(urlsplit(str(req.url)).query))
    assert query == {
        "PhoneNumbers": "8613800138000",   # 裸号在驱动内部补区号
        "SignName": "测试签名",
        "TemplateCode": "SMS_100",
        "TemplateParam": json.dumps({"code": "123456"}, ensure_ascii=False),
    }
    # 上线的查询串必须与参与签名的逐字一致（交给 httpx 重新编码会签名失败）
    assert urlsplit(str(req.url)).query == aliyun._canonical_query(query)

    assert req.headers["x-acs-action"] == "SendSms"
    assert req.headers["x-acs-version"] == "2017-05-25"
    assert req.headers["authorization"].startswith("ACS3-HMAC-SHA256 Credential=test-ak,")
    assert "content-type" not in req.headers


async def test_aliyun_endpoint_override(sms_cfg, monkeypatch):
    monkeypatch.setattr(sms_cfg, "SMS_ENDPOINT", "proxy.internal")
    seen = []

    def handler(request):
        seen.append(request.url.host)
        return _ok(request)

    await _driver(handler).send("13800138000", "SMS_100")
    assert seen == ["proxy.internal"]


@pytest.mark.parametrize(
    "raw, expected",
    [("13800138000", "8613800138000"), ("+8613800138000", "8613800138000"),
     ("8613800138000", "8613800138000"), ("138 0013-8000", "8613800138000")],
)
def test_aliyun_number_normalization(sms_cfg, raw, expected):
    assert aliyun._normalize_to(raw) == expected


async def test_aliyun_refuses_without_credentials(sms_cfg, monkeypatch):
    monkeypatch.setattr(sms_cfg, "SMS_SIGN_NAME", "")
    called = []
    driver = _driver(lambda r: called.append(r) or _ok(r))
    assert driver.is_configured() is False
    with pytest.raises(SmsError) as ei:
        await driver.send("13800138000", "SMS_100")
    assert ei.value.code == "not_configured"
    assert not called, "凭证不全时不得发出请求"


# ==================== 阿里云驱动：响应判定 ====================


async def test_business_rejection_with_http_200_must_raise(sms_cfg):
    """最危险的坑：余额不足等业务失败同样是 HTTP 200，只能靠 Code 判定。"""
    def handler(request):
        return httpx.Response(
            200, json={"Code": "isv.AMOUNT_NOT_ENOUGH", "Message": "余额不足", "RequestId": "R9"}
        )

    with pytest.raises(SmsError) as ei:
        await _driver(handler).send("13800138000", "SMS_100")
    assert ei.value.code == "isv.AMOUNT_NOT_ENOUGH"
    assert ei.value.request_id == "R9"
    assert ei.value.retryable is False, "对着余额不足重试只会白烧钱"


async def test_throttling_is_retryable(sms_cfg):
    def handler(request):
        return httpx.Response(200, json={"Code": "isv.BUSINESS_LIMIT_CONTROL"})

    with pytest.raises(SmsError) as ei:
        await _driver(handler).send("13800138000", "SMS_100")
    assert ei.value.retryable is True


async def test_http_error_without_code_uses_status(sms_cfg):
    with pytest.raises(SmsError) as ei:
        await _driver(lambda r: httpx.Response(500, json={})).send("13800138000", "SMS_100")
    assert ei.value.code == "HTTP_500"


async def test_non_json_response(sms_cfg):
    with pytest.raises(SmsError) as ei:
        await _driver(lambda r: httpx.Response(200, text="<html>")).send("13800138000", "SMS_100")
    assert ei.value.code == "bad_response" and ei.value.retryable


async def test_timeout_and_transport_errors_are_retryable(sms_cfg):
    def timeout(request):
        raise httpx.ConnectTimeout("slow", request=request)

    def refused(request):
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(SmsError) as t:
        await _driver(timeout).send("13800138000", "SMS_100")
    assert t.value.code == "timeout" and t.value.retryable

    with pytest.raises(SmsError) as c:
        await _driver(refused).send("13800138000", "SMS_100")
    assert c.value.code == "transport_error" and c.value.retryable


# ==================== 网关 ====================


class _FakeDriver(SmsDriver):
    name = "fake"
    configured = True

    def __init__(self):
        self.calls = []

    def is_configured(self):
        return self.configured

    async def send(self, to, template_code, params=None, validity_period=None):
        self.calls.append((to, template_code, params, validity_period))
        return "fake-id"


@pytest.fixture
def fake_driver(sms_cfg, monkeypatch):
    """注册一个假驱动并选中它；用例结束把注册表还原。"""
    driver = _FakeDriver()
    register_driver("fake", lambda: driver)
    monkeypatch.setattr(sms_cfg, "SMS_PROVIDER", "fake")
    yield driver
    gateway._FACTORIES.pop("fake", None)
    gateway._INSTANCES.pop("fake", None)


async def test_gateway_routes_scene_to_selected_driver(fake_driver):
    """适配层的核心承诺：选定驱动后，调用方只面对统一接口。"""
    mid = await send_scene_sms(SmsScene.VERIFY_CODE, "13800138000", {"code": "1"}, 300)
    assert mid == "fake-id"
    assert fake_driver.calls == [("13800138000", "SMS_100", {"code": "1"}, 300)]


def test_is_ready_requires_switch_and_driver_configured(fake_driver, sms_cfg, monkeypatch):
    assert is_ready() is True

    fake_driver.configured = False
    assert is_ready() is False, "开关开了但驱动没配齐 = 半启用，不可用"

    fake_driver.configured = True
    monkeypatch.setattr(sms_cfg, "SMS_ENABLED", False)
    assert is_ready() is False


async def test_gateway_error_codes(fake_driver, sms_cfg, monkeypatch):
    monkeypatch.setattr(sms_cfg, "SMS_ENABLED", False)
    with pytest.raises(SmsError) as e1:
        await send_sms("13800138000", "SMS_100")
    assert e1.value.code == "not_enabled"

    monkeypatch.setattr(sms_cfg, "SMS_ENABLED", True)
    fake_driver.configured = False
    with pytest.raises(SmsError) as e2:
        await send_sms("13800138000", "SMS_100")
    assert e2.value.code == "not_configured"

    fake_driver.configured = True
    monkeypatch.setattr(sms_cfg, "SMS_TEMPLATES", {})
    with pytest.raises(SmsError) as e3:
        await send_scene_sms(SmsScene.VERIFY_CODE, "13800138000")
    assert e3.value.code == "template_not_configured"
    assert not fake_driver.calls, "任何前置失败都不得触达驱动"


async def test_unknown_provider_is_unavailable_and_logged_once(sms_cfg, monkeypatch, caplog):
    monkeypatch.setattr(sms_cfg, "SMS_PROVIDER", "nope-provider")
    gateway._WARNED_UNKNOWN.discard("nope-provider")

    with caplog.at_level(logging.ERROR):
        assert is_ready() is False
        assert is_ready() is False
    assert caplog.text.count("nope-provider") == 1, "同一个坏名字只告警一次"

    with pytest.raises(SmsError) as ei:
        await send_sms("13800138000", "SMS_100")
    assert ei.value.code == "unknown_provider"


def test_register_driver_replaces_cached_instance(sms_cfg, monkeypatch):
    monkeypatch.setattr(sms_cfg, "SMS_PROVIDER", "fake2")
    try:
        first, second = _FakeDriver(), _FakeDriver()
        register_driver("fake2", lambda: first)
        assert gateway.get_driver() is first
        register_driver("fake2", lambda: second)
        assert gateway.get_driver() is second
    finally:
        gateway._FACTORIES.pop("fake2", None)
        gateway._INSTANCES.pop("fake2", None)


def test_importing_sms_package_does_not_load_a_provider():
    """包导入不得牵入 httpx 驱动：驱动在首次取用时才延迟加载。"""
    import subprocess
    import sys

    code = (
        "import sys; import webx.lib.sms; "
        "sys.exit(1 if 'webx.lib.sms.aliyun' in sys.modules else 0)"
    )
    assert subprocess.run([sys.executable, "-c", code], cwd=str(__import__('pathlib').Path(__file__).parent.parent)).returncode == 0


# ==================== 配置 ====================


def test_sms_fields_live_in_base_settings(monkeypatch):
    monkeypatch.setenv("SMS_TEMPLATES", '{"verify_code": "SMS_9"}')
    cfg = BaseSettings()
    assert cfg.SMS_TEMPLATES == {"verify_code": "SMS_9"}
    assert cfg.SMS_PROVIDER == "aliyun"


def test_bad_templates_json_falls_back_to_empty(monkeypatch):
    for bad in ("not json", "[1, 2]", ""):
        monkeypatch.setenv("SMS_TEMPLATES", bad)
        assert BaseSettings().SMS_TEMPLATES == {}


def test_legacy_mixin_still_composes_with_base_settings():
    from dataclasses import dataclass

    @dataclass
    class Legacy(SmsSettingsMixin, BaseSettings):
        pass

    assert Legacy().SMS_PROVIDER == "aliyun"
