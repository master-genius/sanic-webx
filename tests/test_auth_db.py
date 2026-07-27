"""用户体系的查库端到端测试（sqlite 内存库，真跑 ORM）。

覆盖：注册/登录/锁定、验证码三层防刷与状态流转、绑定解绑、令牌闭环。
"""

import pytest

from webx.models import (
    User,
    VerifyCode,
    VerifyCodeBizType,
    VerifyCodeChannel,
    VerifyCodeState,
)
from webx.services.user_service import user_service
from webx.services.verifycode_service import vc_status, verifycode_service
from webx.config import settings


# ==================== 注册 / 登录 ====================


async def test_register_and_login(db):
    reg = await user_service.register("alice", "secret123", email="a@example.com")
    assert reg["ok"] is True
    assert "passwd" not in reg["data"], "对外数据绝不能带密码字段"

    login = await user_service.login_verify("alice", "secret123")
    assert login["ok"] is True
    assert login["data"]["username"] == "alice"


async def test_login_by_email_and_mobile(db):
    await user_service.register(
        "bob", "secret123", email="b@example.com", mobile="13800138000"
    )
    # 登录标识按格式自动判定字段，前端不需要区分
    assert (await user_service.login_verify("b@example.com", "secret123"))["ok"]
    assert (await user_service.login_verify("13800138000", "secret123"))["ok"]


async def test_register_rejects_duplicates(db):
    await user_service.register("carol", "secret123", email="c@example.com")

    assert (await user_service.register("carol", "x123456"))["errmsg"] == "用户名已存在"
    dup_email = await user_service.register("carol2", "x123456", email="c@example.com")
    assert dup_email["errmsg"] == "邮箱已被注册"


async def test_login_fails_and_locks(db):
    await user_service.register("dave", "secret123")

    for _ in range(settings.LOGIN_MAX_FAILED):
        result = await user_service.login_verify("dave", "wrong-pass")
        assert result["ok"] is False

    # 达到上限后即便密码正确也应被锁定挡住
    result = await user_service.login_verify("dave", "secret123")
    assert result["ok"] is False
    assert "锁定" in result["errmsg"]


async def test_forbidden_user_cannot_login(db):
    reg = await user_service.register("eve", "secret123")
    await User.filter(id=reg["data"]["id"]).update(forbid=1)

    result = await user_service.login_verify("eve", "secret123")
    assert result["ok"] is False
    assert "禁用" in result["errmsg"]


async def test_passwd_set_zero_blocks_password_login(db):
    """验证码自动注册的用户（随机密码、本人不知晓）不得用密码登录。"""
    await user_service.register("frank", "random-secret", passwd_set=0)
    assert (await user_service.login_verify("frank", "random-secret"))["ok"] is False


# ==================== 验证码 ====================


async def test_send_and_verify_code(db, captured_codes):
    result = await verifycode_service.send_code(
        identifier="user@example.com", channel_type=VerifyCodeChannel.EMAIL
    )
    assert result["ok"] is True
    assert result["expires_in"] == settings.VERIFY_CODE_VALID_MINUTES * 60
    assert len(captured_codes) == 1

    code = captured_codes[0]["code"]
    assert len(code) == 6 and code.isdigit()

    verified = await verifycode_service.verify_only(
        identifier="user@example.com", code=code, channel_type=VerifyCodeChannel.EMAIL
    )
    assert verified["ok"] is True

    row = await VerifyCode.filter(identifier="user@example.com").first()
    assert row.state == VerifyCodeState.VERIFIED, "校验成功后必须消费掉"


async def test_cooldown_blocks_resend(db, captured_codes):
    await verifycode_service.send_code("u2@example.com", VerifyCodeChannel.EMAIL)
    again = await verifycode_service.send_code("u2@example.com", VerifyCodeChannel.EMAIL)

    assert again["ok"] is False
    assert vc_status(again) == 429
    assert len(captured_codes) == 1, "冷却期内不得真的再发一封"


async def test_unregistered_channel_is_rejected_before_writing_row(db):
    """通道未注册时必须在落库【之前】挡住，不能白占额度与 IP 窗口。"""
    result = await verifycode_service.send_code("13800138000", VerifyCodeChannel.MOBILE)

    assert result["ok"] is False
    assert vc_status(result) == 400
    assert await VerifyCode.all().count() == 0, "不该留下任何记录"


async def test_wrong_code_increments_retry_then_locks(db, captured_codes):
    await verifycode_service.send_code("u3@example.com", VerifyCodeChannel.EMAIL)

    for _ in range(settings.VERIFY_CODE_MAX_RETRIES):
        bad = await verifycode_service.verify_only("u3@example.com", "000000")
        assert bad["ok"] is False

    row = await VerifyCode.filter(identifier="u3@example.com").first()
    assert row.state == VerifyCodeState.EXPIRED, "重试超限即熔断"

    # 熔断后进入锁定窗口，连发新码都要被挡
    blocked = await verifycode_service.send_code("u3@example.com", VerifyCodeChannel.EMAIL)
    assert blocked["ok"] is False
    assert vc_status(blocked) == 429


async def test_biz_type_prevents_cross_scene_reuse(db, captured_codes):
    """登录码不能被拿去注册——biz_type 精确匹配。"""
    await verifycode_service.send_code(
        "u4@example.com", VerifyCodeChannel.EMAIL, biz_type=VerifyCodeBizType.LOGIN
    )
    code = captured_codes[0]["code"]

    misused = await verifycode_service.verify_only(
        "u4@example.com", code, biz_type=VerifyCodeBizType.REGISTER
    )
    assert misused["ok"] is False
    assert vc_status(misused) == 404


async def test_send_failure_burns_the_row(db, monkeypatch):
    """投递失败必须把该行烧成 EXPIRED，否则用户被自己的冷却期锁死。"""
    from webx.lib import senders

    async def boom(*a, **kw):
        raise RuntimeError("smtp down")

    monkeypatch.setitem(senders._SENDERS, "email", boom)

    result = await verifycode_service.send_code("u5@example.com", VerifyCodeChannel.EMAIL)
    assert result["ok"] is False
    assert vc_status(result) == 502

    row = await VerifyCode.filter(identifier="u5@example.com").first()
    assert row.state == VerifyCodeState.EXPIRED


async def test_verifycode_login_autoregisters_then_logs_in(db, captured_codes):
    """login 场景：用户不存在则自动注册并发令牌，再来一次则是登录。"""
    await verifycode_service.send_code("new@example.com", VerifyCodeChannel.EMAIL)
    first = await verifycode_service.verify_code(
        "new@example.com", captured_codes[0]["code"]
    )
    assert first["ok"] is True
    assert first["action"] == "register"
    assert first["token"]
    # 脱敏：不回完整邮箱
    assert first["user"]["email"] != "new@example.com"

    captured_codes.clear()
    await verifycode_service.send_code("new@example.com", VerifyCodeChannel.EMAIL)
    second = await verifycode_service.verify_code(
        "new@example.com", captured_codes[0]["code"]
    )
    assert second["action"] == "login"


async def test_verifycode_register_conflicts_when_exists(db, captured_codes):
    await user_service.register("grace", "secret123", email="g@example.com")
    await verifycode_service.send_code(
        "g@example.com", VerifyCodeChannel.EMAIL, biz_type=VerifyCodeBizType.REGISTER
    )
    result = await verifycode_service.verify_code(
        "g@example.com", captured_codes[0]["code"]
    )
    assert result["ok"] is False
    assert vc_status(result) == 409


async def test_daily_limit_enforced(db, captured_codes):
    """每日上限：超出后拒绝，且不再真的投递。"""
    limit = settings.VERIFY_CODE_DAILY_LIMIT_EMAIL

    # 绕开冷却：直接写入当日已发的记录（冷却只看 PENDING 的最新一条）
    from webx.lib.funcs import now_ms
    from webx.lib.genid import gen_id

    now = now_ms()
    for i in range(limit):
        await VerifyCode.create(
            id=gen_id(), code="123456", channel_type=VerifyCodeChannel.EMAIL,
            identifier="limit@example.com", state=VerifyCodeState.VERIFIED,
            created_at=now - (i + 2) * 60_000, expires_at=now,
        )

    result = await verifycode_service.send_code("limit@example.com", VerifyCodeChannel.EMAIL)
    assert result["ok"] is False
    assert vc_status(result) == 429
    assert captured_codes == []


# ==================== 联系方式 ====================


async def test_set_contact_bind_and_unbind(db):
    reg = await user_service.register("henry", "secret123")
    uid = reg["data"]["id"]

    await user_service.set_contact(uid, "email", "h@example.com")
    assert (await user_service.get_by_id(uid)).email == "h@example.com"

    await user_service.set_contact(uid, "email", "")
    assert (await user_service.get_by_id(uid)).email == ""

    with pytest.raises(ValueError):
        await user_service.set_contact(uid, "wechat", "x")


async def test_update_account_is_atomic_and_idempotent(db):
    reg = await user_service.register("ivan", "secret123")
    uid = reg["data"]["id"]

    # 两者皆 None → 不写库，返回 0（幂等成功）
    assert await user_service.update_account(uid) == 0

    await user_service.update_account(uid, username="ivan2", new_passwd="newsecret1")
    user = await user_service.get_by_id(uid)
    assert user.username == "ivan2"
    assert user.passwd_set == 1
    assert user_service.verify_passwd_hash("newsecret1", user.passwd)


async def test_business_failure_does_not_consume_the_code(db, captured_codes):
    """业务失败不该消耗验证码——码仍是 PENDING，用户可以直接重试。

    ``_consume`` 只负责"码对不对"，标记 VERIFIED 由各调用方在业务动作成功后进行。
    这里用"纯注册但账号已存在"触发业务失败。
    """
    await user_service.register("existing", "secret123", email="dup@example.com")
    await verifycode_service.send_code(
        "dup@example.com", VerifyCodeChannel.EMAIL, biz_type=VerifyCodeBizType.REGISTER
    )

    result = await verifycode_service.verify_code(
        "dup@example.com", captured_codes[0]["code"]
    )
    assert result["ok"] is False

    row = await VerifyCode.filter(identifier="dup@example.com").first()
    assert row.state == VerifyCodeState.PENDING, "业务失败不得消耗验证码"
    assert row.retry_count == 0, "码本身是对的，不该计入失败重试"
