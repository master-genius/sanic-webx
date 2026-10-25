"""HTTP 端到端：真起应用 + 真查库，跑完整的注册 → 登录 → 个人中心链路。

与 test_auth_db（服务层）互补：这里验证的是**中间件链 + DTO 校验 + 路由**这一层，
即"从 HTTP 请求到落库"整条路是否接通。
"""

import pytest


@pytest.fixture(scope="module")
def http_app(app_with_db):
    """复用会话级应用（一个进程只能装配一个，见 conftest.app_with_db）。"""
    return app_with_db


def _register_user(app, captured_codes_module, username, email):
    """走完整的「发码 → 注册」两步，返回注册响应。"""
    _, sent = app.test_client.post(
        "/auth/verifycode",
        json={"identifier": email, "channel_type": "email", "biz_type": "register"},
    )
    assert sent.status == 200

    return app.test_client.post(
        "/auth/register",
        json={
            "username": username,
            "password": "secret123",
            "email": email,
            "code": captured_codes_module[-1]["code"],
        },
    )[1]


# ==================== 注册 / 登录 ====================


def test_register_then_login(http_app, captured_codes_module):
    reg = _register_user(http_app, captured_codes_module, "e2euser", "e2e@example.com")
    assert reg.status == 201
    assert reg.json["data"]["user"]["username"] == "e2euser"

    _, login = http_app.test_client.post(
        "/auth/login", json={"username": "e2euser", "password": "secret123"}
    )
    assert login.status == 200
    assert login.json["data"]["token"]
    # 响应里的邮箱必须是脱敏的
    assert login.json["data"]["user"]["email"] != "e2e@example.com"


def test_register_requires_exactly_one_channel(http_app):
    """DTO 层就该挡住「邮箱手机同填/同空」，返回 400。"""
    _, both = http_app.test_client.post(
        "/auth/register",
        json={
            "username": "x1", "password": "secret123",
            "email": "a@b.c", "mobile": "13800138000", "code": "123456",
        },
    )
    assert both.status == 400

    _, neither = http_app.test_client.post(
        "/auth/register",
        json={"username": "x2", "password": "secret123", "code": "123456"},
    )
    assert neither.status == 400


def test_register_rejects_wrong_code(http_app, captured_codes_module):
    http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": "w@example.com", "channel_type": "email", "biz_type": "register"},
    )
    _, resp = http_app.test_client.post(
        "/auth/register",
        json={
            "username": "wrongcode", "password": "secret123",
            "email": "w@example.com", "code": "000000",
        },
    )
    assert resp.status == 400


def test_login_rejects_bad_password(http_app, captured_codes_module):
    _register_user(http_app, captured_codes_module, "e2euser2", "e2e2@example.com")
    _, resp = http_app.test_client.post(
        "/auth/login", json={"username": "e2euser2", "password": "wrong-pass"}
    )
    assert resp.status == 401


def test_mobile_channel_blocked_when_unconfigured(http_app):
    """短信默认已注册但没配凭证（is_ready 为 False）时，发码端点必须明确拒绝而不是 500。"""
    _, resp = http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": "13800138000", "channel_type": "mobile", "biz_type": "register"},
    )
    assert resp.status == 400


# ==================== 个人中心（需登录）====================


def _login_token(app, captured_codes_module, username, email):
    _register_user(app, captured_codes_module, username, email)
    _, login = app.test_client.post(
        "/auth/login", json={"username": username, "password": "secret123"}
    )
    return login.json["data"]["token"]


def test_profile_requires_auth(http_app):
    _, resp = http_app.test_client.get("/api/user/info")
    assert resp.status == 401


def test_profile_info_and_password_change(http_app, captured_codes_module):
    token = _login_token(http_app, captured_codes_module, "profileuser", "profile@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    _, info = http_app.test_client.get("/api/user/info", headers=headers)
    assert info.status == 200
    assert info.json["data"]["username"] == "profileuser"
    # 完整资料接口返回真实邮箱（本人查看自己的信息，无需脱敏）
    assert info.json["data"]["email"] == "profile@example.com"

    # 旧密码错 → 400
    _, bad = http_app.test_client.put(
        "/api/user/passwd",
        headers=headers,
        json={"old_password": "nope-nope", "new_password": "brandnew1"},
    )
    assert bad.status == 400

    _, ok = http_app.test_client.put(
        "/api/user/passwd",
        headers=headers,
        json={"old_password": "secret123", "new_password": "brandnew1"},
    )
    assert ok.status == 200

    _, relogin = http_app.test_client.post(
        "/auth/login", json={"username": "profileuser", "password": "brandnew1"}
    )
    assert relogin.status == 200


def test_username_change_requires_password(http_app, captured_codes_module):
    token = _login_token(http_app, captured_codes_module, "e2euser3", "e2e3@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    _, bad = http_app.test_client.put(
        "/api/user/username",
        headers=headers,
        json={"password": "wrong-pass", "new_username": "renamed"},
    )
    assert bad.status == 400

    _, ok = http_app.test_client.put(
        "/api/user/username",
        headers=headers,
        json={"password": "secret123", "new_username": "renamed"},
    )
    assert ok.status == 200


def test_logout_and_refresh(http_app, captured_codes_module):
    token = _login_token(http_app, captured_codes_module, "e2euser4", "e2e4@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    _, out = http_app.test_client.post("/auth/logout", headers=headers)
    assert out.status == 200

    _, refreshed = http_app.test_client.post("/auth/refresh", headers=headers)
    assert refreshed.status == 200
    assert refreshed.json["data"]["token"]


# ---- /auth/refresh 复查账户状态 ----
# 令牌是无状态的，refresh 是服务端唯一能拦住「被封禁账号无限续命」的位置。
# HTTP 夹具是同步的、不能在测试里直接跑 ORM，故用 stdlib sqlite3 直接改库。


def _e2e_db(app):
    import sqlite3

    return sqlite3.connect(app.ctx.sqlite_path)


def _refresh(app, token):
    return app.test_client.post(
        "/auth/refresh", headers={"Authorization": f"Bearer {token}"}
    )[1]


def test_refresh_rejects_forbidden_user(http_app, captured_codes_module):
    token = _login_token(http_app, captured_codes_module, "banned1", "banned1@example.com")
    assert _refresh(http_app, token).status == 200  # 封禁前正常

    with _e2e_db(http_app) as db:
        db.execute("UPDATE user SET forbid=1 WHERE username='banned1'")

    resp = _refresh(http_app, token)  # 令牌本身仍未过期，但不能再续命
    assert resp.status == 403
    assert "token" not in (resp.json.get("data") or {})


def test_refresh_rejects_deleted_user(http_app, captured_codes_module):
    token = _login_token(http_app, captured_codes_module, "gone1", "gone1@example.com")
    with _e2e_db(http_app) as db:
        db.execute("DELETE FROM user WHERE username='gone1'")
    assert _refresh(http_app, token).status == 401


def test_refresh_admin_branch_still_checks_account(http_app):
    """admin 令牌走 Admin 表：不存在 → 401，被封 → 403，正常 → 200。"""
    from webx.lib.token import get_token

    def admin_token(uid):
        return get_token().make_token(
            {"user_id": uid, "username": uid, "role": "admin"}, id_="admin"
        )

    assert _refresh(http_app, admin_token("adm_none")).status == 401

    cols = (
        "id,username,mobile,mobile_state,email,email_state,realname,sex,info,passwd,"
        "create_time,update_time,failed_count,failed_time,forbid,is_root,openid,unionid,extra"
    )
    with _e2e_db(http_app) as db:
        for uid, forbid in (("adm_ok", 0), ("adm_ban", 1)):
            db.execute(
                f"INSERT INTO superadmin ({cols}) VALUES "
                f"(?,?,'',0,'',0,'',0,'','x',0,0,0,0,?,0,'','','{{}}')",
                (uid, uid, forbid),
            )

    assert _refresh(http_app, admin_token("adm_ok")).status == 200
    assert _refresh(http_app, admin_token("adm_ban")).status == 403


# ==================== 管理端 ====================


def test_admin_endpoints_reject_user_token(http_app, captured_codes_module):
    """C 端令牌打不开管理端——admin_auth_middleware 要求 role == admin。"""
    token = _login_token(http_app, captured_codes_module, "e2euser5", "e2e5@example.com")
    _, resp = http_app.test_client.get(
        "/api/admin/account/", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status == 403


def test_admin_endpoints_reject_anonymous(http_app):
    _, resp = http_app.test_client.get("/api/admin/role/")
    assert resp.status == 401


def test_admin_login_rejects_unknown(http_app):
    """不存在的管理员必须 401（且不泄露"用户不存在"以外的信息）。"""
    _, resp = http_app.test_client.post(
        "/auth/admin-login", json={"username": "nobody", "password": "secret123"}
    )
    assert resp.status == 401


# ==================== 文件上传 / 下载 ====================


def _multipart(filename: str, content: bytes) -> tuple[bytes, dict]:
    """手工拼一个最小 multipart 体——测试要控制原始字节，不走高层封装。"""
    boundary = "----webxtestboundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    return body, {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def _auth(app, codes, username, email) -> dict:
    return {"Authorization": f"Bearer {_login_token(app, codes, username, email)}"}


def test_file_upload_download_roundtrip(http_app, captured_codes_module):
    headers = _auth(http_app, captured_codes_module, "fileuser", "file@example.com")
    content = b"hello-webx-storage" * 100
    body, ct = _multipart("demo.bin", content)

    _, up = http_app.test_client.post(
        "/api/files/bucket1", data=body, headers={**headers, **ct}
    )
    assert up.status == 201, up.text
    assert up.json["data"]["filename"] == "demo.bin"
    assert up.json["data"]["size"] == len(content)

    _, down = http_app.test_client.get("/api/files/bucket1/demo.bin", headers=headers)
    assert down.status == 200
    assert down.body == content
    assert down.headers["Accept-Ranges"] == "bytes"

    _, listing = http_app.test_client.get("/api/files/bucket1", headers=headers)
    assert listing.json["data"]["files"] == ["demo.bin"]


def test_file_endpoints_require_auth(http_app):
    _, resp = http_app.test_client.get("/api/files/bucket1")
    assert resp.status == 401


def test_download_supports_range(http_app, captured_codes_module):
    """下载断点续传：206 + Content-Range，且内容确为该区间。"""
    headers = _auth(http_app, captured_codes_module, "rangeuser", "range@example.com")
    content = bytes(range(256))
    body, ct = _multipart("bytes.bin", content)
    http_app.test_client.post("/api/files/rng", data=body, headers={**headers, **ct})

    _, resp = http_app.test_client.get(
        "/api/files/rng/bytes.bin", headers={**headers, "Range": "bytes=10-19"}
    )
    assert resp.status == 206
    assert resp.body == content[10:20]
    assert "bytes 10-19/256" in resp.headers["Content-Range"]


def test_download_range_beyond_eof_is_416(http_app, captured_codes_module):
    headers = _auth(http_app, captured_codes_module, "r416user", "r416@example.com")
    body, ct = _multipart("small.bin", b"0123456789")
    http_app.test_client.post("/api/files/r416", data=body, headers={**headers, **ct})

    _, resp = http_app.test_client.get(
        "/api/files/r416/small.bin", headers={**headers, "Range": "bytes=999-"}
    )
    assert resp.status == 416
    assert resp.headers["Content-Range"] == "bytes */10"


def test_signed_public_download(http_app, captured_codes_module):
    """签发限时链接 → 无需登录即可下载。"""
    headers = _auth(http_app, captured_codes_module, "signuser", "sign@example.com")
    content = b"public-content"
    body, ct = _multipart("pub.txt", content)
    http_app.test_client.post("/api/files/pub", data=body, headers={**headers, **ct})

    _, signed = http_app.test_client.post(
        "/api/files/pub/pub.txt/sign", json={"ttl_seconds": 600}, headers=headers
    )
    assert signed.status == 200
    sign = signed.json["data"]["sign"]

    _, down = http_app.test_client.get(f"/public/files/pub/pub.txt?sign={sign}")
    assert down.status == 200
    assert down.body == content


def test_public_download_rejects_bad_or_missing_sign(http_app, captured_codes_module):
    """签名无效一律 404——不能用 403 泄露「该文件存在」。"""
    headers = _auth(http_app, captured_codes_module, "badsign", "badsign@example.com")
    body, ct = _multipart("secret.txt", b"x")
    http_app.test_client.post("/api/files/sec", data=body, headers={**headers, **ct})

    _, no_sign = http_app.test_client.get("/public/files/sec/secret.txt")
    assert no_sign.status == 404

    _, bad_sign = http_app.test_client.get("/public/files/sec/secret.txt?sign=garbage")
    assert bad_sign.status == 404


def test_signed_url_cannot_be_moved_to_another_file(http_app, captured_codes_module):
    headers = _auth(http_app, captured_codes_module, "mover", "mover@example.com")
    for name in ("mine.txt", "yours.txt"):
        body, ct = _multipart(name, b"data")
        http_app.test_client.post("/api/files/mv", data=body, headers={**headers, **ct})

    _, signed = http_app.test_client.post("/api/files/mv/mine.txt/sign", headers=headers)
    sign = signed.json["data"]["sign"]

    _, moved = http_app.test_client.get(f"/public/files/mv/yours.txt?sign={sign}")
    assert moved.status == 404, "签名必须与对象键绑定"


def test_chunked_upload_over_http(http_app, captured_codes_module):
    """分片上传全链路：init → 两片 → complete → 下载校验。"""
    headers = _auth(http_app, captured_codes_module, "chunker", "chunk@example.com")

    _, init = http_app.test_client.post(
        "/api/files/big/uploads", json={"filename": "big.bin", "total": 8}, headers=headers
    )
    assert init.status == 201
    uid = init.json["data"]["upload_id"]

    _, p1 = http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"abcd",
        headers={**headers, "Upload-Offset": "0"},
    )
    assert p1.json["data"]["offset"] == 4

    _, status = http_app.test_client.get(f"/api/files/uploads/{uid}", headers=headers)
    assert status.json["data"]["offset"] == 4

    _, p2 = http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"efgh",
        headers={**headers, "Upload-Offset": "4"},
    )
    assert p2.json["data"]["offset"] == 8

    _, done = http_app.test_client.post(
        f"/api/files/uploads/{uid}/complete", headers=headers
    )
    assert done.status == 200
    assert done.json["data"]["key"] == "big/big.bin"

    _, down = http_app.test_client.get("/api/files/big/big.bin", headers=headers)
    assert down.body == b"abcdefgh"


def test_chunked_offset_conflict_returns_real_offset(http_app, captured_codes_module):
    headers = _auth(http_app, captured_codes_module, "conflict", "conflict@example.com")
    _, init = http_app.test_client.post(
        "/api/files/cf/uploads", json={"filename": "c.bin"}, headers=headers
    )
    uid = init.json["data"]["upload_id"]
    http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"abc", headers={**headers, "Upload-Offset": "0"}
    )

    _, dup = http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"abc", headers={**headers, "Upload-Offset": "0"}
    )
    assert dup.status == 409
    assert dup.json["offset"] == 3, "要把真实 offset 回给客户端以便重新对齐"


def test_delete_file(http_app, captured_codes_module):
    headers = _auth(http_app, captured_codes_module, "deleter", "del@example.com")
    body, ct = _multipart("gone.txt", b"x")
    http_app.test_client.post("/api/files/del", data=body, headers={**headers, **ct})

    _, resp = http_app.test_client.delete("/api/files/del/gone.txt", headers=headers)
    assert resp.status == 200

    _, again = http_app.test_client.delete("/api/files/del/gone.txt", headers=headers)
    assert again.status == 404


def test_upload_rejects_path_traversal_filename(http_app, captured_codes_module):
    """恶意文件名必须被存储层挡住，不能写到 bucket 之外。"""
    headers = _auth(http_app, captured_codes_module, "traverse", "trav@example.com")
    body, ct = _multipart("../../escaped.txt", b"pwned")

    _, resp = http_app.test_client.post(
        "/api/files/tv", data=body, headers={**headers, **ct}
    )
    assert resp.status in (400, 403), resp.text


# ==================== 找回密码 / 改名改密（邮箱 or 手机）====================


def test_change_by_code_via_email(http_app, captured_codes_module):
    """邮箱渠道：发 change 码 → 凭码改密 → 新密码可登录。"""
    _register_user(http_app, captured_codes_module, "chguser", "chg@example.com")

    http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": "chg@example.com", "channel_type": "email", "biz_type": "change"},
    )
    _, resp = http_app.test_client.post(
        "/auth/account/change-by-code",
        json={
            "email": "chg@example.com",
            "code": captured_codes_module[-1]["code"],
            "new_password": "changed123",
        },
    )
    assert resp.status == 200, resp.text

    _, login = http_app.test_client.post(
        "/auth/login", json={"username": "chguser", "password": "changed123"}
    )
    assert login.status == 200


def test_change_by_code_requires_exactly_one_channel(http_app):
    """邮箱手机同填/同空都该被 DTO 挡在 400。"""
    _, both = http_app.test_client.post(
        "/auth/account/change-by-code",
        json={
            "email": "a@b.c", "mobile": "13800138000",
            "code": "123456", "new_password": "whatever1",
        },
    )
    assert both.status == 400

    _, neither = http_app.test_client.post(
        "/auth/account/change-by-code",
        json={"code": "123456", "new_password": "whatever1"},
    )
    assert neither.status == 400


def test_change_by_code_requires_something_to_change(http_app):
    """只给身份不给新值，等于什么都没让改。"""
    _, resp = http_app.test_client.post(
        "/auth/account/change-by-code",
        json={"email": "a@b.c", "code": "123456"},
    )
    assert resp.status == 400


def test_change_by_code_mobile_blocked_without_sms(http_app):
    """短信未配置时，手机找回要给明确提示而不是让用户干等验证码。"""
    _, resp = http_app.test_client.post(
        "/auth/account/change-by-code",
        json={"mobile": "13800138000", "code": "123456", "new_password": "whatever1"},
    )
    assert resp.status == 400
    assert "短信" in resp.json["error"]


def test_change_by_code_unknown_account_is_404(http_app):
    _, resp = http_app.test_client.post(
        "/auth/account/change-by-code",
        json={"email": "nobody@example.com", "code": "123456", "new_password": "whatever1"},
    )
    assert resp.status == 404


def test_mobile_register_and_change_by_code(http_app, fake_sms):
    """手机渠道全链路：注册 → 找回改密 → 新密码登录。

    走默认注册的 mobile 通道 + 适配层，只把"服务商"换成假驱动：
    不手工注册任何 sender，证明配好 .env 即可用。
    """
    sent = fake_sms.sent
    mobile = "13800138001"

    # ① 手机注册
    http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": mobile, "channel_type": "mobile", "biz_type": "register"},
    )
    assert sent, "短信驱动应已收到注册验证码"
    assert sent[-1]["to"] == mobile, "驱动拿到的是裸号"
    assert sent[-1]["template"] == "SMS_TEST", "模板码来自 SMS_TEMPLATES 的 verify_code 场景"

    _, reg = http_app.test_client.post(
        "/auth/register",
        json={
            "username": "mobuser",
            "password": "secret123",
            "mobile": mobile,
            "code": sent[-1]["params"]["code"],
        },
    )
    assert reg.status == 201, reg.text
    assert reg.json["data"]["user"]["mobile"] == mobile

    # ② 凭手机验证码改密（找回密码）
    http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": mobile, "channel_type": "mobile", "biz_type": "change"},
    )
    _, resp = http_app.test_client.post(
        "/auth/account/change-by-code",
        json={"mobile": mobile, "code": sent[-1]["params"]["code"], "new_password": "bymobile1"},
    )
    assert resp.status == 200, resp.text

    # ③ 新密码可登录，且手机号本身也能当登录标识
    _, login = http_app.test_client.post(
        "/auth/login", json={"username": mobile, "password": "bymobile1"}
    )
    assert login.status == 200


def _verify_code_rows(app, identifier):
    with _e2e_db(app) as db:
        return db.execute(
            "SELECT COUNT(*) FROM verify_code WHERE identifier=?", (identifier,)
        ).fetchone()[0]


def test_sms_not_ready_is_blocked_before_any_db_write(http_app, fake_sms):
    """开关开着但驱动没配齐 = 半启用：必须在落库之前挡住，不白占当日额度。"""
    fake_sms.configured = False
    mobile = "13800138010"

    _, resp = http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": mobile, "channel_type": "mobile", "biz_type": "register"},
    )
    assert resp.status == 400
    assert not fake_sms.sent
    assert _verify_code_rows(http_app, mobile) == 0


def test_sms_switch_off_blocks_even_when_driver_configured(http_app, fake_sms, monkeypatch):
    from webx.config import get_settings

    monkeypatch.setattr(get_settings(), "SMS_ENABLED", False)
    mobile = "13800138011"
    _, resp = http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": mobile, "channel_type": "mobile", "biz_type": "register"},
    )
    assert resp.status == 400
    assert not fake_sms.sent and _verify_code_rows(http_app, mobile) == 0


def test_sms_provider_failure_is_502_and_burns_the_code(http_app, fake_sms):
    """服务商发送失败：回 502，且该行验证码被烧成 EXPIRED，用户不会被自己的冷却期挡住。"""
    from webx.lib.sms import SmsError

    fake_sms.fail = SmsError("余额不足", code="isv.AMOUNT_NOT_ENOUGH")
    mobile = "13800138012"
    _, resp = http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": mobile, "channel_type": "mobile", "biz_type": "register"},
    )
    assert resp.status == 502

    with _e2e_db(http_app) as db:
        states = [r[0] for r in db.execute(
            "SELECT state FROM verify_code WHERE identifier=?", (mobile,))]
    assert states == ["expired"]


def test_bind_mobile_endpoints_follow_sms_readiness(http_app, captured_codes_module, fake_sms):
    """登录态绑定手机：未就绪时被挡，就绪后能发码（同一个 get_sender 闸）。"""
    headers = _auth(http_app, captured_codes_module, "binder1", "binder1@example.com")

    fake_sms.configured = False
    _, blocked = http_app.test_client.post(
        "/api/user/mobile/bind/code", json={"mobile": "13800138020"}, headers=headers
    )
    assert blocked.status == 400 and not fake_sms.sent

    fake_sms.configured = True
    _, ok = http_app.test_client.post(
        "/api/user/mobile/bind/code", json={"mobile": "13800138020"}, headers=headers
    )
    assert ok.status == 200, ok.text
    assert fake_sms.sent[-1]["to"] == "13800138020"


# ==================== 找回密码发码：账号存在性前置闸 ====================


def test_change_code_rejects_unregistered_account(http_app, captured_codes_module):
    """未注册的标识不该收到找回码——也不该白发一封邮件出去。"""
    before = len(captured_codes_module)

    _, resp = http_app.test_client.post(
        "/auth/verifycode",
        json={
            "identifier": "never-registered@example.com",
            "channel_type": "email",
            "biz_type": "change",
        },
    )
    assert resp.status == 404
    assert "未注册" in resp.json["error"]
    assert len(captured_codes_module) == before, "不得向未注册标识投递验证码"


def test_change_code_reports_format_error_before_existence(http_app):
    """格式非法要报格式错，不能误报「未注册」——否则用户被误导去注册。"""
    _, resp = http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": "not-an-email", "channel_type": "email", "biz_type": "change"},
    )
    assert resp.status == 400
    assert "格式" in resp.json["error"]


def test_change_code_passes_for_registered_account(http_app, captured_codes_module):
    _register_user(http_app, captured_codes_module, "gateuser", "gate@example.com")
    before = len(captured_codes_module)

    _, resp = http_app.test_client.post(
        "/auth/verifycode",
        json={"identifier": "gate@example.com", "channel_type": "email", "biz_type": "change"},
    )
    assert resp.status == 200
    assert len(captured_codes_module) == before + 1


def test_register_and_login_codes_still_reach_unregistered(http_app, captured_codes_module):
    """这道闸**只**管 change：注册/登录码本来就得发往未注册标识，不能被误伤。"""
    for biz in ("register", "login"):
        before = len(captured_codes_module)
        _, resp = http_app.test_client.post(
            "/auth/verifycode",
            json={
                "identifier": f"fresh-{biz}@example.com",
                "channel_type": "email",
                "biz_type": biz,
            },
        )
        assert resp.status == 200, f"{biz} 被误伤: {resp.text}"
        assert len(captured_codes_module) == before + 1


# ==================== 存储契约：Upload-Offset 头 / 分片限制 / HEAD / 表单 filename ====================


@pytest.fixture(scope="module")
def sh(http_app, captured_codes_module):
    """本组共用的已登录请求头。

    每个用例各注册一个用户会各发一次验证码，整个测试会话很快就撞上
    IP 限流（IP_RATE_LIMIT_PER_MIN）——那是被测功能本身在工作，不是缺陷。
    """
    return _auth(http_app, captured_codes_module, "storagecontract", "storagecontract@example.com")


def _new_session(app, headers, bucket, filename="x.bin", total=-1):
    _, init = app.test_client.post(
        f"/api/files/{bucket}/uploads", json={"filename": filename, "total": total}, headers=headers
    )
    assert init.status == 201, init.text
    return init.json["data"]["upload_id"]


def test_chunk_responses_carry_upload_offset_header(http_app, sh):
    """断点续传客户端读 Upload-Offset 头；JSON 客户端读顶层 offset。两处必须一致。"""
    headers = sh
    uid = _new_session(http_app, headers, "oh")

    _, p1 = http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"abcd", headers={**headers, "Upload-Offset": "0"}
    )
    assert p1.headers["Upload-Offset"] == "4" and p1.json["offset"] == 4

    _, st = http_app.test_client.get(f"/api/files/uploads/{uid}", headers=headers)
    assert st.headers["Upload-Offset"] == "4" and st.json["offset"] == 4

    _, bad = http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"zz", headers={**headers, "Upload-Offset": "99"}
    )
    assert bad.status == 409
    assert bad.headers["Upload-Offset"] == "4", "409 必须用头带回真实 offset"
    assert bad.json["offset"] == 4


def test_empty_chunk_is_rejected(http_app, sh):
    headers = sh
    uid = _new_session(http_app, headers, "ec")
    _, resp = http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"", headers={**headers, "Upload-Offset": "0"}
    )
    assert resp.status == 400


def test_oversized_chunk_is_413_and_session_offset_stays_truthful(http_app, sh, monkeypatch):
    from webx.config import get_settings

    monkeypatch.setattr(get_settings(), "UPLOAD_CHUNK_MAX_BYTES", 8)
    headers = sh
    uid = _new_session(http_app, headers, "bc")

    _, resp = http_app.test_client.patch(
        f"/api/files/uploads/{uid}", data=b"x" * 64, headers={**headers, "Upload-Offset": "0"}
    )
    assert resp.status == 413
    _, st = http_app.test_client.get(f"/api/files/uploads/{uid}", headers=headers)
    assert st.json["offset"] == 0, "被拒的分片不得推进 offset"


def test_head_reports_length_and_range_support(http_app, sh):
    headers = sh
    body, ct = _multipart("h.bin", b"0123456789")
    http_app.test_client.post("/api/files/hd", data=body, headers={**headers, **ct})

    _, head = http_app.test_client.head("/api/files/hd/h.bin", headers=headers)
    assert head.status == 200
    assert head.headers["Content-Length"] == "10"
    assert head.headers["Accept-Ranges"] == "bytes"
    assert head.body in (b"", None)

    _, missing = http_app.test_client.head("/api/files/hd/none.bin", headers=headers)
    assert missing.status == 404

    _, signed = http_app.test_client.post("/api/files/hd/h.bin/sign", headers=headers)
    sign = signed.json["data"]["sign"]
    _, pub = http_app.test_client.head(f"/public/files/hd/h.bin?sign={sign}")
    assert pub.status == 200 and pub.headers["Content-Length"] == "10"
    _, bad = http_app.test_client.head("/public/files/hd/h.bin?sign=garbage")
    assert bad.status == 404


def test_form_filename_field_overrides_file_part_name(http_app, sh):
    headers = sh
    boundary = "----fnb"
    body = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"filename\"\r\n\r\nrenamed.txt\r\n"
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"orig.txt\"\r\n"
        f"Content-Type: text/plain\r\n\r\nhello\r\n--{boundary}--\r\n"
    ).encode()
    _, resp = http_app.test_client.post(
        "/api/files/fn", data=body,
        headers={**headers, "Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    assert resp.status == 201, resp.text
    assert resp.json["data"]["filename"] == "renamed.txt"


def test_reserved_dot_names_cannot_be_uploaded(http_app, sh):
    """点开头是存储保留区：不能借上传写进 .uploads 这类内部目录。"""
    headers = sh
    body, ct = _multipart(".hidden", b"x")
    _, resp = http_app.test_client.post("/api/files/dt", data=body, headers={**headers, **ct})
    assert resp.status == 400
    body, ct = _multipart("ok.txt", b"x")
    _, resp = http_app.test_client.post("/api/files/.uploads", data=body, headers={**headers, **ct})
    assert resp.status == 400
