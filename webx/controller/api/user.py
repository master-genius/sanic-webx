"""个人中心端点：资料、改密改名、联系方式绑定/解绑。

全部要求登录——由本目录的 ``__mid__.py`` 挂 ``auth_middleware`` 统一保证。

绑定/解绑的认证方式刻意不同：
  - **绑定**用验证码——码发往【待绑定的新标识】，以此证明用户拥有它
  - **解绑**用当前密码——待解绑的标识可能已不可达（换号/邮箱失效），
    要求它收码等于把用户永久锁死
身份一律取自令牌的 user_id，不经 URL 传入（用户隔离原则）。
"""

from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import validate

from webx.models.dtos import (
    ContactUnbindRequest,
    EmailBindCodeRequest,
    EmailBindRequest,
    MobileBindCodeRequest,
    MobileBindRequest,
    UserInfoResponse,
    UsernameUpdateRequest,
    UserPasswdUpdateRequest,
)
from webx.models import VerifyCodeBizType, VerifyCodeChannel
from webx.lib.senders import get_sender
from webx.services.user_service import user_service
from webx.services.verifycode_service import vc_status, verifycode_service
from webx.core.i18n import get_lang_from_header, t
from webx.lib.regex_rules import EMAIL_RE, MOBILE_RE


async def _current_user(request: Request):
    """取当前登录用户实体。

    Returns:
        ``(user, None)`` 或 ``(None, error_response)``——调用方直接 return 后者。
    """
    lang = get_lang_from_header(request.headers)
    ctx_user = getattr(request.ctx, "user", None)
    user_id = ctx_user.get("user_id") if ctx_user else None
    if not user_id:
        return None, json({"error": t("unauthorized", lang)}, status=401)

    user = await user_service.get_by_id(user_id)
    if not user:
        # 令牌有效但账号已删：不是 401（令牌没问题），是 404
        return None, json({"error": t("user_not_found", lang)}, status=404)
    return user, None

user_bp = Blueprint("user", url_prefix="/api/user")


# ==================== 资料 ====================

@user_bp.get("/info")
async def get_info(request: Request):
    """当前用户完整资料。"""
    user, err = await _current_user(request)
    if err:
        return err

    info = UserInfoResponse(
        id=user.id,
        username=user.username,
        email=user.email or None,
        mobile=user.mobile or None,
        nickname=user.nickname or None,
        avatar=user.avatar or None,
        sex=user.sex,
        province=user.province or None,
        city=user.city or None,
        country=user.country or None,
        points=user.points,
        total_points=user.total_points,
        level=user.level,
        role=user.role,
        passwd_set=user.passwd_set,
        last_login_time=user.last_login_time or None,
        create_time=user.create_time or None,
    )
    return json(
        {
            "message": t("success", get_lang_from_header(request.headers)),
            "data": info.model_dump(),
        }
    )

@user_bp.put("/passwd")
@validate(json=UserPasswdUpdateRequest)
async def update_passwd(request: Request, body: UserPasswdUpdateRequest):
    """设置 / 修改密码。

    按 ``passwd_set`` 分两种语义：
      - =1（已自设）：必须提交正确的 old_password
      - =0（验证码注册的随机密码）：首次设密，无需 old_password
    两条路径成功后均置 passwd_set=1。
    """
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if user.passwd_set:
        if (
            not body.old_password
            or not user.passwd
            or not user_service.verify_passwd_hash(body.old_password, user.passwd)
        ):
            return json({"error": t("old_password_incorrect", lang)}, status=400)

    await user_service.update_passwd(user.id, body.new_password)
    return json({"message": t("success", lang)})

@user_bp.put("/username")
@validate(json=UsernameUpdateRequest)
async def update_username(request: Request, body: UsernameUpdateRequest):
    """修改用户名（需当前密码验证身份）。"""
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if not user.passwd or not user_service.verify_passwd_hash(
        body.password, user.passwd
    ):
        return json({"error": t("old_password_incorrect", lang)}, status=400)

    # 与当前一致视为无需变更（幂等）
    if body.new_username == user.username:
        return json({"message": t("success", lang)})

    existing = await user_service.get_by_username(body.new_username)
    if existing and existing.id != user.id:
        return json({"error": "用户名已存在"}, status=409)

    await user_service.update_username(user.id, body.new_username)
    return json({"message": t("success", lang)})

# ==================== 邮箱绑定 ====================

@user_bp.post("/email/bind/code")
@validate(json=EmailBindCodeRequest)
async def send_email_bind_code(request: Request, body: EmailBindCodeRequest):
    """向【待绑定的新邮箱】发码（登录态，按 user_id 限每日上限）。"""
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if not EMAIL_RE.match(body.email):
        return json({"error": "邮箱格式不正确"}, status=400)

    # 预查重：被他人占用则不发码，避免向陌生邮箱投递 + 注定失败的绑定
    existing = await user_service.get_by_email(body.email)
    if existing and existing.id != user.id:
        return json({"error": "该邮箱已被其他账户绑定"}, status=409)

    # 传 user_id 启用每用户每日上限；不传 ip，跳过公开端点的 IP 通用限流
    result = await verifycode_service.send_code(
        identifier=body.email,
        channel_type=VerifyCodeChannel.EMAIL,
        biz_type=VerifyCodeBizType.BIND,
        user_id=user.id,
    )
    if not result["ok"]:
        return json({"error": result["errmsg"]}, status=vc_status(result, default=429))

    return json(
        {"message": t("success", lang), "data": {"expires_in": result["expires_in"]}}
    )

@user_bp.post("/email/bind")
@validate(json=EmailBindRequest)
async def bind_email(request: Request, body: EmailBindRequest):
    """绑定 / 换绑邮箱。原有邮箱直接被覆盖。"""
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if not EMAIL_RE.match(body.email):
        return json({"error": "邮箱格式不正确"}, status=400)

    # 已是本人当前邮箱 → 幂等成功，不消费验证码
    if user.email == body.email:
        return json({"message": t("success", lang)})

    existing = await user_service.get_by_email(body.email)
    if existing and existing.id != user.id:
        return json({"error": "该邮箱已被其他账户绑定"}, status=409)

    vc = await verifycode_service.verify_only(
        identifier=body.email,
        code=body.code,
        channel_type=VerifyCodeChannel.EMAIL,
        biz_type=VerifyCodeBizType.BIND,
    )
    if not vc["ok"]:
        return json(
            {"error": vc.get("errmsg", "验证码校验失败")}, status=vc_status(vc)
        )

    await user_service.set_contact(user.id, "email", body.email)
    return json({"message": t("success", lang)})

@user_bp.post("/email/unbind")
@validate(json=ContactUnbindRequest)
async def unbind_email(request: Request, body: ContactUnbindRequest):
    """解绑邮箱（当前密码认证）。已为空亦幂等成功。"""
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if not user.passwd or not user_service.verify_passwd_hash(
        body.password, user.passwd
    ):
        return json({"error": t("old_password_incorrect", lang)}, status=400)

    await user_service.set_contact(user.id, "email", "")
    return json({"message": t("success", lang)})

# ==================== 手机绑定 ====================

@user_bp.post("/mobile/bind/code")
@validate(json=MobileBindCodeRequest)
async def send_mobile_bind_code(request: Request, body: MobileBindCodeRequest):
    """向【待绑定的新手机号】发码（登录态）。短信通道未注册时直接挡住。"""
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if get_sender(VerifyCodeChannel.MOBILE) is None:
        return json({"error": "短信服务暂未开通，暂不支持绑定手机号"}, status=400)
    if not MOBILE_RE.match(body.mobile):
        return json({"error": "手机号格式不正确"}, status=400)

    existing = await user_service.get_by_mobile(body.mobile)
    if existing and existing.id != user.id:
        return json({"error": "该手机号已被其他账户绑定"}, status=409)

    result = await verifycode_service.send_code(
        identifier=body.mobile,
        channel_type=VerifyCodeChannel.MOBILE,
        biz_type=VerifyCodeBizType.BIND,
        user_id=user.id,
    )
    if not result["ok"]:
        return json({"error": result["errmsg"]}, status=vc_status(result, default=429))

    return json(
        {"message": t("success", lang), "data": {"expires_in": result["expires_in"]}}
    )

@user_bp.post("/mobile/bind")
@validate(json=MobileBindRequest)
async def bind_mobile(request: Request, body: MobileBindRequest):
    """绑定 / 换绑手机。"""
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if get_sender(VerifyCodeChannel.MOBILE) is None:
        return json({"error": "短信服务暂未开通，暂不支持绑定手机号"}, status=400)
    if not MOBILE_RE.match(body.mobile):
        return json({"error": "手机号格式不正确"}, status=400)

    if user.mobile == body.mobile:
        return json({"message": t("success", lang)})

    existing = await user_service.get_by_mobile(body.mobile)
    if existing and existing.id != user.id:
        return json({"error": "该手机号已被其他账户绑定"}, status=409)

    vc = await verifycode_service.verify_only(
        identifier=body.mobile,
        code=body.code,
        channel_type=VerifyCodeChannel.MOBILE,
        biz_type=VerifyCodeBizType.BIND,
    )
    if not vc["ok"]:
        return json(
            {"error": vc.get("errmsg", "验证码校验失败")}, status=vc_status(vc)
        )

    await user_service.set_contact(user.id, "mobile", body.mobile)
    return json({"message": t("success", lang)})

@user_bp.post("/mobile/unbind")
@validate(json=ContactUnbindRequest)
async def unbind_mobile(request: Request, body: ContactUnbindRequest):
    """解绑手机（当前密码认证）。

    不经短信链路，故**不受短信开关限制**——否则历史遗留的手机号将永远清不掉。
    """
    lang = get_lang_from_header(request.headers)
    user, err = await _current_user(request)
    if err:
        return err

    if not user.passwd or not user_service.verify_passwd_hash(
        body.password, user.passwd
    ):
        return json({"error": t("old_password_incorrect", lang)}, status=400)

    await user_service.set_contact(user.id, "mobile", "")
    return json({"message": t("success", lang)})
