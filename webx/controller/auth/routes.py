"""认证端点：登录 / 注册 / 登出 / 刷新 / 验证码。

本目录的 ``__mid__.py`` 刻意为空——登录/注册本就是公开的；
需要登录的两个端点（logout / refresh）自带 ``@require_token`` 装饰器。
"""

from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import validate

from webx.models.dtos import (
    AccountChangeByCodeRequest,
    LoginRequest,
    RegisterRequest,
    VerifyCodeCheckRequest,
    VerifyCodeSendRequest,
)
from webx.models import VerifyCodeBizType, VerifyCodeChannel
from webx.lib.senders import get_sender
from webx.services.admin_service import admin_service
from webx.services.user_service import user_service
from webx.services.verifycode_service import vc_status, verifycode_service
from webx.core.guards import require_token, with_client_ip
from webx.i18n import get_lang_from_header, t
from webx.lib.funcs import mask_email, mask_mobile
from webx.lib.regex_rules import EMAIL_RE, MOBILE_RE
from webx.lib.token import get_token

auth_bp = Blueprint("auth", url_prefix="/auth")


# ==================== 账密登录 / 注册 ====================

@auth_bp.post("/login")
@validate(json=LoginRequest)
async def login(request: Request, body: LoginRequest):
    """用户登录（公开）。返回令牌与脱敏后的用户视图。"""
    lang = get_lang_from_header(request.headers)

    result = await user_service.login_verify(body.username, body.password)
    if not result["ok"]:
        return json({"error": result.get("errmsg", "登录失败")}, status=401)

    user_data = result["data"]
    # 令牌里只放最小身份，不含联系方式
    payload = {
        "user_id": user_data["id"],
        "username": user_data["username"],
        "role": user_data.get("role", "user"),
    }
    token = get_token().make_token(payload, id_="user")

    return json(
        {
            "message": t("success", lang),
            "data": {
                "token": token,
                "user": {
                    **payload,
                    # 脱敏：前端凭「非空」判定已绑定，不泄露完整值
                    "email": mask_email(user_data.get("email", "")),
                    "mobile": mask_mobile(user_data.get("mobile", "")),
                    # 前端据此判断是否需弹「设置密码」引导
                    "passwd_set": user_data.get("passwd_set", 1),
                },
            },
        },
        status=200,
    )

@auth_bp.post("/admin-login")
@validate(json=LoginRequest)
async def admin_login(request: Request, body: LoginRequest):
    """管理员登录（公开）。令牌 role 固定为 admin，与 C 端令牌互不通用。"""
    lang = get_lang_from_header(request.headers)

    result = await admin_service.login_verify(body.username, body.password)
    if not result["ok"]:
        return json({"error": result.get("errmsg", "登录失败")}, status=401)

    data = result["data"]
    payload = {
        "user_id": data["id"],
        "username": data["username"],
        "role": "admin",
        "is_root": data.get("is_root", 0),
    }
    token = get_token().make_token(payload, id_="admin")

    return json(
        {"message": t("success", lang), "data": {"token": token, "admin": payload}},
        status=200,
    )

@auth_bp.post("/register")
@validate(json=RegisterRequest)
async def register(request: Request, body: RegisterRequest):
    """账密注册（公开）。

    需先调 ``POST /auth/verifycode``（biz_type=register）向所选渠道发码。
    DTO 已保证 email / mobile 恰好二选一。
    """
    lang = get_lang_from_header(request.headers)

    # ── 1. 渠道与格式 ──
    if body.email:
        if not EMAIL_RE.match(body.email):
            return json({"error": "邮箱格式不正确"}, status=400)
        channel, identifier = VerifyCodeChannel.EMAIL, body.email
    else:
        if get_sender(VerifyCodeChannel.MOBILE) is None:
            return json({"error": "短信服务暂未开通，请使用邮箱注册"}, status=400)
        if not MOBILE_RE.match(body.mobile):
            return json({"error": "手机号格式不正确"}, status=400)
        channel, identifier = VerifyCodeChannel.MOBILE, body.mobile

    # ── 2. 预查重：避免把验证码消费在注定失败的注册上 ──
    # （register 内仍有权威查重兜住并发竞态）
    if await user_service.get_by_username(body.username):
        return json({"error": "用户名已存在"}, status=409)
    dup = (
        await user_service.get_by_email(identifier)
        if channel == VerifyCodeChannel.EMAIL
        else await user_service.get_by_mobile(identifier)
    )
    if dup:
        return json({"error": "该邮箱或手机号已被注册"}, status=409)

    # ── 3. 校验验证码（仅校验、不建号；biz_type 精确匹配防跨场景串用）──
    vc = await verifycode_service.verify_only(
        identifier=identifier,
        code=body.code,
        channel_type=channel,
        biz_type=VerifyCodeBizType.REGISTER,
    )
    if not vc["ok"]:
        return json(
            {"error": vc.get("errmsg", "验证码校验失败")}, status=vc_status(vc)
        )

    # ── 4. 落库建号 ──
    kwargs = {channel: identifier}
    if body.nickname:
        kwargs["nickname"] = body.nickname

    result = await user_service.register(body.username, body.password, **kwargs)
    if not result["ok"]:
        return json({"error": result.get("errmsg", "注册失败")}, status=400)

    u = result["data"]
    return json(
        {
            "message": t("success", lang),
            "data": {
                "user": {
                    "id": u["id"],
                    "username": u["username"],
                    "email": u["email"],
                    "mobile": u["mobile"],
                    "nickname": u["nickname"],
                }
            },
        },
        status=201,
    )

# ==================== 会话 ====================

@auth_bp.post("/logout")
@require_token
async def logout(request: Request):
    """登出（需登录）。

    当前为无状态登出：令牌由客户端删除即可。若需服务端强制失效，
    应引入令牌黑名单（Redis 存 token → 过期时间），本框架不预设该依赖。
    """
    return json({"message": t("success", get_lang_from_header(request.headers))})

@auth_bp.post("/refresh")
@require_token
async def refresh(request: Request):
    """刷新令牌（需有效令牌）。

    管理员刷新时复查 forbid：管理端权限大，被封禁后不该靠旧令牌续命；
    C 端不复查，省一次查库。
    """
    lang = get_lang_from_header(request.headers)
    payload = request.ctx.token_payload

    if payload.get("role") == "admin":
        admin = await admin_service.get_by_id(payload.get("user_id"))
        if not admin:
            return json({"error": "账户不存在"}, status=401)
        if admin.forbid:
            return json({"error": "账户已被禁用"}, status=403)

    new_token = get_token().make_token(payload, id_=payload.get("__tokenId__"))
    return json(
        {"message": t("success", lang), "data": {"token": new_token, "user": payload}}
    )

# ==================== 验证码 ====================

@auth_bp.post("/verifycode")
@with_client_ip
@validate(json=VerifyCodeSendRequest)
async def send_verifycode(request: Request, body: VerifyCodeSendRequest):
    """发送验证码（公开）。

    ``biz_type=bind`` 不在此受理：绑定发码须为登录态（按 user_id 限额），
    走 ``/api/user/{email,mobile}/bind/code``，否则可绕过每用户上限。

    ``biz_type=change``（找回密码）额外有账号存在性前置闸，见下方注释。
    """
    lang = get_lang_from_header(request.headers)

    if body.biz_type == VerifyCodeBizType.BIND:
        return json({"error": "绑定验证码请登录后通过绑定接口获取"}, status=400)

    # change（找回密码）的码只发给已注册账户：先校验格式、再按渠道核对存在性。
    # 未注册直接明确拒绝——既给用户确切反馈，也不向陌生标识白发短信/邮件（那是真金白银）。
    # 仅限 change：register/login 本就要发往未注册标识，加这道闸会把它们打死。
    # 先校验格式再查存在，与 change-by-code 对齐，避免格式非法却误报「未注册」。
    #
    # 「这样会不会泄露账号是否存在」——会，但藏不住也没必要藏：注册端点对重复标识
    # 本就回 409，同一套系统里枚举信息早已可得，在这里含糊其辞只会让找回密码难用。
    # 真正挡住滥用的是限流与熔断（IP 限流 / 单标识冷却 + 每日上限 / 验证码重试熔断 /
    # 登录失败锁定），它们都在 settings 里，那才是该调的旋钮。
    if body.biz_type == VerifyCodeBizType.CHANGE:
        if body.channel_type == VerifyCodeChannel.EMAIL:
            if not EMAIL_RE.match(body.identifier):
                return json({"error": "邮箱格式不正确"}, status=400)
            user = await user_service.get_by_email(body.identifier)
        else:
            if not MOBILE_RE.match(body.identifier):
                return json({"error": "手机号格式不正确"}, status=400)
            user = await user_service.get_by_mobile(body.identifier)
        if not user:
            return json({"error": "该账号未注册"}, status=404)

    result = await verifycode_service.send_code(
        identifier=body.identifier,
        channel_type=body.channel_type,
        biz_type=body.biz_type,
        ip=request.ctx.client_ip,
    )
    if not result["ok"]:
        return json({"error": result["errmsg"]}, status=vc_status(result, default=429))

    return json(
        {"message": t("success", lang), "data": {"expires_in": result["expires_in"]}}
    )

@auth_bp.post("/verifycode-login")
@with_client_ip
@validate(json=VerifyCodeCheckRequest)
async def verifycode_login(request: Request, body: VerifyCodeCheckRequest):
    """验证码登录 / 注册（公开）。

    按落库时的 biz_type 分流：login（不存在则自动注册）/ register（已存在则冲突）
    / change（仅验证身份，不发令牌）。
    """
    lang = get_lang_from_header(request.headers)

    result = await verifycode_service.verify_code(
        identifier=body.identifier,
        code=body.code,
        channel_type=body.channel_type,
        ip=request.ctx.client_ip,
    )
    if not result["ok"]:
        return json(
            {"error": result.get("errmsg", "验证失败")},
            status=vc_status(result, default=400),
        )

    action = result.get("action")
    if action in ("login", "register"):
        return json(
            {
                "message": t("success", lang),
                "data": {
                    "token": result["token"],
                    "user": result["user"],
                    "action": action,
                },
            }
        )

    # change：只回身份信息，不发令牌
    return json(
        {
            "message": t("success", lang),
            "data": {
                "action": "change",
                "identifier": result.get("identifier"),
                "channel_type": result.get("channel_type"),
            },
        }
    )

@auth_bp.post("/account/change-by-code")
@with_client_ip
@validate(json=AccountChangeByCodeRequest)
async def change_account_by_code(request: Request, body: AccountChangeByCodeRequest):
    """凭「邮箱/手机 + 验证码」改名/改密（公开，兼作找回密码）。

    需先调 ``POST /auth/verifycode``（channel_type=email|mobile, biz_type=change）
    向所选渠道发码。DTO 已保证 email / mobile 恰好二选一。
    """
    lang = get_lang_from_header(request.headers)

    # ── 1. 渠道与格式（与注册同款校验）──
    if body.email:
        if not EMAIL_RE.match(body.email):
            return json({"error": "邮箱格式不正确"}, status=400)
        channel, identifier = VerifyCodeChannel.EMAIL, body.email
    else:
        # 短信通道未注册时挡住手机找回，否则用户会卡在"收不到码"
        if get_sender(VerifyCodeChannel.MOBILE) is None:
            return json({"error": "短信服务暂未开通，请使用邮箱找回"}, status=400)
        if not MOBILE_RE.match(body.mobile):
            return json({"error": "手机号格式不正确"}, status=400)
        channel, identifier = VerifyCodeChannel.MOBILE, body.mobile

    # ── 2. 先定位账户：避免把验证码消费在不存在的账户上 ──
    user = (
        await user_service.get_by_email(identifier)
        if channel == VerifyCodeChannel.EMAIL
        else await user_service.get_by_mobile(identifier)
    )
    if not user:
        return json({"error": t("user_not_found", lang)}, status=404)

    # 新用户名查重：与自身相同视为不改名（幂等）
    new_username = body.new_username
    if new_username is not None:
        if new_username == user.username:
            new_username = None
        else:
            existing = await user_service.get_by_username(new_username)
            if existing and existing.id != user.id:
                return json({"error": "用户名已存在"}, status=409)

    # 渠道须与发码时一致，防止「邮箱码拿去改手机账户」这类串用
    vc = await verifycode_service.verify_only(
        identifier=identifier,
        code=body.code,
        channel_type=channel,
        biz_type=VerifyCodeBizType.CHANGE,
    )
    if not vc["ok"]:
        return json(
            {"error": vc.get("errmsg", "验证码校验失败")}, status=vc_status(vc)
        )

    # 边界：若只传了与当前一致的用户名，归一后两者皆 None，
    # update_account 不写库返回 0——视为幂等成功（验证码已消费，不报错）
    await user_service.update_account(
        user.id, username=new_username, new_passwd=body.new_password
    )
    return json({"message": t("success", lang)})
