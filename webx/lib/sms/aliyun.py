"""阿里云国内短信驱动（dysmsapi 2017-05-25，httpx 直连，零新依赖）。

为什么不用官方 SDK
------------------
`alibabacloud-dysmsapi20180501` 会拖入 25 个传递依赖（aiohttp / requests / urllib3
/ cryptography / apscheduler / websocket-client …），与项目一贯的依赖克制不符
（storage_server 亦刻意走 libcrypto 而非 cryptography）。本模块用 httpx（项目既有）
+ stdlib 的 hmac/hashlib/urllib.parse 直连，签名逻辑约 30 行。

签名算法出处（**逐行移植自 SDK 源码，非文档记忆**）
--------------------------------------------------
    call_api 分派      alibabacloud_tea_openapi/client.py:2061
        signature_version / signature_algorithm 均不设 → 走 do_request
        → V3 `ACS3-HMAC-SHA256`（do_rpcrequest 的 V1 HMAC-SHA1 是遗留路径，不会走到）
    签名构造           alibabacloud_openapi_util/client.py: get_authorization
    请求参数           阿里云国内短信 SendSms:
        action=SendSms, version=2017-05-25, style=RPC,
        method=POST, pathname=/, auth_type=AK

    ★ 关键：本接口所有业务参数走 **query string**，请求体为空
      （dysmsapi 只传了 OpenApiRequest(query=...)，未传 body），
      故 hashed_payload 恒为空串的 SHA256，且不设 content-type。

设计要点
--------
3. **`Code` 必须显式校验**（最危险的坑）。阿里云的业务失败——余额不足 /
   模板未审核 / 签名不匹配 / 号码黑名单——同样返回 **HTTP 200**，只在响应体里给
   一个非 "OK" 的 `Code`。不判这一项 = 接口报成功、DB 行是 PENDING、
   用户永远收不到短信且日志无痕。

4. **双超时**（沿用原项目的既有教训）：httpx 的 read/write
   是**空闲超时**（块间间隔）而非总时长，上游每 N 秒挤一个字节就能让请求永远挂着；
   故另用 `asyncio.timeout` 包一层总时长硬上限。
"""

import asyncio
import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import httpx

from webx.config import settings
from webx.lib.funcs import mask_mobile
from webx.lib.sms.base import SmsDriver, SmsError

logger = logging.getLogger(__name__)


# ==================== 协议常量 ====================

_ALGORITHM = "ACS3-HMAC-SHA256"
_API_VERSION = "2017-05-25"
_ACTION = "SendSms"
# 本接口请求体恒为空 → payload 哈希是常量，无需每次计算
_EMPTY_PAYLOAD_SHA256 = hashlib.sha256(b"").hexdigest()

# 阿里云国内短信标准接入地址。SMS_ENDPOINT 仅保留作私网代理/联调覆盖。
_DEFAULT_ENDPOINT = "dysmsapi.aliyuncs.com"

_SUCCESS_CODE = "OK"

# 可重试的响应码（限流 / 系统繁忙一类的瞬时故障）。
# ⚠️ 尽力而为的分类：未知响应码一律按【不可重试】处理——短信是花钱的，
#    对着「模板不存在 / 余额不足」重试只会白烧钱且永远不会变好。
#    本标志当前无人消费（验证码服务不重试），是留给未来调用方的元数据。
_RETRYABLE_CODES = frozenset({
    "isv.BUSINESS_LIMIT_CONTROL",   # 触发运营商流控
    "isv.SYSTEM_BUSY",
    "SystemError",
    "ServiceUnavailable",
    "Throttling",
    "Throttling.User",
    "Throttling.Api",
})



# ==================== 签名（纯函数，可单测 / 可与 SDK 做差分验证）====================


def _canonical_query(params: dict[str, object]) -> str:
    """构造 V3 规范化查询串：键升序，键值均按 RFC3986 编码（保留 `~`）。

    值为 None 的参数整项跳过（与 SDK get_canonical_query_string 同口径）。
    """
    items = sorted((k, v) for k, v in params.items() if v is not None)
    return "&".join(
        f"{quote(str(k), safe='~')}={quote(str(v), safe='~')}" for k, v in items
    )


def _build_authorization(
    *,
    method: str,
    canonical_uri: str,
    canonical_query: str,
    headers: dict[str, str],
    hashed_payload: str,
    access_key_id: str,
    access_key_secret: str,
) -> tuple[str, str]:
    """按 ACS3-HMAC-SHA256 计算 Authorization 头。

    Returns:
        (authorization, signed_headers)

    ⚠️ `headers` 必须**恰好**是随请求发出的、参与签名的那批头（不含 Authorization
    自身）。规范化头块自带尾部换行，故 canonical_request 中头块与 signed_headers
    之间会出现一个空行——这是 V3 规范要求，不是笔误。
    """
    lowered = {k.lower(): str(v).strip() for k, v in headers.items()}
    signed_headers = ";".join(sorted(lowered))
    canonical_headers = "".join(f"{k}:{lowered[k]}\n" for k in sorted(lowered))

    canonical_request = (
        f"{method}\n"
        f"{canonical_uri}\n"
        f"{canonical_query}\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{hashed_payload}"
    )
    string_to_sign = (
        f"{_ALGORITHM}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    )
    signature = hmac.new(
        access_key_secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    authorization = (
        f"{_ALGORITHM} Credential={access_key_id},"
        f"SignedHeaders={signed_headers},Signature={signature}"
    )
    return authorization, signed_headers


# ==================== 发送 ====================


def _resolve_endpoint() -> str:
    """接入地址：显式配置 > 国内短信标准地址。"""
    if settings.SMS_ENDPOINT:
        return settings.SMS_ENDPOINT.strip()
    return _DEFAULT_ENDPOINT


def _normalize_to(mobile: str) -> str:
    """把业务侧手机号归一为阿里云要求的「区号 + 号码」形式。

    ⚠️ **归一化只在网关内部发生，绝不回写 DB**。`verify_code.identifier` 存的是
    `MOBILE_RE` 校验过的裸 11 位号，校验环节按 identifier 精确查行——一旦把 `86`
    前缀写进 identifier，发码与验码两侧的匹配会全线断裂（用户永远验不过）。
    """
    raw = mobile.strip().replace(" ", "").replace("-", "")
    if raw.startswith("+"):
        return raw[1:]

    cc = settings.SMS_DEFAULT_COUNTRY_CODE.strip()
    if not cc:
        return raw
    # 大陆号段恒以 1 开头，故裸号不可能以 "86" 起始，此判断对本项目无误判风险；
    # 未来接入其他区号时需改为按长度判定。
    return raw if raw.startswith(cc) else f"{cc}{raw}"


async def _send(
    to: str,
    template_code: str,
    params: dict | None = None,
    validity_period: int | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """调用一次阿里云 SendSms。

    ``transport`` 仅供测试注入（httpx.MockTransport），生产路径不传。

    Args:
        to:              手机号（裸号即可，内部自动补区号）
        template_code:   控制台报备的模板码
        params:          模板变量，如 {"code": "123456"}
        validity_period: 短信有效期（秒），None = 不指定

    Returns:
        message_id: 供应商侧消息 ID，用于对账与工单追踪

    Raises:
        SmsError: 未配置 / 网络异常 / 超时 / 供应商返回非 "OK"
    """
    if not is_configured():
        raise SmsError(
            "阿里云短信配置不完整（需 SMS_ACCESS_KEY_ID / SMS_ACCESS_KEY_SECRET / SMS_SIGN_NAME）",
            code="not_configured",
        )
    if not template_code:
        raise SmsError("模板码为空", code="empty_template")

    endpoint = _resolve_endpoint()
    masked = mask_mobile(to)

    # ── 业务参数（全部走 query string，请求体为空）──
    query_params: dict[str, object] = {
        "PhoneNumbers": _normalize_to(to),
        "SignName": settings.SMS_SIGN_NAME,
        "TemplateCode": template_code,
        # 模板参数必须是 JSON **字符串**，不是对象
        "TemplateParam": json.dumps(params or {}, ensure_ascii=False),
    }
    # 国内 SendSms 不支持 ValidityPeriod。保留函数参数以兼容现有调用方，
    # 验证码有效期仍由本系统 VerifyCode.expires_at 权威控制。

    canonical_query = _canonical_query(query_params)

    # ── 参与签名的头（不含 Authorization 自身）──
    signing_headers: dict[str, str] = {
        "host": endpoint,
        "x-acs-version": _API_VERSION,
        "x-acs-action": _ACTION,
        "x-acs-date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "x-acs-signature-nonce": uuid.uuid4().hex,
        "x-acs-content-sha256": _EMPTY_PAYLOAD_SHA256,
        "accept": "application/json",
    }

    authorization, _ = _build_authorization(
        method="POST",
        canonical_uri="/",
        canonical_query=canonical_query,
        headers=signing_headers,
        hashed_payload=_EMPTY_PAYLOAD_SHA256,
        access_key_id=settings.SMS_ACCESS_KEY_ID,
        access_key_secret=settings.SMS_ACCESS_KEY_SECRET,
    )
    request_headers = {**signing_headers, "Authorization": authorization}

    # 直接把规范化查询串拼进 URL，确保**上线内容与签名内容逐字一致**
    # （交给 httpx 的 params= 会按它自己的规则重新编码，可能与签名不符 → 签名校验失败）
    url = f"https://{endpoint}/?{canonical_query}"

    try:
        # 双超时：asyncio.timeout 卡总时长，httpx.Timeout 只做卡死探测（详见模块 docstring）
        async with asyncio.timeout(settings.SMS_TOTAL_TIMEOUT_MS / 1000):
            async with httpx.AsyncClient(
                transport=transport,
                timeout=httpx.Timeout(
                    settings.SMS_READ_TIMEOUT_MS / 1000,
                    connect=settings.SMS_CONNECT_TIMEOUT_MS / 1000,
                )
            ) as client:
                response = await client.post(url, headers=request_headers)
    except (TimeoutError, httpx.TimeoutException) as e:
        # TimeoutError = asyncio.timeout 总超时；httpx.TimeoutException = 空闲超时。
        # 二者不同继承树，必须都写。
        logger.error(f"[短信] 请求超时: to={masked} {type(e).__name__}")
        raise SmsError("短信接口请求超时", code="timeout", retryable=True) from e
    except httpx.HTTPError as e:
        logger.error(f"[短信] 网络异常: to={masked} {type(e).__name__}: {e}")
        raise SmsError(f"短信接口网络异常: {e}", code="transport_error", retryable=True) from e

    try:
        body = response.json()
    except Exception as e:
        logger.error(
            f"[短信] 响应非 JSON: to={masked} status={response.status_code} "
            f"body={response.text[:200]!r}"
        )
        raise SmsError("短信接口返回非 JSON 响应", code="bad_response", retryable=True) from e

    resp_code = str(body.get("Code") or "").strip()
    request_id = str(body.get("RequestId") or "")

    # ★ 关键：业务失败也可能是 HTTP 200，只能靠 Code 判定。
    if response.status_code != 200 or resp_code.upper() != _SUCCESS_CODE:
        code = resp_code or f"HTTP_{response.status_code}"
        desc = str(body.get("Message") or "")
        logger.error(
            f"[短信] 发送被拒: to={masked} status={response.status_code} "
            f"code={code} desc={desc} request_id={request_id}"
        )
        raise SmsError(
            f"短信发送失败: {code} {desc}".strip(),
            code=code,
            request_id=request_id,
            retryable=code in _RETRYABLE_CODES,
        )

    message_id = str(body.get("BizId") or "")
    logger.info(f"[短信] 发送成功: to={masked} message_id={message_id} request_id={request_id}")
    return message_id



def is_configured() -> bool:
    """阿里云所需凭证与签名是否齐备（不含总开关，总开关归网关管）。"""
    return bool(
        settings.SMS_ACCESS_KEY_ID
        and settings.SMS_ACCESS_KEY_SECRET
        and settings.SMS_SIGN_NAME
    )


class AliyunSmsDriver(SmsDriver):
    """阿里云国内短信驱动。

    ``transport`` 供测试注入 ``httpx.MockTransport``，让请求构造、签名头与响应判定
    不走真实网络也能被覆盖。
    """

    name = "aliyun"

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport

    def is_configured(self) -> bool:
        return is_configured()

    async def send(
        self,
        to: str,
        template_code: str,
        params: dict | None = None,
        validity_period: int | None = None,
    ) -> str:
        return await _send(to, template_code, params, validity_period, self._transport)
