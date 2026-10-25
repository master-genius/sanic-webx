"""短信适配层的契约：驱动接口、场景枚举、错误类型。

驱动只负责「把一条已选定模板的短信交给某个服务商」，不认识业务场景、
不读场景映射——那是网关（``webx.lib.sms.gateway``）与服务商无关的部分。
换服务商 = 换驱动 + 换模板码，调用方一行都不用动。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from typing import ClassVar


class SmsScene(StrEnum):
    """短信业务场景。

    枚举值 = `SMS_TEMPLATES` 配置里的 key。新增场景：
      ① 这里加一个成员；② 控制台报备模板；③ .env 的 SMS_TEMPLATES 加一条映射。
    业务代码只认场景名，模板码变更（换模板 / 换供应商）不影响任何调用点。
    """

    VERIFY_CODE = "verify_code"    # 验证码（登录 / 注册 / 变更 / 绑定共用）


class SmsError(Exception):
    """短信发送失败。

    Attributes:
        code:       供应商响应码或本模块的内部错误码，便于日志聚合与告警分类
        request_id: 供应商请求 ID，向工单提供的唯一凭据
        retryable:  是否值得重试（瞬时故障 True / 业务性拒绝 False）
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "",
        request_id: str = "",
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id
        self.retryable = retryable


class SmsDriver(ABC):
    """服务商驱动契约。

    约定：
      - ``send`` **失败即抛 SmsError**，不吞错、不重试（失败策略归调用方，
        验证码服务已有 try/except → SEND_FAILED 分支）。
      - 业务性拒绝（余额不足 / 模板未审核 / 签名不匹配 ...）即使服务商回 HTTP 200
        也必须抛错；只看 HTTP 状态码会让「接口报成功、用户永远收不到」无痕发生。
      - 号码格式（区号前缀等）是服务商各自的要求，由驱动在内部归一，
        **绝不回写调用方的存储**（验证码表按裸号精确匹配）。
    """

    #: 驱动名，对应 ``SMS_PROVIDER`` 配置值
    name: ClassVar[str]

    @abstractmethod
    def is_configured(self) -> bool:
        """该服务商所需的凭证 / 签名等是否齐备（不含总开关 ``SMS_ENABLED``）。"""

    @abstractmethod
    async def send(
        self,
        to: str,
        template_code: str,
        params: dict | None = None,
        validity_period: int | None = None,
    ) -> str:
        """按模板码发送一条短信，返回服务商侧消息 ID（用于对账与工单追踪）。

        Args:
            to:              裸手机号
            template_code:   控制台报备的模板码
            params:          模板变量，如 {"code": "123456"}
            validity_period: 有效期（秒）提示；服务商不支持时可忽略，
                             权威有效期始终由 VerifyCode.expires_at 控制
        """
