"""短信网关：与服务商无关的统一入口。

用法::

    from webx.lib.sms import SmsScene, send_scene_sms
    await send_scene_sms(SmsScene.VERIFY_CODE, "13800138000", {"code": "123456"})

设计要点
--------
1. **场景化，而非自由文本**。短信是模板制的——内容须预先在服务商控制台报备，
   代码侧只能传 template_code + 参数。故对外暴露的是「业务场景」而非
   send_sms(to, text)（那是服务商无法兑现的接口）。场景 → 模板码由配置
   `SMS_TEMPLATES` 驱动，**新增业务场景只加配置、不改代码**。

2. **驱动可换**。``SMS_PROVIDER`` 选驱动（默认 aliyun）；``register_driver`` 注册
   自定义驱动。选定之后调用方只面对本模块，不关心背后是谁。

3. **失败即抛异常**，与 `webx.lib.mail.send_mail` 契约一致：网关不吞错、不重试。

4. **可用性 = 总开关 AND 驱动已配齐**，统一用 `is_ready()` 判断，不要直读
   ``SMS_ENABLED``：开关开了但凭证没配是「半启用」态，会让请求穿过端点闸门、
   白写一行验证码占掉当日额度后才失败。
"""

from __future__ import annotations

import logging
from typing import Callable

from webx.config import settings
from webx.lib.sms.base import SmsDriver, SmsError

logger = logging.getLogger(__name__)

DriverFactory = Callable[[], SmsDriver]

_FACTORIES: dict[str, DriverFactory] = {}
_INSTANCES: dict[str, SmsDriver] = {}
_WARNED_UNKNOWN: set[str] = set()


def register_driver(name: str, factory: DriverFactory) -> None:
    """注册驱动工厂（重复注册即覆盖，并丢弃已缓存的旧实例）。"""
    _FACTORIES[name] = factory
    _INSTANCES.pop(name, None)
    _WARNED_UNKNOWN.discard(name)
    logger.info("[短信] 注册驱动: %s", name)


def _aliyun() -> SmsDriver:
    # 延迟 import：只有真选了阿里云才需要 httpx 与其签名代码
    from webx.lib.sms.aliyun import AliyunSmsDriver

    return AliyunSmsDriver()


register_driver("aliyun", _aliyun)


def get_driver() -> SmsDriver | None:
    """取 ``SMS_PROVIDER`` 指定的驱动实例；名字未注册返回 None。

    未知驱动名按「短信不可用」处理而不是中止启动，但每个名字只告警一次 ERROR——
    否则配置写错会被静默当成「没开短信」。
    """
    name = settings.SMS_PROVIDER
    inst = _INSTANCES.get(name)
    if inst is not None:
        return inst
    factory = _FACTORIES.get(name)
    if factory is None:
        if name not in _WARNED_UNKNOWN:
            _WARNED_UNKNOWN.add(name)
            logger.error(
                "[短信] SMS_PROVIDER=%r 没有对应驱动（已注册: %s），短信视为不可用",
                name, ", ".join(sorted(_FACTORIES)),
            )
        return None
    inst = _INSTANCES[name] = factory()
    return inst


def is_ready() -> bool:
    """短信是否真正可用：总开关开启，且所选驱动的凭证与签名齐备。"""
    if not settings.SMS_ENABLED:
        return False
    driver = get_driver()
    return driver is not None and driver.is_configured()


async def send_sms(
    to: str,
    template_code: str,
    params: dict | None = None,
    validity_period: int | None = None,
) -> str:
    """按模板码发送短信（底层原语，一般用 send_scene_sms）。

    Raises:
        SmsError: 未启用 / 驱动不存在 / 配置不完整 / 发送失败
    """
    if not settings.SMS_ENABLED:
        raise SmsError("短信服务未启用（SMS_ENABLED=false）", code="not_enabled")
    driver = get_driver()
    if driver is None:
        raise SmsError(
            f"短信驱动 {settings.SMS_PROVIDER!r} 未注册", code="unknown_provider"
        )
    if not driver.is_configured():
        raise SmsError("短信服务配置不完整，请检查 SMS_* 凭证与签名", code="not_configured")
    if not template_code:
        raise SmsError("模板码为空", code="empty_template")
    return await driver.send(to, template_code, params, validity_period)


async def send_scene_sms(
    scene: str,
    to: str,
    params: dict | None = None,
    validity_period: int | None = None,
) -> str:
    """按业务场景发送短信（对外主入口）。

    场景 → 模板码经 ``settings.SMS_TEMPLATES`` 解析，业务代码无需知道模板码。

    Raises:
        SmsError: 场景未配置模板码，或下层发送失败
    """
    scene_key = str(scene)
    template_code = settings.SMS_TEMPLATES.get(scene_key, "")
    if not template_code:
        raise SmsError(
            f"短信场景 {scene_key} 未配置模板码，请在 SMS_TEMPLATES 中登记",
            code="template_not_configured",
        )
    return await send_sms(to, template_code, params, validity_period)
