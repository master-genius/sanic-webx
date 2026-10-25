"""短信适配层。对外只需要这里导出的名字::

    from webx.lib.sms import SmsScene, send_scene_sms, is_ready

    await send_scene_sms(SmsScene.VERIFY_CODE, "13800138000", {"code": "123456"})

结构：``base`` 契约（驱动接口 / 场景 / 错误）· ``gateway`` 与服务商无关的统一入口 ·
``aliyun`` 阿里云驱动。接入新服务商 = 实现 ``SmsDriver`` 并 ``register_driver``，
再把 ``SMS_PROVIDER`` 指过去。

本包 import 时**不**加载 httpx 与具体驱动，驱动在首次取用时才延迟导入。
"""

from __future__ import annotations

from dataclasses import dataclass

from webx.lib.sms.base import SmsDriver, SmsError, SmsScene
from webx.lib.sms.gateway import (
    get_driver,
    is_ready,
    register_driver,
    send_scene_sms,
    send_sms,
)

__all__ = [
    "SmsDriver",
    "SmsError",
    "SmsScene",
    "SmsSettingsMixin",
    "get_driver",
    "is_ready",
    "register_driver",
    "send_scene_sms",
    "send_sms",
]


@dataclass
class SmsSettingsMixin:
    """已废弃的兼容壳：``SMS_*`` 配置字段现已并入 ``webx.config.BaseSettings``。

    旧写法 ``class Settings(SmsSettingsMixin, BaseSettings)`` 仍可工作，新代码直接
    继承 ``BaseSettings`` 即可。
    """
