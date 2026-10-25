"""领取服务配置：在 ``BaseSettings`` 上只改默认值，租约参数的单一来源仍是核心配置。"""

from __future__ import annotations

from dataclasses import dataclass, field

from webx.config import BaseSettings, configure_settings, env


@dataclass
class TaskServerSettings(BaseSettings):
    APP_NAME: str = field(default_factory=lambda: env.str("APP_NAME", "webx-task-server"))


def configure() -> TaskServerSettings:
    return configure_settings(TaskServerSettings())  # type: ignore[return-value]
