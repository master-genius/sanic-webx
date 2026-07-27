"""业务配置：继承 webx 的 BaseSettings，只写自己的字段。

``configure_settings`` 把实例注册为全局单例，框架内部 ``from webx.config
import settings`` 拿到的代理会解析到这里——框架与业务配置类零编译期耦合。
"""

from dataclasses import dataclass, field

from webx.config import BaseSettings, configure_settings, env


@dataclass
class Settings(BaseSettings):
    # 模型模块：dbsync 与 Tortoise 的加载源。
    # 用到 webx 自带的用户体系/权限体系，就把对应模型模块列进来。
    MODELS_MODULES: list[str] = field(
        default_factory=lambda: ["app.models", "webx.models"]
    )

    # 业务自有字段示例
    WELCOME_TEXT: str = field(default_factory=lambda: env.str("WELCOME_TEXT", "hello webx"))


settings = configure_settings(Settings())
