"""业务配置：继承 webx 的 BaseSettings，只写自己的字段。

``configure_settings`` 把实例注册为全局单例，框架内部 ``from webx.config
import settings`` 拿到的代理会解析到这里——框架与业务配置类零编译期耦合。
"""

from dataclasses import dataclass, field

from webx.config import BaseSettings, configure_settings, env


@dataclass
class Settings(BaseSettings):
    # 这里只写业务自有的配置字段。
    #
    # ⚠️ 模型模块**没有配置项**：按约定自动挂接——装配哪个模块就扫它的
    # <pkg>/models，webx 自己也走同一套（挂了内置路由就带上 webx/models）。
    # 确定的模块 + 确定的目录，扫描即知。不想要某个模块的模型就把它的
    # models.py 改名成 _models.py 或删掉，探测不到自然不加载。

    # 业务自有字段示例
    WELCOME_TEXT: str = field(default_factory=lambda: env.str("WELCOME_TEXT", "hello webx"))


settings = configure_settings(Settings())
