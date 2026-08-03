"""核心层（地板）：应用工厂、蓝图扫描、ORM、异常、日志、路由级守卫。

**中间件不在这里**——认证 / RBAC / CORS 都在 ``webx.middleware``，一个中间件一个模块。
加一条中间件永远不该变成"改 core 文件"；且中间件要查库（依赖 models / services），
留在 core 里会把业务模型拽进地板层。

配置、路径与文案同样不在这里——它们是**被所有层使用的叶子模块**，放在 core 里会让
lib / services / middleware 反向依赖 core、构成包级循环。
见 ``webx.config`` / ``webx.paths`` / ``webx.i18n``。

``t`` / ``register`` 在这里只是转发（``webx.i18n`` 才是本体），沿用老的
``from webx.core import t`` 写法不受影响。
"""

from webx.core.errors import (
    ForbiddenError,
    InvalidParamsError,
    NotFoundError,
    TooManyRequestsError,
    UnauthorizedError,
    WebxError,
)
from webx.i18n import get_lang_from_header, register, t

__all__ = [
    "get_lang_from_header", "register", "t",
    "WebxError", "NotFoundError", "UnauthorizedError",
    "ForbiddenError", "InvalidParamsError", "TooManyRequestsError",
]
