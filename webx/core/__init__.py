"""核心层：应用工厂、蓝图扫描、中间件（认证/权限/CORS）、ORM、异常、i18n、日志。

配置与路径不在这里——它们是**被所有层使用的叶子模块**，放在 core 里会让
lib / services 反向依赖 core、构成包级循环。见 ``webx.config`` 与 ``webx.paths``。
"""

from webx.core.errors import (
    ForbiddenError,
    InvalidParamsError,
    NotFoundError,
    TooManyRequestsError,
    UnauthorizedError,
    WebxError,
)
from webx.core.i18n import get_lang_from_header, register, t

__all__ = [
    "get_lang_from_header", "register", "t",
    "WebxError", "NotFoundError", "UnauthorizedError",
    "ForbiddenError", "InvalidParamsError", "TooManyRequestsError",
]
