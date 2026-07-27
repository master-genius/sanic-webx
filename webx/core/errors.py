"""统一业务异常。

原项目在 ``main.py`` 里为每个业务异常手写一个 ``@app.exception`` 处理器
（配额超限、状态冲突、文件校验…），每加一种异常就得改主入口。这里收敛成
**一个基类 + 一个处理器**：异常自带 HTTP 状态码与 i18n key，抛出即可，
主入口零改动。

业务用法::

    from webx.core.errors import WebxError

    class QuotaExceededError(WebxError):
        status = 403
        code = "quota_exceeded"        # 同时作为 i18n key

    raise QuotaExceededError(scope="daily", limit=10, current=11)

响应体::

    {"error": "<按 code 渲染的本地化文案>", "code": "quota_exceeded",
     "scope": "daily", "limit": 10, "current": 11}
"""

from __future__ import annotations

import logging

from sanic import Request
from sanic.response import json

from webx.core.i18n import get_lang_from_header, t

logger = logging.getLogger(__name__)


class WebxError(Exception):
    """业务异常基类。

    Attributes:
        status: HTTP 状态码。
        code: 错误码，同时作为 i18n 文案 key。
        context: 附加字段，既用于渲染文案的占位符，也原样并入响应体供前端分支。
    """

    status: int = 400
    code: str = "error"

    def __init__(
        self,
        message: str | None = None,
        *,
        code: str | None = None,
        status: int | None = None,
        **context,
    ):
        # code / status 是显式形参而非 context：否则 `raise NotFoundError(code="x")`
        # 会把 x 塞进 context、而实际错误码仍是类属性，静默给出错误响应。
        if code is not None:
            self.code = code
        if status is not None:
            self.status = status
        self.context = context
        self.message = message
        super().__init__(message or self.code)

    def to_dict(self, lang: str) -> dict:
        # 显式 message 优先；否则按 code 渲染本地化文案（context 充当占位符实参）
        text = self.message or t(self.code, lang, **self.context)
        return {"error": text, "code": self.code, **self.context}


class NotFoundError(WebxError):
    status = 404
    code = "not_found"


class UnauthorizedError(WebxError):
    status = 401
    code = "unauthorized"


class ForbiddenError(WebxError):
    status = 403
    code = "forbidden"


class InvalidParamsError(WebxError):
    status = 400
    code = "invalid_params"


class TooManyRequestsError(WebxError):
    status = 429
    code = "too_many_requests"


def setup_error_handlers(app) -> None:
    """注册统一异常处理器（由 create_app 自动调用）。"""

    @app.exception(WebxError)
    async def _handle_webx_error(request: Request, exc: WebxError):
        lang = get_lang_from_header(request.headers)
        # 5xx 才记栈：4xx 是预期内的业务拒绝，刷栈只会淹没真正的故障
        if exc.status >= 500:
            logger.exception("业务异常 %s: %s", exc.code, exc)
        return json(exc.to_dict(lang), status=exc.status)
