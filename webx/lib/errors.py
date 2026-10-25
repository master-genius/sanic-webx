"""业务异常基类。

放在 ``lib`` 而不是 ``core``：services 层（配额、任务状态机…）也要抛带 HTTP 语义的业务异常，
而 services 不得依赖 core（分层顺序见 tests/test_layering.py）。基类只依赖 i18n，是叶子级的东西。
``webx.core.errors`` 重新导出它，既有的 ``from webx.core.errors import WebxError`` 写法不受影响；
异常处理器仍在 core（``setup_error_handlers``）。
"""

from __future__ import annotations

from webx.i18n import t


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
