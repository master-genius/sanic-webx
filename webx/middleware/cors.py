"""CORS：预检短路 + 响应头装配。

``setup_cors(app)`` 是**应用级**装配（由 ``create_app`` 调用），与按目录挂载的
``MIDDLEWARES`` 不是同一层，但同属「中间件」语义域，故同放本包。
"""

from __future__ import annotations

from sanic import Request, response

from webx.config import settings


def build_cors_headers(request: Request) -> dict[str, str]:
    """按配置构造 CORS 响应头。

    - dev 环境（``APP_ENV=dev``）放开所有来源，方便本地联调
    - 生产环境只回显命中 ``CORS_ALLOWED_ORIGINS`` 白名单的 Origin

    ⚠️ ``Allow-Headers`` 必须显式列出 ``Authorization``——按 Fetch 规范
    通配符 ``*`` 不覆盖 Authorization。
    """
    origin = request.headers.get("Origin", "")

    common = {
        "Access-Control-Allow-Methods": settings.CORS_ALLOW_METHODS,
        "Access-Control-Allow-Headers": settings.CORS_ALLOW_HEADERS,
        "Access-Control-Max-Age": "86400",
    }

    if settings.is_dev:
        return {"Access-Control-Allow-Origin": "*", **common}

    if origin and origin in settings.CORS_ALLOWED_ORIGINS:
        return {"Access-Control-Allow-Origin": origin, **common}

    # 未配置白名单或 Origin 不在名单内：不回 CORS 头，浏览器自行拦截
    return {}


def setup_cors(app) -> None:
    """为应用注册 CORS：预检短路 + 响应头注入。

    **幂等**：它挂在 ``before_server_start`` 里，而同一个 app 对象可以被启动多次（开发热重载、测试客户端
    每个请求起停一次）。不幂等的话每次启动都会再叠一层中间件——除了重复加头，更糟的是路由器的信号表
    随之逐次膨胀，数百次重启后 ``sanic_routing`` 生成路由树时递归超限（``RecursionError``）。
    """
    if getattr(app.ctx, "_webx_cors_installed", False):
        return
    app.ctx._webx_cors_installed = True

    @app.middleware("request")
    async def _cors_preflight(request: Request):
        if request.method == "OPTIONS":
            return response.text("", headers=build_cors_headers(request))

    @app.middleware("response")
    async def _cors_headers(request: Request, resp):
        for key, value in build_cors_headers(request).items():
            resp.headers[key] = value
