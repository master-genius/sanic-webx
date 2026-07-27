"""应用工厂。

取代原项目那个把日志、CORS、蓝图、DB、ZMQ、四个业务异常处理器全塞在一起的
``main.py``：装配流程收进 ``create_app()``，业务项目的入口只剩几行。

    # app/main.py
    from webx.core.app import create_app
    from app.config import settings          # 已 configure_settings 注册

    app = create_app("app.controller")

需要额外启动/关闭动作（缓存、消息队列、定时器…）时用钩子参数注入，
不必改框架文件。
"""

from __future__ import annotations

import logging
import os
from typing import Awaitable, Callable, Iterable, Optional

from sanic import Sanic, text
from sanic.response import json

from webx.core.auto_blueprint import auto_register_blueprints
from webx.config import settings
from webx.core.db import close_db, init_db
from webx.core.errors import setup_error_handlers
from webx.core.i18n import get_lang_from_header, t
from webx.core.log_config import setup_logging
from webx.core.middleware import setup_cors

logger = logging.getLogger(__name__)

Hook = Callable[[Sanic], Awaitable[None]]


# 可单独开关的内置路由组
BUILTIN_GROUPS = ("auth", "api", "admin", "public")


def _builtin_packages(spec: bool | Iterable[str]) -> list[str]:
    """把 ``builtin_controller`` 归一化为待扫描的包名列表。

    ``True`` → 整个 ``webx.controller``（含其根级 ``__mid__.py``）；
    子集 → 逐个扫 ``webx.controller.<组>``，未列出的组不注册。
    """
    if spec is True:
        return ["webx.controller"]
    if not spec:
        return []

    groups = list(spec)
    unknown = [g for g in groups if g not in BUILTIN_GROUPS]
    if unknown:
        raise ValueError(
            f"未知的内置路由组 {unknown}，可选: {', '.join(BUILTIN_GROUPS)}"
        )
    return [f"webx.controller.{g}" for g in groups]


def create_app(
    controller_package: str,
    *,
    name: Optional[str] = None,
    use_db: bool = True,
    builtin_controller: bool | Iterable[str] = True,
    on_startup: Iterable[Hook] = (),
    on_shutdown: Iterable[Hook] = (),
    health_endpoints: bool = True,
) -> Sanic:
    """装配一个 webx 应用。

    Args:
        controller_package: 业务控制器根包名，如 ``"app.controller"``。
        name: Sanic 应用名，默认取 ``settings.APP_NAME``。
        use_db: 是否自动初始化/关闭 Tortoise 连接。传 ``False`` 时框架**完全不碰**
            数据库——``on_startup`` 钩子照常执行，若钩子或路由要查库，
            建连就得你自己在钩子里做，否则会撞上晦涩的
            "No TortoiseContext is currently active"。
        builtin_controller: 挂载哪些内置路由组。``True`` 全挂，``False`` 全不挂，
            也可传子集如 ``["auth", "api"]``
            （可选 ``auth`` / ``api`` / ``admin`` / ``public``）。
            想改写其中某一组：把它从这里去掉，然后在自己的 controller 里重写——
            内置与业务**不能同路径共存**，Sanic 会 RouteExists 报错。
            内置路由走**同一套** ``__mid__.py`` 扫描机制，没有特殊待遇。
        on_startup: 追加的启动钩子，按序在 DB 初始化**之后**执行。
        on_shutdown: 追加的关闭钩子，按序在 DB 关闭**之前**执行。
        health_endpoints: 是否注册 ``/`` 与 ``/health``。

    Returns:
        装配完成的 Sanic 实例。
    """
    # ⚠️ 时序：必须在 Sanic() 构造【之前】配置日志。Sanic 构造时即 dictConfig，
    # 其 handler 在那一刻捕获 sys.stdout 引用——晚一步，日志就落不了盘。
    setup_logging(log_dir=settings.WEB_LOG_DIR, debug=settings.debug)

    app = Sanic(name or settings.APP_NAME)

    app.config.EXTENSIONS_JSON = True
    # CORS 允许头必须显式含 Authorization（Fetch 规范：通配符不覆盖它）。
    # 注意逗号后不能带空格——sanic-ext 解析只 lower 不 strip。
    app.config.CORS_ALLOW_HEADERS = settings.CORS_ALLOW_HEADERS
    app.config.REQUEST_MAX_SIZE = settings.REQUEST_MAX_SIZE

    # 蓝图扫描在构造期完成（Sanic 要求路由在 server 启动前注册完毕）。
    # ⚠️ 内置路由与业务路由**不能同路径共存**：Sanic 遇到重复路径直接 RouteExists 报错，
    # 没有"后注册的覆盖先注册的"这回事。要改写内置的某一组，就用 builtin_controller
    # 把那一组关掉，再在自己的 controller 里重写。
    for package in _builtin_packages(builtin_controller):
        auto_register_blueprints(app, package)
    auto_register_blueprints(app, controller_package)

    setup_error_handlers(app)

    startup_hooks = list(on_startup)
    shutdown_hooks = list(on_shutdown)

    @app.before_server_start
    async def _startup(app: Sanic):
        setup_cors(app)
        if use_db:
            await init_db(app)
        for hook in startup_hooks:
            await hook(app)
        logger.info("✅ 服务启动 PID=%s", os.getpid())

    @app.after_server_stop
    async def _shutdown(app: Sanic):
        for hook in shutdown_hooks:
            await hook(app)
        if use_db:
            await close_db(app)

    if health_endpoints:

        @app.get("/")
        async def _index(request):
            lang = get_lang_from_header(request.headers)
            return json(
                {
                    "name": app.name,
                    "env": settings.APP_ENV,
                    "status": "running",
                    "message": t("success", lang),
                }
            )

        @app.get("/health")
        async def _health(request):
            """存活探针：不查库、不依赖外部组件，永远轻量。"""
            return text("ok")

    return app
