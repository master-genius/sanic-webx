"""应用工厂。

取代原项目那个把日志、CORS、蓝图、DB、ZMQ、四个业务异常处理器全塞在一起的
``main.py``：装配流程收进 ``create_app()``，**业务侧连入口模块都不需要**。

    # run.py —— 项目唯一入口
    from app.config import settings          # 已 configure_settings 注册
    from webx.core.app import create_app

    app = create_app(modules=["app"])        # 按约定展开 app/controller、app/i18n

``modules`` 走约定（``<pkg>/controller/`` 必扫、``<pkg>/i18n.py`` 可选），部署组合由
启动参数决定；也可以用 ``controller_package=`` 精确指定包名，两者可并存。

需要额外启动/关闭动作（缓存、消息队列、定时器…）时用钩子参数注入，
不必改框架文件。
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
from typing import Any, Awaitable, Callable, Iterable, Optional

from sanic import Sanic, text
from sanic.response import json

from webx.core.auto_blueprint import auto_register_blueprints
from webx.config import settings
from webx.core.db import close_db, init_db
from webx.core.errors import setup_error_handlers
from webx.i18n import get_lang_from_header, t
from webx.core.log_config import setup_logging
from webx.middleware import setup_cors

logger = logging.getLogger(__name__)

Hook = Callable[[Sanic], Awaitable[None]]


# 可单独开关的内置路由组
BUILTIN_GROUPS = ("auth", "api", "admin", "public")


def _builtin_packages(spec: bool | Iterable[str]) -> list[str]:
    """把 ``builtin_controller`` 归一化为待扫描的包名列表。

    ``True`` → 整个 ``webx.controller``（含其根级 ``__mid__.py``）；
    ``False`` / 空列表 → 一组都不挂（**显式**表达的"不要"，照办）；
    子集 → 逐个扫 ``webx.controller.<组>``，未列出的组不注册。

    ⚠️ 组名写错**不中止启动**，剔除无效项后继续；若一个有效的都不剩，回退成全加载。
    取舍是「先保证服务起得来」——内置组配错时，宁可多挂几组也不要让整个服务拒绝启动。
    但每一次回退都必须**留声**（WARNING），否则「我以为只挂了 auth」与「实际全挂了」
    这件事就永远暴露不出来，那正是白送攻击面的由来。
    """
    if spec is True:
        return ["webx.controller"]
    if not spec:
        return []

    groups = list(spec)
    known = [g for g in groups if g in BUILTIN_GROUPS]
    unknown = [g for g in groups if g not in BUILTIN_GROUPS]

    if unknown:
        logger.warning(
            "⚠️ 未知的内置路由组 %s 已忽略，可选: %s",
            unknown,
            ", ".join(BUILTIN_GROUPS),
        )
    if not known:
        logger.warning(
            "⚠️ 内置路由组配置 %s 全部无效，回退为**全部加载**（%s，含 admin 管理端）。"
            "只要部分组请写成逗号分隔的有效组名；确实不要内置路由，请显式配置 false",
            groups,
            ",".join(BUILTIN_GROUPS),
        )
        return ["webx.controller"]

    return [f"webx.controller.{g}" for g in known]


def _log_builtin_summary(builtin_packages: Iterable[str]) -> None:
    """启动时明确报告「实际挂了哪些内置路由组」。

    默认全挂是有意的，但「我以为只挂了 auth」与「实际全挂了」只有靠这一行才看得出来。
    挂了 admin 组时多提一句：它带整套账号 CRUD 与权限管理端点。
    """
    pkgs = list(builtin_packages)
    if not pkgs:
        logger.info("内置路由组: 无（builtin_controller=False，纯业务服务）")
        return

    groups = (
        list(BUILTIN_GROUPS)
        if "webx.controller" in pkgs
        else [p.rsplit(".", 1)[-1] for p in pkgs]
    )
    logger.info("内置路由组: %s", ",".join(groups))
    if "admin" in groups:
        logger.info(
            "内置 admin 组已挂载（账号 CRUD、角色/权限管理，位于 /api/admin/*）。"
            "不需要请设 WEBX_BUILTIN=auth,api,public 或 false"
        )


def _expand_modules(modules: Iterable[str]) -> list[str]:
    """把业务模块包名按**约定**展开，并就地完成 i18n 与 models 的挂接。

    约定（目录名固定，包名由部署声明）::

        <pkg>/controller/     必须有 —— 蓝图扫描的根
        <pkg>/i18n.py         可选   —— import 即注册文案（要的只是副作用）
        <pkg>/models(.py|/)   可选   —— 交给 ORM 建表/查询

    这就是原先业务入口模块里那几行手写装配的去处：业务只管把目录摆成这个样子，
    不必再写一个入口模块。缺失的部分**存在就加载、不存在就跳过**，与 ``__mid__.py``
    「文件不存在是合法配置」的语义一致。

    models 由调用方统一挂接（见 `create_app` 里的 ``apply_module_models``），
    因为 ``webx`` 自己也走同一套约定，不属于本函数处理的业务模块之列。

    Returns:
        待扫描的 controller 包名列表。

    Raises:
        ImportError: ``<pkg>.controller`` 不存在。这是硬性约定，缺了整个模块就没有
            任何路由可注册，静默跳过等于「装配成功但一个接口都没有」。
    """
    from webx.config import module_exists

    packages: list[str] = []

    for mod in modules:
        if module_exists(f"{mod}.i18n"):
            importlib.import_module(f"{mod}.i18n")
            logger.debug("已加载业务文案模块: %s.i18n", mod)

        ctrl = f"{mod}.controller"
        if not module_exists(ctrl):
            raise ImportError(
                f"业务模块 {mod!r} 缺少 controller 包（期望 {ctrl}）。"
                f"约定：<pkg>/controller/ 是蓝图扫描的根，必须存在。"
            )
        packages.append(ctrl)

    return packages


def _admin_enabled(builtin_packages: Iterable[str]) -> bool:
    """本次装配是否挂载了内置 admin 组（决定要不要做 admin 权限内务）。

    收的是**已归一化的包名列表**而非原始 spec：`_builtin_packages` 会对无效组名
    发 WARNING，让它在一次装配里跑两遍会把同一条告警打印两次，看起来像是两处配错。
    """
    return any(p == "webx.controller" or p.endswith(".admin") for p in builtin_packages)


def _storage_enabled(builtin_packages: Iterable[str]) -> bool:
    """本次装配是否挂载了会用到文件存储的内置组（api 的文件端点、public 的签名下载）。"""
    return any(
        p == "webx.controller" or p.endswith((".api", ".public")) for p in builtin_packages
    )


def _prepare_storage() -> None:
    """启动时建好存储目录，并回收超龄未完成的分片上传会话。

    会话回收不接线就是磁盘泄漏：init 之后从不 complete 的会话会永久残留。
    多 worker 各跑一次是安全的（rmtree 幂等，竞态由回收函数内部兜住）。
    存储不可用不该拖垮整个服务——其余路由照常，只是文件端点会各自报错——所以只记 ERROR。
    """
    from webx.services.storage_service import storage_service

    try:
        storage_service.ensure_dirs()
        storage_service.cleanup_stale_uploads()
    except OSError as e:
        logger.error("存储初始化失败（文件相关端点将不可用）: %s", e)


def register_admin_permissions() -> None:
    """把内置管理端的权限定义写进注册表（纯内存、幂等）。

    延迟 import：``webx.lib`` / ``webx.services`` 会反向用到 core 的东西，
    模块级 import 会构成包级循环。放在函数里，只有真挂了 admin 组才加载。
    """
    from webx.lib.admin_permissions import register_admin_permissions as _reg

    _reg()


def create_app(
    controller_package: str | Iterable[str] | None = None,
    *,
    modules: Iterable[str] = (),
    name: Optional[str] = None,
    use_db: bool = True,
    builtin_controller: bool | Iterable[str] = True,
    global_middleware: Iterable[Any] = (),
    on_startup: Iterable[Hook] = (),
    on_shutdown: Iterable[Hook] = (),
    health_endpoints: bool = True,
) -> Sanic:
    """装配一个 webx 应用。

    Args:
        controller_package: 业务控制器根包名，如 ``"app.controller"``；
            也可传**多个**（如 ``["app.controller", "appx.controller"]``），
            用于「一个进程按需装配若干业务模块」的部署形态。各包独立扫描、
            各自继承自己的根 ``__mid__.py``，**互不影响**——中间件是逐个复制到
            蓝图对象上的，不存在共享容器。唯一的跨包约束是蓝图名全局唯一
            （撞了会抛 ``BlueprintNameConflictError``，启动即失败）。
        modules: 业务**模块包**名，如 ``["app"]`` / ``["app", "appx"]``。
            按约定展开为 ``<pkg>.controller``（扫蓝图）与 ``<pkg>.i18n``（import
            注册文案，可选），见 `_expand_modules`。这条路径让业务**不需要写入口
            模块**——把目录摆成约定的样子就能被装配，部署组合由启动参数决定。

            与 ``controller_package`` 可并存（先内置组、再 modules、最后
            controller_package）；两个都不给则只有内置组，属于合法配置（纯网关）。
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

            挂载了 ``admin`` 组时，框架自动把**内置管理端的权限定义**写进内存
            注册表（运行时 RBAC 中间件查的就是它）；**没挂就不写**——它描述的就是
            ``/api/admin/*`` 那些端点的权限，没挂却注册等于凭空多出一堆没有对应
            路由的权限。这件事以前要业务在自己的入口模块里无条件手写，于是
            ``builtin_controller=False``（框架推荐的新服务起步姿势）时是**错的**。

            ⚠️ 装配**不会**把注册表同步进数据库。那是运维动作，手动执行
            ``python tools/org.py init-perm``。
        global_middleware: **全站级**中间件，对内置组与全部业务包一并生效。
            它是 ``__mid__.py`` 继承链的最外层起点，详见
            ``auto_register_blueprints`` 的同名参数（含准入规则与 404 边界）。
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
    # 全站链对每一次扫描都传同一份 → 内置组与业务包吃到的是同一套最外层中间件。
    if controller_package is None:
        biz_packages = []
    elif isinstance(controller_package, str):
        biz_packages = [controller_package]
    else:
        biz_packages = list(controller_package)

    # 保序去重：同一个 controller 包被扫两次会**静默双挂中间件**——蓝图是模块级
    # 单例，第二次 import 命中缓存拿到同一批对象，于是 bp.middleware() 被调两次，
    # 链直接翻倍；而 app.blueprint() 的二次注册被 Sanic 静默跳过，蓝图数看着还正常。
    # 三道守卫都拦不住它：seen 是单次调用内的、名字冲突判的是 `existing is not bp`。
    # 触发方式很日常：modules=["webx"] 与 builtin_controller 重叠，
    # 或 controller_package="app.controller" 与 modules=["app"] 并写。
    # 只归一化一次：重复调用会把无效组名的 WARNING 打印两遍
    builtin_packages = _builtin_packages(builtin_controller)
    _log_builtin_summary(builtin_packages)

    # 模型模块一律按约定扫 <pkg>/models —— **webx 自己也走这套**，不搞特殊。
    # 挂了任何内置路由组就带上 webx.models（auth/api/admin 组都要查 User/Permission
    # 那几张表），没挂就不带。业务因此不需要在 Settings 里写任何模型模块：
    # 确定的模块 + 确定的目录，扫描即知，配置是多余的。
    from webx.config import apply_module_models

    model_sources = list(modules) + (["webx"] if builtin_packages else [])
    if model_sources:
        logger.debug("模型模块 = %s", apply_module_models(model_sources))

    seen_pkgs: set[str] = set()
    all_packages: list[str] = []
    for pkg in builtin_packages + _expand_modules(modules) + biz_packages:
        if pkg in seen_pkgs:
            logger.warning(
                "controller 包 %s 被重复指定，只扫描一次。"
                "检查 builtin_controller / modules / controller_package 是否指向了同一个包",
                pkg,
            )
            continue
        seen_pkgs.add(pkg)
        all_packages.append(pkg)

    for package in all_packages:
        auto_register_blueprints(app, package, global_middleware=global_middleware)

    setup_error_handlers(app)

    startup_hooks = list(on_startup)

    # 内置管理端的权限定义写进**内存注册表** —— 只在挂了 admin 组时做。
    # 它描述的就是 /api/admin/* 那些端点的权限，没挂 admin 却注册 = 凭空多出一堆
    # 没有对应路由的权限。运行时 RBAC 中间件查的就是这张表，故必须在服务进程里执行。
    #
    # ⚠️ 这里**不做**「同步进数据库」。改表结构是运维动作，只能手动执行
    # （``python tools/org.py init-perm``）：它会按代码删除库里已废弃的权限，
    # 让它跟着每次启动自动跑，等于把一次误删的影响面从「运维敲错命令」扩大到
    # 「任何一次重启」。何况启动钩子是**每个 worker 各跑一次**，多 worker 下
    # 还会并发写同一批记录。
    if _admin_enabled(builtin_packages):
        register_admin_permissions()
    shutdown_hooks = list(on_shutdown)
    prepare_storage = _storage_enabled(builtin_packages)

    @app.before_server_start
    async def _startup(app: Sanic):
        setup_cors(app)
        if use_db:
            await init_db(app)
        if prepare_storage:
            _prepare_storage()
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
