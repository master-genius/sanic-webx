"""``create_app`` 的装配契约：全站中间件、多业务包、跨包撞名、admin 内务联动。

这四件事的共同点是**它们只在 `create_app` 这一层才成立**——`auto_register_blueprints`
单次调用看不到全貌（全站链要对每次调用都生效、撞名要跨扫描根、admin 内务要看
`builtin_controller`）。故单独一个文件，与只测单次扫描的
`test_auto_blueprint_registration.py` / `test_auto_blueprint_mid_failure.py` 分开。

钉死的契约：

  A. ``global_middleware`` 对**内置组与业务包一并生效**——这是此前唯一有安全含义的
     缺口：业务根 ``__mid__.py`` 盖不到 ``/api/user`` 等内置路由，且无声。
  B. 全站链与包级 ``__mid__.py`` 重复声明**不双跑**（迁移期可以两边都留着）。
  C. 全站链**不得**把公开端点锁死——准入规则的反向不变量。
  D. ``controller_package`` 收列表时各包独立继承，互不影响。
  E. 蓝图名撞车**跨扫描根**也能抓到，且点名双方。
  F. admin 权限内务**挂了才做，没挂一件都不做**。
"""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sanic import Sanic

from webx.core.app import create_app
from webx.core.auto_blueprint import BlueprintNameConflictError
from webx.lib import permissions as perm_registry

_counter = {"n": 0}

_BP_SRC = (
    "from sanic import Blueprint\n"
    "from sanic.response import text\n"
    '{n}_bp = Blueprint("{n}", url_prefix="{p}")\n'
    '@{n}_bp.get("/ping")\n'
    "async def ping(request):\n"
    '    return text("{n}")\n'
)


async def g_one(request):
    """全站链成员：幂等的上下文注入（真实场景里是 request_id 那一类）。"""
    return None


async def g_two(request):
    return None


@pytest.fixture
def biz_pkgs():
    """产出 ``build(*specs) -> [包名...]``，每个 spec 是 ``{相对路径: 源码}``。

    每次用独立的临时目录与包名，避免 ``sys.modules`` 串味。
    """
    tmp = Path(tempfile.mkdtemp())
    sys.path.insert(0, str(tmp))
    created: list[str] = []

    def build(*specs: dict[str, str]) -> list[str]:
        for i, files in enumerate(specs):
            _counter["n"] += 1
            pkg = f"bizpkg{_counter['n']}"
            created.append(pkg)
            (tmp / pkg).mkdir(parents=True, exist_ok=True)
            (tmp / pkg / "__init__.py").write_text("")
            for rel, src in files.items():
                path = tmp / pkg / rel
                path.parent.mkdir(parents=True, exist_ok=True)
                for parent in path.parents:
                    if parent == tmp / pkg:
                        break
                    (parent / "__init__.py").touch()
                path.write_text(src)
        return created

    yield build

    for pkg in created:
        for m in [m for m in sys.modules if m == pkg or m.startswith(pkg + ".")]:
            sys.modules.pop(m, None)
    sys.path.remove(str(tmp))
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def mkapp():
    """产出 ``build(controller_package, **kw) -> Sanic``，自动给独立应用名并善后。

    ⚠️ 每次装配前清掉 ``webx.controller`` / ``app.controller`` 的模块缓存：
    **蓝图是模块级单例**——模块只 import 一次，蓝图对象随之固化，第二个 app
    复用同一个对象会撞 ``RouteExists``（见 `_find_blueprints` 的 docstring，
    那里也说明了生产环境的正解是把 ``__blueprints__`` 写成工厂）。
    清掉缓存 → 重新 import → 拿到一批全新的蓝图对象，各 app 互不干扰。
    """
    names: list[str] = []

    def build(controller_package, **kw):
        for m in [
            m
            for m in sys.modules
            if m.startswith(("webx.controller", "app.controller"))
        ]:
            sys.modules.pop(m, None)

        _counter["n"] += 1
        name = f"compose{_counter['n']}"
        names.append(name)
        kw.setdefault("use_db", False)
        return create_app(controller_package, name=name, **kw)

    yield build

    for n in names:
        Sanic._app_registry.pop(n, None)


def _mids(app: Sanic, bp_name: str) -> list[str]:
    return [fm.middleware.func.__name__ for fm in app.blueprints[bp_name]._future_middleware]


# ==================== A：全站链覆盖内置组与业务包 ====================


def test_global_middleware_reaches_builtin_and_business(mkapp):
    """缺口修复的核心：同一份全站链，内置路由与业务路由**都**吃得到。

    修复前 ``app/controller/__mid__.py`` 的链只覆盖业务包，``/api/user``、
    ``/api/files`` 这些内置端点一条都不跑——且**没有任何报错**。
    """
    app = mkapp("app.controller", global_middleware=[g_one, g_two])

    assert app.blueprints, "至少要装配出蓝图，否则这个断言是空转"
    for name in app.blueprints:
        assert _mids(app, name)[:2] == ["g_one", "g_two"], (
            f"{name} 没吃到全站链或顺序不对：{_mids(app, name)}"
        )


def test_global_middleware_is_the_outermost_layer(mkapp):
    """顺序契约：全站 → 包级 → 目录级，从外到内。

    内层守卫依赖外层已注入的上下文，顺序错了就读不到。
    """
    app = mkapp("app.controller", global_middleware=[g_one])

    # note 走完整链：全站 + 业务根 + api 目录
    assert _mids(app, "note") == [
        "g_one",
        "request_id",
        "client_ip",
        "lang",
        "access_note",
        "auth_middleware",
    ]
    # 内置组没有业务的根链，但同样以全站链打头
    assert _mids(app, "webx_user") == ["g_one", "auth_middleware"]


def test_global_middleware_dedups_with_package_level(mkapp):
    """迁移安全：与包级 ``__mid__.py`` 重复声明**不双跑**。

    这条让迁移可以分两步走——先把中间件加进全站链，再慢慢从各包的
    ``__mid__.py`` 里删；中间态两边都留着也不会执行两遍。
    依赖 `_deduplicate` 的 identity 去重（同一个函数对象）。
    """
    from webx.middleware import client_ip, lang, request_id

    app = mkapp("app.controller", global_middleware=[request_id, client_ip, lang])

    chain = _mids(app, "note")
    for m in ("request_id", "client_ip", "lang"):
        assert chain.count(m) == 1, f"{m} 被挂了 {chain.count(m)} 次：{chain}"
    # 业务根 __mid__.py 里独有的那个仍在，且排在全站链之后
    assert chain == ["request_id", "client_ip", "lang", "access_note", "auth_middleware"]


def test_global_middleware_does_not_lock_public_endpoints(mkapp):
    """准入规则的反向不变量：全站链**不得**给公开端点带上认证。

    与 `test_no_api_blueprint_lacks_auth` 配成一对（那条管「该有的不能少」，
    这条管「不该有的不能多」）。全站链只放幂等的上下文注入，任何做准入决策的
    中间件都不准进——它对所有包生效，包括 ``/auth/login`` 与签名下载端点，
    一行配置就能锁死全站。
    """
    app = mkapp("app.controller", global_middleware=[g_one, g_two])

    auth_like = {"auth_middleware", "admin_auth_middleware", "role_perm_middleware"}
    for name in ("webx_auth", "webx_public_file"):
        assert not (set(_mids(app, name)) & auth_like), (
            f"公开组 {name} 被全站链带上了认证中间件：{_mids(app, name)}"
        )


# ==================== D：多业务包 ====================


def test_multiple_controller_packages_are_independent(mkapp, biz_pkgs):
    """两个业务包同时装配：各自继承自己的根 ``__mid__.py``，互不影响。

    中间件是逐个**复制**到蓝图对象上的，不存在共享容器，所以隔离是结构性的——
    这也是多包部署不需要任何额外隔离机制的原因。
    """
    a, b = biz_pkgs(
        {
            "__mid__.py": "async def a_mw(request): return None\nMIDDLEWARES = [a_mw]\n",
            "one.py": _BP_SRC.format(n="pkga_one", p="/pkga/one"),
        },
        {
            "__mid__.py": "async def b_mw(request): return None\nMIDDLEWARES = [b_mw]\n",
            "two.py": _BP_SRC.format(n="pkgb_two", p="/pkgb/two"),
        },
    )
    app = mkapp([a, b], builtin_controller=False, global_middleware=[g_one])

    assert set(app.blueprints) == {"pkga_one", "pkgb_two"}
    assert _mids(app, "pkga_one") == ["g_one", "a_mw"], "只该拿到自己包的链"
    assert _mids(app, "pkgb_two") == ["g_one", "b_mw"], "b 的链不得渗进 a，反之亦然"


def test_single_package_string_still_works(mkapp):
    """向后兼容：``controller_package`` 传字符串的老写法一行都不用改。"""
    app = mkapp("app.controller", builtin_controller=False)
    assert set(app.blueprints) == {"note", "public"}


# ==================== G：modules —— 按约定装配，业务不写入口模块 ====================


def test_modules_expands_controller_and_loads_i18n(mkapp, biz_pkgs):
    """``modules=["pkg"]`` 展开为 ``pkg.controller``（扫蓝图）+ ``pkg.i18n``（注册文案）。

    这两件事正是 ``app/main.py`` 里手写的那两行。有了约定，业务只要把目录摆成
    这个样子就能被装配，**不需要入口模块**。
    """
    import webx.i18n as I

    (pkg,) = biz_pkgs(
        {
            "controller/one.py": _BP_SRC.format(n="mod_one", p="/mod/one"),
            "i18n.py": "from webx.i18n import register\n"
            "register('zh', {'_probe_key': '文案已注册'})\n",
        }
    )
    try:
        app = mkapp(None, modules=[pkg], builtin_controller=False)

        assert set(app.blueprints) == {"mod_one"}, "controller 包要被扫到"
        assert I._MESSAGES["zh"].get("_probe_key") == "文案已注册", "i18n 要被 import"
    finally:
        I._MESSAGES.get("zh", {}).pop("_probe_key", None)


def test_models_follow_the_modules(mkapp, biz_pkgs):
    """**模型跟随模块**：装配哪个模块就带上它的 ``<pkg>/models``，没有任何配置项。

    装了 appx 的路由却漏了 appx.models，表压根不会建，要等到第一次查询才以晦涩的
    "no such table" 暴露。让这两件事绑在一起走，是多模块部署下唯一能杜绝该错的办法。
    """
    from webx.config import resolved_models

    (pkg,) = biz_pkgs(
        {
            "controller/one.py": _BP_SRC.format(n="mod_withmodels", p="/mod/m"),
            "models.py": "# 空模型模块，存在即可\n",
        }
    )
    mkapp(None, modules=["app", pkg], builtin_controller=False)

    assert resolved_models() == ["app.models", f"{pkg}.models"]


def test_webx_models_follow_the_builtin_switch(mkapp):
    """``webx`` 自己也走同一套约定——挂了内置路由就带上 ``webx/models``，关了就不带。

    这是「webx 没有特殊待遇」的具体体现：它就是一个模块，模型跟着它是否被装配走，
    而不是靠业务在配置里声明一行 ``"webx.models"``。
    """
    from webx.config import resolved_models

    mkapp(None, modules=["app"], builtin_controller=True)
    assert resolved_models() == ["app.models", "webx.models"]

    mkapp(None, modules=["app"], builtin_controller=False)
    assert resolved_models() == ["app.models"], "关掉内置路由就不该再带 webx.models"


def test_service_and_cli_resolve_the_same_models(mkapp, biz_pkgs):
    """**服务进程与 CLI 必须算出同一份模型集合。**

    服务读 ``WEBX_MODULES`` 装配路由（run.py / server.sh），``tools/dbsync.py`` 据同一
    份清单建表。两边若各算各的，就会出现「``WEBX_MODULES=app,appx`` 起的服务路由齐全，
    但 dbsync 只建了 app 的表」——接口一查库就 no such table，而建表那一步毫无征兆。

    所以两边共用 `webx.config.apply_module_models`，本用例把这个同源关系钉死。
    """
    import pathlib

    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "tools"))
    import _bootstrap  # noqa: E402

    from webx.config import resolved_models

    (pkg,) = biz_pkgs(
        {
            "controller/one.py": _BP_SRC.format(n="mod_same", p="/mod/s"),
            "models.py": "# 空模型模块，存在即可\n",
        }
    )

    mkapp(None, modules=["app", pkg], builtin_controller=False)
    by_service = resolved_models()

    # CLI 路径（等价于 dbsync --modules，或环境变量 WEBX_MODULES）
    _bootstrap.load_settings(f"app,{pkg}")
    by_cli = resolved_models()

    assert by_service == ["app.models", f"{pkg}.models"]
    # CLI 未显式关内置路由，故按同一规则多带 webx.models——两边的**规则**一致
    assert by_cli == ["app.models", f"{pkg}.models", "webx.models"]


def test_module_without_models_is_skipped(mkapp, biz_pkgs):
    """模块没有 ``models`` 就跳过——「不存在是合法配置」。

    这也是**禁用的正道**：不想要某个模块的模型，把 ``models.py`` 改名成
    ``_models.py`` 或删掉即可（``find_spec`` 探测不到），而不是去配黑名单。
    """
    from webx.config import resolved_models

    (pkg,) = biz_pkgs({"controller/one.py": _BP_SRC.format(n="mod_nomodels", p="/mod/n")})
    mkapp(None, modules=[pkg], builtin_controller=False)

    assert resolved_models() == []


def test_modules_without_i18n_is_legal(mkapp, biz_pkgs):
    """``i18n`` 是可选的：不存在就跳过，与「``__mid__.py`` 不存在是合法配置」同构。"""
    (pkg,) = biz_pkgs({"controller/one.py": _BP_SRC.format(n="mod_noi18n", p="/mod/x")})
    app = mkapp(None, modules=[pkg], builtin_controller=False)
    assert set(app.blueprints) == {"mod_noi18n"}


def test_modules_missing_controller_is_a_loud_error(mkapp, biz_pkgs):
    """``controller`` 是**硬约定**：缺了就报错，绝不静默跳过。

    静默跳过的后果是「装配成功但这个模块一个接口都没有」——服务起得来、
    健康检查全绿、接口全 404，与 `MiddlewareConfigError` 那条「零路由空服务会
    骗过健康检查」是同一类事故。
    """
    (pkg,) = biz_pkgs({"i18n.py": "# 有 i18n 但没有 controller\n"})
    with pytest.raises(ImportError, match="controller"):
        mkapp(None, modules=[pkg], builtin_controller=False)


def test_duplicate_package_is_scanned_once(mkapp, caplog):
    """同一个 controller 包被指定两次 → **只扫一次**，并留一条 WARNING。

    重复扫描的后果是**静默双挂中间件**：蓝图是模块级单例，第二次 import 命中缓存
    拿到同一批对象，于是 ``bp.middleware()`` 被调两次、链直接翻倍；而
    ``app.blueprint()`` 的二次注册被 Sanic 静默跳过，蓝图数看着还正常。

    三道守卫全都拦不住它——``seen`` 是单次调用内的，名字冲突判的是
    ``existing is not bp``（同一对象，放行）。所以只能在扫描入口去重。

    触发方式很日常：``controller_package="app.controller"`` 与 ``modules=["app"]``
    并写，或 ``modules=["webx"]`` 与 ``builtin_controller`` 重叠。
    """
    with caplog.at_level("WARNING"):
        app = mkapp("app.controller", modules=["app"], builtin_controller=False)

    assert _mids(app, "note") == [
        "request_id",
        "client_ip",
        "lang",
        "access_note",
        "auth_middleware",
    ], "中间件链不得因重复扫描而翻倍"
    assert "app.controller" in caplog.text and "重复" in caplog.text, "必须留声"


def test_builtin_package_overlapping_modules_is_deduped(mkapp, caplog):
    """``modules=["webx"]`` 与 ``builtin_controller`` 指向同一个包时同样只扫一次。"""
    with caplog.at_level("WARNING"):
        app = mkapp(None, modules=["app", "webx"], builtin_controller=True)

    assert _mids(app, "webx_user") == ["auth_middleware"], "内置组的链不得翻倍"
    assert "webx.controller" in caplog.text


def test_modules_and_controller_package_coexist(mkapp, biz_pkgs):
    """两条路径可并存：老项目逐个模块迁移时不必一次性切换。"""
    (pkg,) = biz_pkgs({"controller/one.py": _BP_SRC.format(n="mod_mix", p="/mod/mix")})
    app = mkapp("app.controller", modules=[pkg], builtin_controller=False)
    assert set(app.blueprints) == {"mod_mix", "note", "public"}


def test_modules_path_matches_explicit_package(mkapp):
    """``modules=["app"]`` 与 ``controller_package="app.controller"`` 装配结果一致。

    这是删掉 ``app/main.py`` 的正当性依据：换了装配入口，产出不变。
    """
    explicit = mkapp("app.controller")
    by_module = mkapp(None, modules=["app"])

    assert set(explicit.blueprints) == set(by_module.blueprints)
    for n in explicit.blueprints:
        assert _mids(explicit, n) == _mids(by_module, n), f"{n} 的中间件链不一致"


# ==================== E：跨扫描根的蓝图撞名 ====================


def test_name_conflict_across_builtin_and_business(mkapp, biz_pkgs):
    """业务包取了内置蓝图的名字 → 启动即失败，且**点名双方**。

    这是多包部署最现实的撞车。守卫查的是 ``app.blueprints`` 而非某次扫描内的
    ``seen``，故跨扫描根同样抓得到（内置组与业务包是不同的调用）。
    """
    (pkg,) = biz_pkgs({"clash.py": _BP_SRC.format(n="webx_user", p="/biz/user")})

    with pytest.raises(BlueprintNameConflictError) as ei:
        mkapp(pkg, builtin_controller=["api"])

    msg = str(ei.value)
    assert "webx.controller.api.user" in msg, f"要点名内置那边的定义处: {msg}"
    assert f"{pkg}.clash" in msg, f"要点名业务那边的定义处: {msg}"
    assert "/api/user" in msg and "/biz/user" in msg, f"两个前缀都要给出: {msg}"


# ==================== F：admin 权限内务只在挂载时执行 ====================


@pytest.fixture(autouse=True)
def _clean_permission_registry():
    """权限注册表是模块级全局字典，测试之间必须隔离。"""
    perm_registry.clear()
    yield
    perm_registry.clear()


def test_admin_permissions_registered_when_admin_mounted(mkapp):
    """挂了 admin 组 → 内置权限定义自动进注册表，业务不用写一行。"""
    mkapp("app.controller", builtin_controller=["admin"])
    assert perm_registry.PERMISSIONS, "挂了 admin 却没注册任何内置权限"


def test_admin_permissions_skipped_when_admin_not_mounted(mkapp):
    """**没挂 admin → 一件都不做**。

    这正是下沉前的错误：业务在自己的 ``main.py`` 里无条件调
    ``register_admin_permissions()``，于是 ``builtin_controller=False``
    （框架推荐的新服务起步姿势）时会注册一堆没有对应路由的权限，
    启动钩子还会往库里写一堆用不到的记录。
    """
    mkapp("app.controller", builtin_controller=False)
    assert not perm_registry.PERMISSIONS, "没挂 admin 不该注册任何内置权限"

    mkapp("app.controller", builtin_controller=["api", "auth", "public"])
    assert not perm_registry.PERMISSIONS, "只挂非 admin 组同样不该注册"


def test_startup_never_syncs_permissions_to_db(mkapp, monkeypatch):
    """**装配只写内存注册表，绝不碰数据库里的权限表。**

    同步（``init_permissions``）会按代码删除库里已废弃的权限，是运维动作，只能由
    ``python tools/org.py init-perm`` 手动执行。让它跟着每次启动自动跑有两重问题：

      · 一次误删的影响面从「运维敲错命令」扩大到「任何一次重启」；
      · 启动钩子是**每个 worker 各跑一次**（实测 ``--workers 3`` → 执行 3 次），
        而 ``Permission`` 有 ``unique_together=(name, orgid)``、``_sync_one`` 又是
        check-then-act，多 worker 首次启动会并发写同一批记录。

    两种 ``use_db`` 都要验：不是「没连库所以没同步」，而是**压根不该同步**。
    """
    import webx.services.init_permission as ip

    called: list[dict] = []

    async def _spy(**kw):
        called.append(kw)

    monkeypatch.setattr(ip, "init_permissions", _spy)

    for use_db in (False, True):
        app = mkapp("app.controller", builtin_controller=["admin"], use_db=use_db)
        _, resp = app.test_client.get("/health")
        assert resp.status == 200, f"use_db={use_db} 时应当正常启动"

    assert perm_registry.PERMISSIONS, "内存注册表照常填好（RBAC 中间件运行时要查）"
    assert called == [], "启动过程不得触发任何权限同步"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))


def test_builtin_summary_is_logged(mkapp, caplog):
    """启动必须明说「实际挂了哪些内置组」——默认全挂是有意的，提示是唯一的可见性。"""
    with caplog.at_level("INFO"):
        mkapp(None, modules=["app"], builtin_controller=True)
    assert "内置路由组: auth,api,admin,public" in caplog.text
    assert "admin 组已挂载" in caplog.text, "挂了 admin 要多提一句它带什么、怎么关"

    caplog.clear()
    with caplog.at_level("INFO"):
        mkapp(None, modules=["app"], builtin_controller=["auth", "api"])
    assert "内置路由组: auth,api" in caplog.text
    assert "admin 组已挂载" not in caplog.text

    caplog.clear()
    with caplog.at_level("INFO"):
        mkapp(None, modules=["app"], builtin_controller=False)
    assert "内置路由组: 无" in caplog.text


def test_all_invalid_builtin_groups_fallback_warning_is_explicit(mkapp, caplog):
    """配错回退为全加载时，WARNING 要写明「实际挂了什么」，而不只是「无效」。"""
    with caplog.at_level("WARNING"):
        mkapp(None, modules=["app"], builtin_controller=["nope"])
    assert "全部加载" in caplog.text
    assert "auth,api,admin,public" in caplog.text and "admin" in caplog.text
