"""蓝图注册的结构性契约 —— 嵌套子目录的中间件继承 + 跨模块蓝图归属。

【契约 A：嵌套继承是累积的】
  子目录的中间件链 = 全部祖先目录的链 + 自己的链，按「从外到内」顺序。
  内层守卫（如 inner_mw）依赖 auth 已注入 ctx.user，顺序错了守卫就读不到用户。

【契约 B：一个 Blueprint 对象只能归属一个模块】
  `_find_blueprints` 在没有 ``__blueprints__`` 时扫描模块的**全部属性**，
  凡 Blueprint 实例一律收走——**包括 import 进来的别人家的蓝图**。命中后
  `_apply_and_register` 会把本组中间件挂到那个共享对象上。

  实测（sanic 25.12，修复前）：``api/sys/x.py`` 里一句 ``from ..task import task_bp``
  → ``/api/tasks`` 的链被多挂一遍并追加 inner 守卫，普通用户全体 404；
  ``app.blueprint()`` 二次注册被 Sanic 同名跳过，**不抛异常、不打警告、日志无痕**。

  修复后：归属判定按路由 handler 的 ``__module__`` 认出外来蓝图并无声跳过；
  管不到的两种情况（显式 ``__blueprints__`` / 判据失真）分别由 seen 查重与
  孤儿核对兜底，最坏结果都是**启动失败**，绝不静默改动鉴权链。

【契约 E：webx 特有 —— create_app 会多次调用注册器】
  ``seen`` / ``skipped`` 是单次调用内的状态，故孤儿核对必须对「owner 不在本次
  扫描根之下」的蓝图让步，否则内置组之间的正常 import 会误抛 OrphanBlueprintError。
"""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sanic import Sanic

from webx.core import auto_blueprint
from webx.core.auto_blueprint import (
    BlueprintNameConflictError,
    DuplicateBlueprintError,
    OrphanBlueprintError,
    auto_register_blueprints,
)

_counter = {"n": 0}

_BP_SRC = (
    "from sanic import Blueprint\n"
    "from sanic.response import text\n"
    '{n}_bp = Blueprint("{n}", url_prefix="{p}")\n'
    '@{n}_bp.get("/ping")\n'
    "async def ping(request):\n"
    '    return text("{n}")\n'
)


def _mid(*names: str) -> str:
    """生成一个 ``__mid__.py`` 源码：每个名字造一个同名中间件函数。"""
    defs = "".join(f"async def {n}(request):\n    return None\n" for n in names)
    return defs + f"MIDDLEWARES = [{', '.join(names)}]\n"


def _mkpkg(base: Path, *rel_dirs: str) -> None:
    for d in rel_dirs:
        (base / d).mkdir(parents=True, exist_ok=True)
        (base / d / "__init__.py").write_text("")


@pytest.fixture
def tree():
    """搭一棵三层树，产出 ``build(extra) -> (app, 包名)``::

        <pkg>/
        ├── __mid__.py        []                       ← 根，恒空
        └── api/
            ├── __mid__.py    [auth_mw, taskid_mw]
            ├── task.py       task_bp     /api/tasks   ← 普通用户组，必须保持干净
            └── sys/
                ├── __mid__.py  [inner_mw]
                └── notice.py   sys_notice_bp  /api/sys/notice

    ``extra`` 用于额外塞文件（键是相对 ``<pkg>/`` 的路径，源码里 ``<PKG>`` 会被替换）。
    每次用独立包名与独立 Sanic 实例，避免 ``sys.modules`` / Sanic 应用注册表串味。
    """
    tmp = Path(tempfile.mkdtemp())
    _counter["n"] += 1
    pkg = f"regtree{_counter['n']}"
    app_name = f"regapp{_counter['n']}"
    sys.path.insert(0, str(tmp))
    base = tmp / pkg

    def build(extra: dict[str, str] | None = None):
        _mkpkg(base, "", "api", "api/sys")
        (base / "__mid__.py").write_text("MIDDLEWARES = []\n")
        (base / "api" / "__mid__.py").write_text(_mid("auth_mw", "taskid_mw"))
        (base / "api" / "sys" / "__mid__.py").write_text(_mid("inner_mw"))
        (base / "api" / "task.py").write_text(_BP_SRC.format(n="task", p="/api/tasks"))
        (base / "api" / "sys" / "notice.py").write_text(
            _BP_SRC.format(n="sys_notice", p="/api/sys/notice")
        )
        for rel, src in (extra or {}).items():
            (base / rel).write_text(src.replace("<PKG>", pkg))
        return Sanic(app_name), pkg

    yield build

    for m in [m for m in sys.modules if m == pkg or m.startswith(pkg + ".")]:
        sys.modules.pop(m, None)
    sys.path.remove(str(tmp))
    Sanic._app_registry.pop(app_name, None)
    shutil.rmtree(tmp, ignore_errors=True)


def _mids_of(app: Sanic, name: str) -> list[str]:
    return [fm.middleware.func.__name__ for fm in app.blueprints[name]._future_middleware]


# ==================== 契约 A：嵌套继承累积且有序 ====================


def test_nested_dir_middlewares_accumulate_in_order(tree):
    """二级子目录 = 父链 + 自己的链，顺序从外到内（inner 守卫必须排在 auth 之后）。"""
    app, pkg = tree()
    auto_register_blueprints(app, pkg)

    assert _mids_of(app, "sys_notice") == ["auth_mw", "taskid_mw", "inner_mw"]
    # 父组不受子目录追加的影响——子目录的 __mid__ 绝不能向上渗透
    assert _mids_of(app, "task") == ["auth_mw", "taskid_mw"]


def test_nested_dir_named_sys_does_not_shadow_stdlib(tree):
    """目录名取 ``sys`` 不遮蔽标准库：绝对导入下 ``<pkg>.api.sys`` 与 stdlib ``sys`` 无关。"""
    app, pkg = tree()
    auto_register_blueprints(app, pkg)

    assert "sys_notice" in app.blueprints
    assert sys.modules["sys"].__name__ == "sys"  # stdlib 仍是那个 stdlib
    assert not hasattr(sys, "sys_notice_bp")  # 没有被写串


# ==================== 契约 B：跨模块蓝图污染必须被消除或中止 ====================

_POLLUTE = "from <PKG>.api.task import task_bp\n" + _BP_SRC.format(  # ← 病灶
    n="sys_task", p="/api/sys/task"
)


def test_cross_module_blueprint_import_is_silently_skipped(tree):
    """import 别组蓝图 → 归属判定认出它、**无声跳过**，装配照常完成。

    判据是路由 handler 的 ``__module__``：``api/task.py`` 定义的蓝图，其 handler
    属于 ``...api.task``；``api/sys/pollute.py`` 扫到它时一看 handler 不出自本模块，
    就知道这是 import 进来的，直接不收。于是本组的中间件根本没机会挂上去 ——
    事故被**消除**，而不是被检测。
    """
    app, pkg = tree(extra={"api/sys/pollute.py": _POLLUTE})
    auto_register_blueprints(app, pkg)  # 不抛

    # 双方的链都必须正确：普通组没被挂上 inner，sys 组该有的一个不少
    assert _mids_of(app, "task") == ["auth_mw", "taskid_mw"]
    assert _mids_of(app, "sys_task") == ["auth_mw", "taskid_mw", "inner_mw"]


def test_reverse_direction_sys_bp_keeps_its_guard(tree):
    """**致命方向**：宽松组 import 严格组的蓝图，sys 蓝图绝不能因此丢掉 inner 守卫。

    修复前：``api/aaa.py``（字母序靠前，先被扫到）会把 sys 蓝图提前注册，
    那一刻继承链里还没有 inner_mw；等扫到 ``api/sys/`` 时二次注册被 Sanic 跳过，
    守卫再也挂不上 —— 内部接口对所有登录用户敞开。
    """
    app, pkg = tree(
        extra={
            "api/aaa.py": "from <PKG>.api.sys.notice import sys_notice_bp\n"
            + _BP_SRC.format(n="aaa", p="/api/aaa"),
        }
    )
    auto_register_blueprints(app, pkg)  # 不抛

    assert _mids_of(app, "sys_notice") == [
        "auth_mw",
        "taskid_mw",
        "inner_mw",
    ], "sys 蓝图必须保住 inner 守卫"
    assert _mids_of(app, "aaa") == ["auth_mw", "taskid_mw"]


# ---- 兜底：归属判定管不到的两种情况，仍由 seen 查重拦截 ----


def test_explicit_blueprints_declaring_foreign_bp_is_still_caught(tree):
    """显式 ``__blueprints__`` = 完全信任作者，**不做**归属判定 —— 故仍需 seen 兜底。

    作者若把别人的蓝图写进 ``__blueprints__``，那是明确的错误意图，不是手滑，
    此时中止启动是对的。
    """
    app, pkg = tree(
        extra={
            "api/sys/pollute.py": _POLLUTE + "__blueprints__ = [task_bp, sys_task_bp]\n",
        }
    )
    with pytest.raises(DuplicateBlueprintError) as ei:
        auto_register_blueprints(app, pkg)

    msg = str(ei.value)
    assert "task" in msg and "api.sys.pollute" in msg  # 病灶与受害者都点名
    assert "__blueprints__" in msg  # 且给出修法


def test_orphan_blueprint_aborts_startup(tree):
    """归属判定唯一的新风险：判错 → 蓝图无人认领 → **接口静默消失**。必须拦下。

    构造法：蓝图建在 ``orphan_bp.py``，但它的 handler 定义在 ``handlers.py``。
    于是每个扫到它的模块都认为「handler 不出自我，不是我的」而跳过它。
    """
    app, pkg = tree(
        extra={
            "handlers.py": "from sanic.response import text\n"
            "async def shared(request):\n    return text('x')\n",
            "api/sys/orphan_bp.py": "from sanic import Blueprint\n"
            "from <PKG>.handlers import shared\n"
            'orphan_bp = Blueprint("orphan", url_prefix="/api/sys/orphan")\n'
            'orphan_bp.add_route(shared, "/ping")\n',
        }
    )
    with pytest.raises(OrphanBlueprintError) as ei:
        auto_register_blueprints(app, pkg)

    assert "orphan" in str(ei.value)
    assert "__blueprints__" in str(ei.value)  # 给出修法


def test_handler_rewritten_by_foreign_decorator_becomes_a_loud_failure(tree):
    """**假阴性**（本该拥有却判成外来）：判据失真的最现实成因，必须吵闹地失败。

    成因：一个定义在**别的模块**、且**没用 functools.wraps** 的装饰器包住 handler，
    于是 ``handler.__module__`` 指向装饰器所在模块，而不是蓝图所在模块。
    此时该蓝图被每个模块判为「不是我的」→ 无人注册。孤儿核对把它变成
    **启动失败**，而不是「接口悄悄 404」。判据可以不稳，失败方式不能不响。
    """
    app, pkg = tree(
        extra={
            "baddeco.py": "def wrap(f):\n"
            "    async def inner(*a, **k):\n        return await f(*a, **k)\n"
            "    return inner\n",  # ← 故意不用 @wraps
            "api/sys/decorated.py": "from sanic import Blueprint\n"
            "from sanic.response import text\n"
            "from <PKG>.baddeco import wrap\n"
            'deco_bp = Blueprint("deco", url_prefix="/api/sys/deco")\n'
            '@deco_bp.get("/ping")\n'
            "@wrap\n"
            "async def ping(request):\n    return text('x')\n",
        }
    )
    with pytest.raises(OrphanBlueprintError) as ei:
        auto_register_blueprints(app, pkg)
    assert "deco" in str(ei.value)


def test_false_positive_direction_is_caught_by_seen(tree):
    """**假阳性**（不该拥有却判成拥有）：退化回旧的重复注册，由 seen 查重兜底。

    构造：``api/sys/adder.py`` import 别组蓝图，并把**本模块定义的** handler 加上去。
    该 handler 的 ``__module__`` 正是本模块 → 归属判定放行 → 两个模块都认领它。

    两个方向合起来的性质：**判据不稳定不会造成静默的鉴权问题**——
    要么被孤儿核对拦下，要么被 seen 拦下，最坏结果都是启动失败。
    """
    app, pkg = tree(
        extra={
            "api/sys/adder.py": "from sanic.response import text\n"
            "from <PKG>.api.task import task_bp\n"
            "async def extra(request):\n    return text('x')\n"
            'task_bp.add_route(extra, "/extra")\n',
        }
    )
    with pytest.raises(DuplicateBlueprintError):
        auto_register_blueprints(app, pkg)


def test_orphan_check_does_not_escalate_a_skipped_group(tree, caplog):
    """**两类错误的交叉点**：owner 组因 ``__mid__.py`` 失败被整组跳过时，孤儿核对必须让步。

    场景：``api/sys/__mid__.py`` 坏了 → 按既有契约（test_auto_blueprint_mid_failure
    契约 3）整个 sys 子树跳过、父组与兄弟组照常注册。而 ``api/aaa.py`` import 了
    sys 的蓝图 —— Python import 会执行 owner 模块，蓝图对象因此存在并被登记为
    foreign；可它的 owner 目录已被跳过，于是「无人注册」。

    若此时抛 OrphanBlueprintError，就把「一个组缺失、服务照常」升级成
    「整个应用起不来」——严格劣于既有契约。正确行为：只记 ERROR，应用照常启动。
    """
    app, pkg = tree(
        extra={
            "api/sys/__mid__.py": "from __definitely_not_real__ import x\nMIDDLEWARES=[x]\n",
            "api/aaa.py": "from <PKG>.api.sys.notice import sys_notice_bp\n"
            + _BP_SRC.format(n="aaa", p="/api/aaa"),
        }
    )
    with caplog.at_level("ERROR"):
        auto_register_blueprints(app, pkg)  # 必须不抛

    # 契约 3 依旧成立：sys 组整体缺失，父组与兄弟组完好
    assert "sys_notice" not in app.blueprints
    assert {"task", "aaa"} <= set(app.blueprints)
    assert _mids_of(app, "task") == ["auth_mw", "taskid_mw"]
    # 但必须留声，且说清这是哪一类问题
    assert "sys_notice" in caplog.text and "__mid__" in caplog.text


def test_orphan_is_resolved_by_explicit_declaration(tree):
    """孤儿的修法确实有效：真正拥有它的模块显式声明后，一切正常。"""
    app, pkg = tree(
        extra={
            "handlers.py": "from sanic.response import text\n"
            "async def shared(request):\n    return text('x')\n",
            "api/sys/orphan_bp.py": "from sanic import Blueprint\n"
            "from <PKG>.handlers import shared\n"
            'orphan_bp = Blueprint("orphan", url_prefix="/api/sys/orphan")\n'
            'orphan_bp.add_route(shared, "/ping")\n'
            "__blueprints__ = [orphan_bp]\n",
        }
    )
    auto_register_blueprints(app, pkg)  # 不抛

    assert _mids_of(app, "orphan") == ["auth_mw", "taskid_mw", "inner_mw"]


def test_explicit_blueprints_declaration_is_the_escape_hatch(tree):
    """显式 ``__blueprints__`` 后，import 别组蓝图（为复用常量/函数）不再误伤。"""
    app, pkg = tree(
        extra={"api/sys/pollute.py": _POLLUTE + "__blueprints__ = [sys_task_bp]\n"}
    )
    auto_register_blueprints(app, pkg)  # 不抛

    assert _mids_of(app, "sys_task") == ["auth_mw", "taskid_mw", "inner_mw"]
    assert _mids_of(app, "task") == ["auth_mw", "taskid_mw"]


def test_blueprint_factory_still_works_with_ownership_filter(tree):
    """webx 特有：``__blueprints__`` 的**工厂形态**不能被归属判定/查重破坏。

    工厂是「同一 controller 包注册进多个 app」的唯一正确写法（每个 app 拿到全新
    实例，不会撞 RouteExists）。它走显式声明分支 → 跳过归属判定；且每次返回新
    对象 → id 各不相同，seen 查重天然不误报。
    """
    app, pkg = tree(
        extra={
            "api/sys/factory.py": "from sanic import Blueprint\n"
            "from sanic.response import text\n"
            "def make():\n"
            '    bp = Blueprint("made", url_prefix="/api/sys/made")\n'
            '    @bp.get("/ping")\n'
            "    async def ping(request):\n        return text('x')\n"
            "    return [bp]\n"
            "__blueprints__ = make\n",
        }
    )
    auto_register_blueprints(app, pkg)  # 不抛

    assert _mids_of(app, "made") == ["auth_mw", "taskid_mw", "inner_mw"]


# ==================== 契约 C：扫描顺序是确定的、父先于子 ====================


def test_scan_order_is_parent_files_then_subdirs(tree, monkeypatch):
    """**顺序契约**：同一目录内先按文件名 ``sorted`` 处理 .py，全部处理完才递归子目录。

    这条不是「实现碰巧如此」，而是必须长期保持的性质，因为它决定了
    「同一个蓝图被两处发现时，哪一边的中间件链先生效」——见
    `DuplicateBlueprintError` 里两个污染方向的后果分析。
    """
    order = []
    orig = auto_blueprint._apply_and_register
    monkeypatch.setattr(
        auto_blueprint,
        "_apply_and_register",
        lambda app, bp, mids, source, seen=None: (
            order.append(source),
            orig(app, bp, mids, source, seen),
        )[1],
    )

    app, pkg = tree(
        extra={
            "api/z_last.py": _BP_SRC.format(n="z_last", p="/api/z"),
            "api/a_first.py": _BP_SRC.format(n="a_first", p="/api/a"),
        }
    )
    auto_register_blueprints(app, pkg)

    tails = [s.rsplit(".", 2)[-2:] for s in order]  # 只看 (目录, 模块)
    assert tails == [
        ["api", "a_first"],  # ① 本目录文件，按文件名排序
        ["api", "task"],
        ["api", "z_last"],
        ["sys", "notice"],  # ② 本目录文件全部处理完，才进子目录
    ], order


# ==================== 契约 D：只有蓝图对象会触发守卫，不误伤正常复用 ====================


@pytest.mark.parametrize(
    "stmt,label",
    [
        ("from <PKG>.api.task import ping", "import 父目录的函数"),
        ("from <PKG>.api import task", "import 父目录的模块对象"),
        ("import <PKG>.api.task", "import 整个父模块路径"),
        ("from <PKG>.services_stub import CONST", "import 普通常量"),
    ],
)
def test_importing_parent_non_blueprint_never_trips_the_guard(tree, stmt, label):
    """子目录 import 父级模块的**非蓝图符号**一律放行——这是预期的代码复用，不是污染。"""
    app, pkg = tree(
        extra={
            "services_stub.py": "CONST = 1\n",
            "api/sys/reuse.py": stmt
            + "\n"
            + _BP_SRC.format(n="sys_reuse", p="/api/sys/reuse"),
        }
    )
    auto_register_blueprints(app, pkg)  # 不抛

    assert _mids_of(app, "sys_reuse") == ["auth_mw", "taskid_mw", "inner_mw"], label
    assert _mids_of(app, "task") == ["auth_mw", "taskid_mw"], label


def test_star_import_is_silently_skipped_not_an_error(tree):
    """星号导入把别组蓝图带进了命名空间 —— 归属判定认出它、无声跳过，**不报错**。"""
    app, pkg = tree(
        extra={
            "api/sys/reuse.py": "from <PKG>.api.task import *\n"
            + _BP_SRC.format(n="sys_reuse", p="/api/sys/reuse"),
        }
    )
    auto_register_blueprints(app, pkg)  # 不抛

    assert _mids_of(app, "task") == ["auth_mw", "taskid_mw"], "普通组不得被挂上 inner"
    assert _mids_of(app, "sys_reuse") == ["auth_mw", "taskid_mw", "inner_mw"]
    assert _mids_of(app, "sys_notice") == ["auth_mw", "taskid_mw", "inner_mw"]


def test_duplicate_blueprint_name_reports_both_owners(tree):
    """**不同对象、同名字** —— 在 ``app.blueprint()`` 之前拦下，并点名双方定义处。

    `DuplicateBlueprintError` 按 ``id(bp)`` 查重，抓的是「同一个对象被两处发现」；
    两个各自独立的蓝图即使同名也 id 不同、那道守卫放行。Sanic 自己会抛
    ``AssertionError: Blueprint names must be unique``——同样是启动即崩、安全，
    但它**只报名字、不报哪两个模块**。单包时 grep 一下就找到，多包部署时得在
    几个包里翻，故这里提前拦截，用归属判定反查出双方。

    由此得出命名约定：**蓝图名是全局唯一命名空间，与 URL 前缀无关**，
    多包部署按包加前缀（内置的已统一为 ``webx_``）。
    """
    app, pkg = tree(extra={"api/sys/dupname.py": _BP_SRC.format(n="task", p="/api/sys/task")})
    with pytest.raises(BlueprintNameConflictError) as ei:
        auto_register_blueprints(app, pkg)

    msg = str(ei.value)
    assert "api.task" in msg and "api.sys.dupname" in msg, f"双方都要点名: {msg}"
    assert "/api/tasks" in msg and "/api/sys/task" in msg, f"两个前缀都要给出: {msg}"

    # 先注册的普通用户组毫发无损（文件先于子目录扫描）
    assert _mids_of(app, "task") == ["auth_mw", "taskid_mw"]
    assert app.blueprints["task"].url_prefix == "/api/tasks"


def test_same_object_across_sibling_modules_is_skipped(tree):
    """兄弟模块之间的 import 同样由归属判定处理：定义方注册，import 方跳过。"""
    app, pkg = tree(
        extra={
            "api/sys/a_first.py": _BP_SRC.format(n="shared", p="/api/sys/shared"),
            "api/sys/b_second.py": "from <PKG>.api.sys.a_first import shared_bp\n"
            + _BP_SRC.format(n="second", p="/api/sys/second"),
        }
    )
    auto_register_blueprints(app, pkg)  # 不抛

    # 只注册一次，且链正确（不会因为被扫到两次而重复挂载）
    assert _mids_of(app, "shared") == ["auth_mw", "taskid_mw", "inner_mw"]
    assert _mids_of(app, "second") == ["auth_mw", "taskid_mw", "inner_mw"]


# ============ 契约 E：多次调用注册器（webx 的 create_app 就是这么干的）============


@pytest.fixture
def two_roots():
    """两个**独立的 controller 根包**，模拟 ``create_app`` 里内置组 + 业务组的多次调用::

        <pkg>a/               ← 第一次调用扫这个
        ├── __mid__.py   [a_mw]
        └── auth.py      auth_bp   /a/auth
        <pkg>b/               ← 第二次调用扫这个
        ├── __mid__.py   [b_mw]
        └── api.py       ← import 了 a 包的 auth_bp（正常的跨组复用写法之外的误 import）
    """
    tmp = Path(tempfile.mkdtemp())
    _counter["n"] += 1
    pa, pb = f"rootA{_counter['n']}", f"rootB{_counter['n']}"
    app_name = f"multiapp{_counter['n']}"
    sys.path.insert(0, str(tmp))

    def build(b_api_src: str, b_init_src: str = ""):
        _mkpkg(tmp / pa, "")
        _mkpkg(tmp / pb, "")
        (tmp / pa / "__mid__.py").write_text(_mid("a_mw"))
        (tmp / pa / "auth.py").write_text(_BP_SRC.format(n="auth", p="/a/auth"))
        (tmp / pb / "__mid__.py").write_text(_mid("b_mw"))
        # 放在 __init__.py 里的 handler 不会被扫描器当成控制器模块，
        # 但它的 __module__ 落在扫描根之内 —— 正好用来造「根内的真孤儿」。
        (tmp / pb / "__init__.py").write_text(b_init_src)
        (tmp / pb / "api.py").write_text(b_api_src.replace("<A>", pa))
        return Sanic(app_name), pa, pb

    yield build

    for m in [
        m
        for m in sys.modules
        if m in (pa, pb) or m.startswith(pa + ".") or m.startswith(pb + ".")
    ]:
        sys.modules.pop(m, None)
    sys.path.remove(str(tmp))
    Sanic._app_registry.pop(app_name, None)
    shutil.rmtree(tmp, ignore_errors=True)


def test_foreign_bp_owned_by_another_scan_root_is_not_an_orphan(two_roots):
    """**webx 特有的回归**：跨调用的 import 不得被误判成孤儿。

    ``seen`` / ``skipped`` 是单次 `auto_register_blueprints` 调用内的状态，而
    ``create_app`` 对每个内置路由组各调一次。B 包 import 了 A 包的蓝图时，B 那次
    调用只看得见「它被跳过」，看不见「A 那次调用已经注册过它」——若照搬上游的
    孤儿核对，这里会**误抛 OrphanBlueprintError**，把一次正常启动变成崩溃。

    正确行为：owner 不在本次扫描根之下 → 本次调用没有裁判权，只记 ERROR。
    注意归属**过滤**本身跨调用照常生效（下面断言 A 的蓝图没被挂上 B 的中间件），
    让步的只是核对，鉴权污染仍被杜绝。
    """
    app, pa, pb = two_roots(
        "from <A>.auth import auth_bp\n" + _BP_SRC.format(n="bapi", p="/b/api")
    )
    auto_register_blueprints(app, pa)
    auto_register_blueprints(app, pb)  # 必须不抛

    assert _mids_of(app, "auth") == ["a_mw"], "A 的蓝图不得被挂上 B 组的中间件"
    assert _mids_of(app, "bapi") == ["b_mw"]


def test_real_orphan_still_aborts_when_owner_is_inside_the_scan_root(two_roots):
    """让步只针对「owner 在扫描根之外」；根内的真孤儿照旧中止启动。

    与上一条配对，钉死豁免的边界不是「多次调用一律放行」。
    """
    app, pa, pb = two_roots(
        # api.py 建蓝图，但 handler 来自本包的 __init__（扫描根之内、却不是控制器模块）
        # → 每个扫到它的模块都判「不是我的」，且 owner 确实归本次调用管辖。
        "from sanic import Blueprint\n"
        "from . import shared\n"
        'ghost_bp = Blueprint("ghost", url_prefix="/b/ghost")\n'
        'ghost_bp.add_route(shared, "/ping")\n',
        "from sanic.response import text\n"
        "async def shared(request):\n    return text('x')\n",
    )
    auto_register_blueprints(app, pa)
    with pytest.raises(OrphanBlueprintError):
        auto_register_blueprints(app, pb)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
