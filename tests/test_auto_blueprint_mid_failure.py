"""``__mid__.py`` 加载失败时的隔离行为 —— 鉴权配置的失败方式只能是「不放行」。

背景：``_load_mid_file`` 里原有一句 ``except ImportError: return []``。它的本意是
「文件不存在返回空」，但那个情况在上面的 ``exists()`` 已经拦掉了，所以它实际只吞
**真实故障**——把 ``__mid__.py`` 里的一个函数名写错，就静默变成「该组全部路由无鉴权
且应用照常启动、日志无痕」。

修复后的契约（本文件逐条钉死）：

  1. ``__mid__.py`` 存在但加载失败 → 抛 ``MiddlewareConfigError``，绝不返回空列表；
  2. 某组失败 → 该组**及其全部子目录**都不注册（否则子组会带着缺失的继承链上线，
     看似正常实则少一道鉴权——比整组消失危险得多）；
  3. 子组失败 → **不影响父组与兄弟组**，它们照常注册且中间件链完整；
  4. 被跳过的组必须可见：ERROR 日志（运维发现问题的唯一渠道）；
  5. **根** ``__mid__.py`` 失败 → 直接抛，不降级（否则是零路由空服务，
     还会骗过健康检查：进程活着、端口通）；
  6. 一切正常时行为不变（真实内置 controller 装配回归）。
"""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sanic import Sanic

from webx.core.auto_blueprint import MiddlewareConfigError, auto_register_blueprints

# ---- 测试用中间件（只需可调用，auto_blueprint 不关心它做什么）----
_MID_SRC = """
async def m(request):
    return None
MIDDLEWARES = [m]
"""
_MID_EMPTY = "MIDDLEWARES = []\n"
# 导入一个不存在的模块 → ImportError（正是旧实现会静默吞掉的那类）
_MID_BROKEN_IMPORT = "from __definitely_not_a_real_module__ import x\nMIDDLEWARES = [x]\n"
# 语法错误 → SyntaxError（另一类真实故障，同样不能被当成"没配"）
_MID_BROKEN_SYNTAX = "MIDDLEWARES = [\n"


def _bp_src(name: str, prefix: str) -> str:
    """带一个真实 handler —— 空蓝图不产生路由，``router.finalize()`` 会拒绝。"""
    return (
        "from sanic import Blueprint\n"
        "from sanic.response import text\n"
        f'{name}_bp = Blueprint("{name}", url_prefix="{prefix}")\n'
        f'@{name}_bp.get("/ping")\n'
        "async def ping(request):\n"
        f'    return text("{name}")\n'
    )


_counter = {"n": 0}


def _build_tree(
    root: Path,
    pkg: str,
    *,
    root_mid=_MID_EMPTY,
    good_child_mid=_MID_BROKEN_IMPORT,
    sibling_mid=_MID_BROKEN_IMPORT,
) -> Path:
    """搭一棵覆盖「父 / 兄弟 / 子」三种位置的 controller 树::

        <pkg>/
        ├── __mid__.py            root_mid
        ├── ok.py                 ok_bp            ← 顶层兄弟，任何失败都不该波及
        ├── good/
        │   ├── __mid__.py        [m]              ← 正常组
        │   ├── g.py              good_bp
        │   └── badchild/
        │       ├── __mid__.py    good_child_mid   ← 【子目录失败点】
        │       ├── bc.py         badchild_bp
        │       └── deeper/
        │           ├── __mid__.py  空（本身没问题）
        │           └── d.py        deeper_bp      ← 必须被连坐跳过
        └── broken/
            ├── __mid__.py        sibling_mid      ← 【兄弟目录失败点】
            └── b.py              broken_bp
    """
    base = root / pkg
    for d in ("", "good", "good/badchild", "good/badchild/deeper", "broken"):
        (base / d).mkdir(parents=True, exist_ok=True)
        (base / d / "__init__.py").write_text("")
    (base / "__mid__.py").write_text(root_mid)
    (base / "ok.py").write_text(_bp_src("ok", "/ok"))
    (base / "good" / "__mid__.py").write_text(_MID_SRC)
    (base / "good" / "g.py").write_text(_bp_src("good", "/good"))
    (base / "good" / "badchild" / "__mid__.py").write_text(good_child_mid)
    (base / "good" / "badchild" / "bc.py").write_text(_bp_src("badchild", "/badchild"))
    (base / "good" / "badchild" / "deeper" / "__mid__.py").write_text(_MID_EMPTY)
    (base / "good" / "badchild" / "deeper" / "d.py").write_text(
        _bp_src("deeper", "/deeper")
    )
    (base / "broken" / "__mid__.py").write_text(sibling_mid)
    (base / "broken" / "b.py").write_text(_bp_src("broken", "/broken"))
    return base


@pytest.fixture
def tree():
    """产出 ``build(**kw) -> (app, 包名, 树根目录)``。

    每次用独立包名与独立 Sanic 实例，避免 ``sys.modules`` / Sanic 应用注册表串味
    （蓝图是模块级单例，同一模块被两个 app 注册会 RouteExists）。
    """
    tmp = Path(tempfile.mkdtemp())
    _counter["n"] += 1
    pkg = f"faketl{_counter['n']}"
    app_name = f"midtest{_counter['n']}"
    sys.path.insert(0, str(tmp))

    def build(**kw):
        base = _build_tree(tmp, pkg, **kw)
        return Sanic(app_name), pkg, base

    yield build

    sys.path.remove(str(tmp))
    for name in [m for m in sys.modules if m == pkg or m.startswith(pkg + ".")]:
        sys.modules.pop(name, None)
    Sanic._app_registry.pop(app_name, None)
    shutil.rmtree(tmp, ignore_errors=True)


def _mid_names(bp) -> list[str]:
    return [fm.middleware.func.__name__ for fm in bp._future_middleware]


# ==================== 契约 2 + 3：失败隔离的两个方向 ====================


def test_failure_isolated_both_directions(tree):
    app, pkg, base = tree()
    auto_register_blueprints(app, pkg)
    names = set(app.blueprints)

    # ↑ 向上不扩散：父组、顶层兄弟组照常注册
    assert "good" in names, "子目录失败不得影响父组"
    assert "ok" in names, "子目录失败不得影响顶层兄弟组"

    # ↓ 向下连坐：失败组自己 + 它的子目录都不注册
    assert "badchild" not in names, "失败组自身必须跳过"
    assert "deeper" not in names, (
        "失败组的子目录必须一并跳过——否则它会带着缺失的继承链上线，少一道鉴权且看不出来"
    )
    assert "broken" not in names, "兄弟失败组必须跳过"


def test_surviving_group_keeps_full_middleware_chain(tree):
    """幸存组的中间件链必须完好（不能因为邻居出事就掉链子）。"""
    app, pkg, base = tree()
    auto_register_blueprints(app, pkg)
    assert _mid_names(app.blueprints["good"]) == ["m"]


def test_skipped_group_routes_are_unreachable(tree):
    """被跳过 = 路由压根不存在（404），而不是"存在但没鉴权"。"""
    app, pkg, base = tree()
    auto_register_blueprints(app, pkg)
    app.router.finalize()
    paths = {r.path for r in app.router.routes}
    assert not any(p.startswith(("broken", "badchild", "deeper")) for p in paths), paths


# ==================== 契约 4：可见性（唯一渠道 = 日志）====================


def test_skipped_groups_logged_at_error(tree, caplog):
    """被跳过必须留下 ERROR 日志——运维发现问题的唯一渠道，不能只是静静少几个路由。"""
    app, pkg, base = tree()
    with caplog.at_level("ERROR"):
        auto_register_blueprints(app, pkg)
    text = caplog.text
    assert "跳过整组" in text
    assert "broken" in text and "badchild" in text  # 两个失败组各记一条
    assert "deeper" not in text  # 被连坐的子组不单独记


def test_healthy_tree_registers_everything(tree):
    """全正常时各级组（含深层子组）都注册。"""
    app, pkg, base = tree(good_child_mid=_MID_EMPTY, sibling_mid=_MID_EMPTY)
    auto_register_blueprints(app, pkg)
    assert {"ok", "good", "badchild", "deeper", "broken"} <= set(app.blueprints)


# ==================== 契约 1：两类真实故障都不得被吞 ====================


@pytest.mark.parametrize("bad", [_MID_BROKEN_IMPORT, _MID_BROKEN_SYNTAX])
def test_import_and_syntax_errors_both_skip(tree, bad):
    """ImportError（旧实现会吞）与 SyntaxError 一视同仁，都算配置故障。"""
    app, pkg, base = tree(sibling_mid=bad, good_child_mid=_MID_EMPTY)
    auto_register_blueprints(app, pkg)
    assert "broken" not in app.blueprints
    assert "ok" in app.blueprints and "good" in app.blueprints


def test_unhashable_middleware_entry_does_not_abort_startup(tree):
    """``MIDDLEWARES`` 里混入 dict/list（不可哈希）不得中止整个应用启动。

    去重原用 ``dict.fromkeys``，要求元素可哈希；而该调用在 ``_scan_directory`` 的 try
    之外，``TypeError`` 会一路冒到顶、整个服务起不来——既违反「某个中间件有问题就跳过
    它」，也绕过了「组级失败只跳该子树」的隔离。正确行为：该元素被
    ``_apply_and_register`` 以 warning 跳过，蓝图照常注册。
    """
    app, pkg, base = tree(good_child_mid=_MID_EMPTY, sibling_mid="MIDDLEWARES=[{'a': 1}]\n")
    auto_register_blueprints(app, pkg)  # 不抛
    assert "broken" in app.blueprints  # 组仍注册
    assert _mid_names(app.blueprints["broken"]) == []  # 无效项被跳过
    assert "ok" in app.blueprints and "good" in app.blueprints


def test_duplicate_middleware_still_deduplicated(tree):
    """改了去重实现后，原有的「父子重复声明同一中间件只挂一次」必须保持。"""
    dup = "async def m(r): pass\nMIDDLEWARES=[m, m]\n"
    app, pkg, base = tree(good_child_mid=_MID_EMPTY, sibling_mid=dup)
    auto_register_blueprints(app, pkg)
    assert len(app.blueprints["broken"]._future_middleware) == 1


def test_missing_mid_file_is_not_an_error(tree):
    """文件不存在 = 该目录不追加中间件，合法，必须与"加载失败"严格区分。"""
    app, pkg, base = tree(good_child_mid=_MID_EMPTY, sibling_mid=_MID_EMPTY)
    (base / "broken" / "__mid__.py").unlink()
    auto_register_blueprints(app, pkg)
    assert "broken" in app.blueprints


# ==================== 契约 5：根配置失败不降级 ====================


def test_root_mid_failure_raises(tree):
    """根 ``__mid__`` 决定所有组的基础链，失败即零路由空服务 → 直接崩，不静默。

    ⚠️「根」是相对**本次调用**而言的：``create_app`` 对每个内置路由组各调一次，
    所以同一个 ``webx/controller/api/__mid__.py``，在 ``builtin_controller=True``
    时是子目录（失败→跳过该组），在 ``builtin_controller=["api"]`` 时是根（失败→抛）。
    """
    app, pkg, base = tree(root_mid=_MID_BROKEN_IMPORT)
    with pytest.raises(MiddlewareConfigError):
        auto_register_blueprints(app, pkg)


# ============ 契约 6：正常路径行为不变（真实内置 controller 回归）============


def test_real_assembly_has_no_skipped_group(app_with_db):
    """真实装配回归：内置 + 业务的每一组都在，中间件链与 ``__mid__.py`` 声明一致。"""
    admin_chain = ["admin_auth_middleware", "role_perm_middleware"]
    expected = {
        # 内置：webx/controller/（自身没有根 __mid__.py，故链条就是各组自己声明的那段）
        "auth": [],  # 公开组，鉴权走路由级 @require_token
        "user": ["auth_middleware"],
        "file": ["auth_middleware"],
        "account": admin_chain,
        "auth_permission": admin_chain,
        "permission": admin_chain,
        "role": admin_chain,
        "role_permission": admin_chain,
        "user_role": admin_chain,
        "public_file": [],  # 签名下载：授权靠限时签名本身，不需要登录
        # 业务示例：app/controller/（根 __mid__.py 提供 request_id / client_ip / lang）
        "public": ["request_id", "client_ip", "lang", "access_note"],
        "note": ["request_id", "client_ip", "lang", "access_note", "auth_middleware"],
    }
    assert set(app_with_db.blueprints) == set(expected), (
        "蓝图组与预期不一致——多出来是漏更新本用例，少了极可能是某组被静默跳过"
    )
    for name, want in expected.items():
        assert _mid_names(app_with_db.blueprints[name]) == want, f"{name} 的中间件链变了"


def test_no_api_blueprint_lacks_auth(app_with_db):
    """兜底不变量：``/api/*`` 与 ``/admin/*`` 下每个蓝图都必须挂着某种认证中间件。"""
    for name, bp in app_with_db.blueprints.items():
        prefix = bp.url_prefix or ""
        if not prefix.startswith(("/api", "/admin")):
            continue
        mids = _mid_names(bp)
        assert any(
            m in ("auth_middleware", "admin_auth_middleware") for m in mids
        ), f"{name} ({prefix}) 没有任何认证中间件：{mids}"
