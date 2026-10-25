"""分层约束：包级依赖必须是自上而下的有向无环图。

这条约束是**结构性**的，靠人工 review 守不住——一次随手的 import 就能让
`lib` 反向依赖 `core`，环就回来了。故用测试焊死：新增跨层 import 时，
要么它符合分层顺序，要么这个测试立刻失败。

分层顺序（自底向上，下层绝不可 import 上层）::

    paths       项目根定位，零依赖
    config      BaseSettings，只依赖 paths + environs
    i18n        文案注册与取用，零依赖
    lib         纯工具：令牌 / 哈希 / ID / 表结构同步 / 邮件 / 短信，零 ORM
    models      Tortoise 模型 + DTO
    services    业务逻辑，查库
    middleware  可插拔中间件（鉴权那几条要查库，故必须在 models/services 之上）
    core        应用骨架：应用工厂、蓝图扫描、ORM 初始化、异常、日志、路由级守卫
    controller  路由
    cli         命令行工具

历史教训一：`config` / `paths` 原先在 `core/` 里，导致 `lib` 与 `services`
反向依赖 `core`，而 `core` 又依赖 `lib.token` —— 构成 `core ⇄ lib` 的包级循环。
把这两个叶子模块提到顶层即解环。同理 `init_permission` 因要写库而从 `lib` 移入 `services`。

历史教训二（中间件拆包）：鉴权中间件从 `core/middleware.py` 拆进 `middleware/` 后，
它必须排在 `models` / `services` **之上**（要查库），而 `core.app` 要装配 CORS、
`core.guards` 要复用 `DEFAULT_CLAIMS`，于是 `core` 只能排在 `middleware` **之上**。
两条约束联立，`core.i18n` 就不能再留在 core 里（中间件全都要用它）——按 `config`
的先例提到顶层做叶子。`get_client_ip` 同理从 `core.guards` 下沉进 `lib`：
中间件与路由装饰器都要用，只能沉到两者共同的下游。
"""

import ast
import collections
import pathlib

import pytest

# 自底向上，索引即层级；下层不得 import 上层
LAYER_ORDER = [
    "paths",
    "config",
    "i18n",
    "lib",
    "models",
    "services",
    "middleware",
    "core",
    "controller",
    "cli",
]
RANK = {name: i for i, name in enumerate(LAYER_ORDER)}

WEBX_ROOT = pathlib.Path(__file__).resolve().parent.parent / "webx"


def _layer_of(module: str) -> str | None:
    """``webx.lib.token`` → ``lib``；非分层模块（如 ``webx`` 本身）返回 None。"""
    parts = module.split(".")
    if len(parts) < 2 or parts[0] != "webx":
        return None
    return parts[1] if parts[1] in RANK else None


def _collect_edges() -> dict[str, set[tuple[str, str]]]:
    """扫描全部源码，返回 ``{源层: {(目标层, 出处文件)}}``。"""
    edges: dict[str, set[tuple[str, str]]] = collections.defaultdict(set)

    for path in WEBX_ROOT.rglob("*.py"):
        module = ".".join(path.relative_to(WEBX_ROOT.parent).with_suffix("").parts)
        src = _layer_of(module)
        if src is None:
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                targets = [node.module]
            elif isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            else:
                continue

            for target in targets:
                dst = _layer_of(target)
                if dst is not None and dst != src:
                    edges[src].add((dst, str(path.relative_to(WEBX_ROOT.parent))))

    return edges


def test_no_package_level_cycles():
    """任意两层之间不得互相依赖。"""
    edges = _collect_edges()
    plain = {src: {dst for dst, _ in pairs} for src, pairs in edges.items()}

    cycles = {
        tuple(sorted((src, dst)))
        for src, dsts in plain.items()
        for dst in dsts
        if src in plain.get(dst, set())
    }
    assert not cycles, f"存在包级循环依赖: {sorted(cycles)}"


def test_dependencies_point_downward():
    """每条跨层依赖都必须指向更低的层。"""
    violations = [
        f"{src} → {dst}  ({where})"
        for src, pairs in _collect_edges().items()
        for dst, where in pairs
        if RANK[dst] > RANK[src]
    ]
    assert not violations, "下层依赖了上层:\n  " + "\n  ".join(sorted(violations))


def test_lib_stays_free_of_orm():
    """``lib`` 是零 ORM 依赖的工具层——它一旦 import 模型，环就回来了。"""
    offenders = [
        str(path.relative_to(WEBX_ROOT.parent))
        for path in (WEBX_ROOT / "lib").rglob("*.py")
        if "webx.models" in path.read_text(encoding="utf-8")
    ]
    assert not offenders, f"lib 不得依赖 models: {offenders}"


def test_leaf_modules_stay_leaves():
    """顶层叶子模块只能依赖更底层的叶子，不能反向依赖任何业务层。

    ``i18n`` 在这里的意义尤其大：全框架（含 middleware / core / controller）都要用它，
    它一旦回头 import 任何上层，环立刻回来。
    """
    for name, allowed in (("paths", set()), ("config", {"paths"}), ("i18n", set())):
        deps = {dst for dst, _ in _collect_edges().get(name, set())}
        assert deps <= allowed, f"webx/{name}.py 依赖了 {deps - allowed}，它必须保持叶子"


def test_core_no_longer_owns_middleware():
    """中间件已拆成独立包——``core/middleware.py`` 不得复活。

    这是本次结构改造要守住的东西：加一条中间件永远只该动 ``webx/middleware/``
    或业务自己的 ``app/middlewares/``，绝不该回到「改 core 文件」。
    """
    assert not (WEBX_ROOT / "core" / "middleware.py").exists(), (
        "core/middleware.py 又出现了：中间件应放在 webx/middleware/，一个中间件一个模块"
    )


# ==================== 卫星包：与核心的依赖方向 ====================
#
# webx 核心之外的独立部署单元（存储服务、worker）。方向是单向的：
#   webx_storage → webx          （存储服务复用核心的存储原语 / 配置 / 应用工厂）
#   webx 核心    ↛ 任何卫星包
#   webx_worker  ↛ webx*         （worker 要能单独打包成客户端，不得带上服务端框架）

REPO_ROOT = WEBX_ROOT.parent


def _imports_of(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.add(node.module)
        elif isinstance(node, ast.Import):
            out.update(alias.name for alias in node.names)
    return out


def _top(module: str) -> str:
    return module.split(".")[0]


def test_core_does_not_depend_on_satellite_packages():
    satellites = {"webx_storage", "webx_worker"}
    offenders = [
        f"{p.relative_to(REPO_ROOT)} → {m}"
        for p in WEBX_ROOT.rglob("*.py")
        for m in _imports_of(p)
        if _top(m) in satellites
    ]
    assert not offenders, "webx 核心不得依赖卫星包:\n  " + "\n  ".join(offenders)


def test_worker_is_fully_independent_of_the_server_framework():
    """webx_worker 只许用标准库与 httpx：它要能单独打包给没有服务端框架的机器。"""
    worker = REPO_ROOT / "webx_worker"
    if not worker.exists():
        pytest.skip("webx_worker 尚未引入")
    offenders = [
        f"{p.relative_to(REPO_ROOT)} → {m}"
        for p in worker.rglob("*.py")
        for m in _imports_of(p)
        if _top(m) in {"webx", "webx_storage", "sanic", "tortoise", "sanic_ext", "environs"}
    ]
    assert not offenders, "webx_worker 必须完全独立:\n  " + "\n  ".join(offenders)

