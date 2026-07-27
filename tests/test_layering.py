"""分层约束：包级依赖必须是自上而下的有向无环图。

这条约束是**结构性**的，靠人工 review 守不住——一次随手的 import 就能让
`lib` 反向依赖 `core`，环就回来了。故用测试焊死：新增跨层 import 时，
要么它符合分层顺序，要么这个测试立刻失败。

分层顺序（自底向上，下层绝不可 import 上层）::

    paths       项目根定位，零依赖
    config      BaseSettings，只依赖 paths + environs
    lib         纯工具：令牌 / 哈希 / ID / 表结构同步 / 邮件 / 短信，零 ORM
    models      Tortoise 模型 + DTO
    services    业务逻辑，查库
    core        应用骨架：蓝图扫描、中间件、应用工厂、ORM 初始化、异常、i18n、日志
    middleware  可插拔中间件
    controller  路由
    cli         命令行工具

历史教训：`config` / `paths` 原先在 `core/` 里，导致 `lib` 与 `services`
反向依赖 `core`，而 `core` 又依赖 `lib.token` —— 构成 `core ⇄ lib` 的包级循环。
把这两个叶子模块提到顶层即解环。同理 `init_permission` 因要写库而从 `lib` 移入 `services`。
"""

import ast
import collections
import pathlib

# 自底向上，索引即层级；下层不得 import 上层
LAYER_ORDER = [
    "paths",
    "config",
    "lib",
    "models",
    "services",
    "core",
    "middleware",
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


def test_config_and_paths_are_leaves():
    """配置与路径必须保持叶子地位：只能依赖彼此，不能反向依赖任何业务层。"""
    for name, allowed in (("paths", set()), ("config", {"paths"})):
        deps = {dst for dst, _ in _collect_edges().get(name, set())}
        assert deps <= allowed, f"webx/{name}.py 依赖了 {deps - allowed}，它必须保持叶子"
