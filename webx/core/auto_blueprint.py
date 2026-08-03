"""蓝图自动注册器。

核心功能：
  1. 扫描 controller 包，自动发现并注册 Sanic 蓝图。
  2. 按目录层级应用中间件，支持继承与模块级追加。

约定：
  - ``__mid__.py``：目录级中间件配置，导出列表变量 ``MIDDLEWARES``；
    子目录自动继承父目录的中间件链，可再追加。
  - ``__mid__``：模块级变量（写在具体 ``.py`` 里），仅对该模块的蓝图追加。
  - ``__blueprints__``：（可选）模块显式导出的蓝图列表；未定义则自动扫描模块属性。

与原实现的唯一差别、也是通用化的关键：**controller 包路径由调用方传入**，
不再写死 ``xymol.controllers``（原项目写死的那个）、也不再用 ``Path(__file__).parent.parent`` 推目录。
webx 是独立安装的包，那种写法只会扫到框架自己的安装目录。目录由
``importlib.util.find_spec`` 从包名反查，包与目录永远一致。
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
from pathlib import Path
from typing import Any, Optional

from sanic import Blueprint, Sanic

logger = logging.getLogger(__name__)


class MiddlewareConfigError(Exception):
    """``__mid__.py`` 存在但加载失败（导入错误 / 语法错误 / 求值抛异常）。

    与「文件不存在」严格区分：不存在 = 该目录不追加中间件（合法配置，返回空列表）；
    加载失败 = **配置意图未知**，绝不可当成空列表放行——那会让整组路由裸奔。
    """


def _deduplicate(mids: list[Any]) -> list[Any]:
    """保持原始顺序的去重。

    ⚠️ 不用 ``dict.fromkeys``：它要求元素可哈希，而 ``MIDDLEWARES`` 里混入 dict/list
    （配置写错）会抛 ``TypeError: unhashable type``。该调用位于 ``_scan_directory``
    的 try 之外，异常会**直接中止整个应用启动**——既违反「配置有问题就跳过那一组」
    的契约，也绕过了组级失败隔离。中间件恒为函数对象，按 identity 去重与原语义
    等价（函数的 ``__eq__`` 即 ``is``），且对任何元素都不会抛。
    非可调用元素继续交给 ``_apply_and_register`` 逐个 warning 跳过。
    """
    out: list[Any] = []
    for m in mids:
        if not any(m is x for x in out):
            out.append(m)
    return out


def _package_dir(package: str) -> Path:
    """由包名反查其文件系统目录。

    比 ``__file__`` 上溯可靠：无论包被 ``pip install -e`` 还是普通安装、
    无论进程 CWD 在哪，find_spec 给出的都是该包真实所在目录。
    """
    spec = importlib.util.find_spec(package)
    if spec is None or not spec.submodule_search_locations:
        raise ImportError(f"controller 包不存在或不是包：{package}")
    return Path(list(spec.submodule_search_locations)[0])


def _load_mid_file(dir_path: Path, module_prefix: str) -> list[Any]:
    """加载目录下 ``__mid__.py`` 的 MIDDLEWARES；文件不存在返回空列表。

    Raises:
        MiddlewareConfigError: 文件存在但加载失败。
            ⚠️ 此处**不可**吞掉任何异常返回 ``[]``：文件不存在的情况上面已经拦掉了，
            剩下能走到 except 的全是真实故障（函数名写错、依赖缺失、语法错误）。
            ``except ImportError: return []`` 那种写法会让「``__mid__.py`` 里一个笔误」
            静默变成「该组全部路由无鉴权且应用照常启动」——鉴权配置的失败方式
            只能是「不放行」，不能是「当作没配」。
    """
    if not (dir_path / "__mid__.py").exists():
        return []

    module_name = f"{module_prefix}.__mid__"
    try:
        module = importlib.import_module(module_name)
        mids = list(getattr(module, "MIDDLEWARES", []))
        logger.debug("加载目录中间件: %s -> %d 个", module_name, len(mids))
        return mids
    except Exception as e:
        logger.error("加载中间件配置 %s 失败: %s", module_name, e)
        raise MiddlewareConfigError(f"{module_name}: {e}") from e


def auto_register_blueprints(
    app: Sanic,
    controller_package: str,
    controller_dir: Optional[Path] = None,
) -> int:
    """扫描 controller 包，注册蓝图并应用层级中间件。

    Args:
        app: Sanic 应用实例。
        controller_package: 控制器根包名，如 ``"app.controller"``。
        controller_dir: 目录路径；默认由包名反查（正常情况无需传）。

    Returns:
        成功注册的蓝图数量。

    Raises:
        MiddlewareConfigError: **根** ``__mid__.py``（即 ``controller_package`` 自己那份）
            加载失败。根配置决定所有组的基础中间件链，它失败意味着每一组都要被跳过 →
            起来一个零路由的空服务，还会骗过健康检查（进程活着、端口通）。这种状态
            没有任何「局部可用」价值，故不降级、直接崩。子目录失败则**不**走这里
            （见 ``_scan_directory``：只跳那一棵子树，父组与兄弟组照常注册）。

            ⚠️ 「根」是相对本次调用而言的：``create_app`` 对每个内置路由组各调一次，
            所以 ``webx/controller/api/__mid__.py`` 在 ``builtin_controller=True`` 时是
            子目录（失败→跳过该组），在 ``builtin_controller=["api"]`` 时是根（失败→抛）。
    """
    controller_dir = controller_dir or _package_dir(controller_package)

    if not controller_dir.exists():
        logger.warning("控制器目录不存在: %s", controller_dir)
        return 0

    logger.info("🚀 开始扫描控制器: %s (%s)", controller_package, controller_dir)

    # 1. 根目录 __mid__.py（失败不降级，理由见 docstring）
    base_mids = _load_mid_file(controller_dir, controller_package)
    # 2. 递归扫描（组级失败在 _scan_directory 内就地记 ERROR 并跳过整棵子树）
    count = _scan_directory(app, controller_dir, controller_package, base_mids)

    logger.info("✅ 蓝图自动注册完成，共注册 %d 个蓝图", count)
    return count


def _scan_directory(
    app: Sanic,
    dir_path: Path,
    module_prefix: str,
    inherited_mids: list[Any],
) -> int:
    """递归扫描目录，加载蓝图并应用中间件链。

    失败隔离的两个方向由本函数顶部这个 try/except 一并给出，依赖其执行顺序
    （① 载本目录 ``__mid__`` ② 注册本目录 ``.py`` ③ 递归子目录）：

      - **子目录失败** → 异常发生在子目录自己那次调用的 ① 里，就地 return 0；
        父目录的 ② 早已完成、③ 的循环继续走下一个兄弟 → 父组与兄弟组毫发无损。
      - **本目录失败** → ②③ 都不执行 → 子目录压根没机会注册。这条是关键：
        子目录继承父目录的中间件，若父组失败却仍递归进去，子组会带着**缺失的
        继承链**上线——看似正常、实则少一道鉴权，比整组消失危险得多。

    选「整组跳过」而非「整个服务崩」：路由压根不注册 → 404 不可达，同样杜绝
    鉴权旁路，但爆炸半径只限于该组。
    """
    try:
        dir_mids = _load_mid_file(dir_path, module_prefix)
    except MiddlewareConfigError as e:
        logger.error(
            "⛔ 中间件配置加载失败，跳过整组: %s —— 该目录及其所有子目录一律不注册路由"
            "（对外表现为 404 不可达）。原因: %s",
            module_prefix,
            e,
        )
        return 0

    # 中间件链 = 父级继承 + 本目录（去重保序）
    effective_mids = _deduplicate(inherited_mids + dir_mids)

    count = 0

    # 排序保证加载顺序确定性（跨机器/文件系统一致）
    py_files = sorted(f for f in dir_path.iterdir() if f.is_file() and f.suffix == ".py")
    for py_file in py_files:
        # 跳过私有文件（含 __init__.py / __mid__.py）
        if py_file.name.startswith("_"):
            continue

        file_module_name = f"{module_prefix}.{py_file.stem}"
        try:
            module = importlib.import_module(file_module_name)
            blueprints = _find_blueprints(module)
            if not blueprints:
                continue

            # 模块级追加：只影响本模块的蓝图
            module_mids = list(getattr(module, "__mid__", []))
            final_mids = _deduplicate(effective_mids + module_mids)

            for bp in blueprints:
                _apply_and_register(app, bp, final_mids, file_module_name)
                count += 1
        except Exception as e:
            logger.error("❌ 加载控制器模块 %s 失败: %s", file_module_name, e)
            # 启动期直接失败，好过带病运行
            raise

    for sub_dir in sorted(d for d in dir_path.iterdir() if d.is_dir()):
        if sub_dir.name.startswith("_") or sub_dir.name == "__pycache__":
            continue
        count += _scan_directory(
            app, sub_dir, f"{module_prefix}.{sub_dir.name}", effective_mids
        )

    return count


def _find_blueprints(module) -> list[Blueprint]:
    """从模块中找出蓝图。

    ``__blueprints__`` 支持三种形态：

      - 蓝图对象或其列表 —— 最常见的写法
      - **可调用对象**（工厂）—— 每次注册时调用一次，返回蓝图或蓝图列表

    工厂形态是"同一 controller 包注册进多个 app"的唯一正确写法：模块只 import
    一次，模块级的蓝图对象随之固化，第二个 app 复用它会撞 RouteExists。写成
    ``__blueprints__ = lambda: [make_xxx_blueprint()]`` 则每个 app 拿到全新实例。
    """
    explicit = getattr(module, "__blueprints__", None)
    if explicit is not None:
        if callable(explicit) and not isinstance(explicit, Blueprint):
            explicit = explicit()
        return list(explicit) if isinstance(explicit, (list, tuple)) else [explicit]

    found = []
    for attr_name in dir(module):
        if attr_name.startswith("_"):
            continue
        attr = getattr(module, attr_name)
        if isinstance(attr, Blueprint):
            found.append(attr)
    return found


def _apply_and_register(app: Sanic, bp: Blueprint, mids: list[Any], source: str) -> None:
    """把中间件挂到蓝图上并注册进 app。"""
    for mid in mids:
        if callable(mid):
            bp.middleware(mid)
        else:
            logger.warning("跳过无效的中间件对象: %r (来源: %s)", mid, source)

    app.blueprint(bp)
    logger.debug("📦 注册蓝图: %s (%s) [来自 %s]", bp.name, bp.url_prefix or "/", source)
