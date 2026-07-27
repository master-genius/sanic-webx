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


def _deduplicate(mids: list[Any]) -> list[Any]:
    """保持原始顺序的去重。"""
    return list(dict.fromkeys(mids))


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
    """加载目录下 ``__mid__.py`` 的 MIDDLEWARES；文件不存在返回空列表。"""
    if not (dir_path / "__mid__.py").exists():
        return []

    module_name = f"{module_prefix}.__mid__"
    try:
        module = importlib.import_module(module_name)
        mids = list(getattr(module, "MIDDLEWARES", []))
        logger.debug("加载目录中间件: %s -> %d 个", module_name, len(mids))
        return mids
    except ImportError:
        return []
    except Exception as e:
        logger.error("加载中间件配置 %s 失败: %s", module_name, e)
        raise


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
    """
    controller_dir = controller_dir or _package_dir(controller_package)

    if not controller_dir.exists():
        logger.warning("控制器目录不存在: %s", controller_dir)
        return 0

    logger.info("🚀 开始扫描控制器: %s (%s)", controller_package, controller_dir)

    base_mids = _load_mid_file(controller_dir, controller_package)
    count = _scan_directory(app, controller_dir, controller_package, base_mids)

    logger.info("✅ 蓝图自动注册完成，共注册 %d 个蓝图", count)
    return count


def _scan_directory(
    app: Sanic,
    dir_path: Path,
    module_prefix: str,
    inherited_mids: list[Any],
) -> int:
    """递归扫描目录，加载蓝图并应用中间件链。"""
    dir_mids = _load_mid_file(dir_path, module_prefix)
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
