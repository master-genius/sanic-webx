"""项目根定位。

webx 作为**独立安装的包**存在（可能躺在 site-packages 里），因此框架内任何
"上溯 N 级找项目根" 的写法都是错的——那只会溯到 webx 自己的安装目录。

本模块是全框架唯一的项目根真相来源，三级回退：

  1. ``configure_root(path)`` —— 业务项目在入口显式声明（最高优先级、最可靠）
  2. 环境变量 ``WEBX_ROOT``   —— 部署期声明（systemd / 容器友好）
  3. ``Path.cwd()``           —— 兜底

项目根用于：``.env`` 定位、相对日志目录、相对存储目录等一切"相对项目"的路径。
"""

from __future__ import annotations

import os
from pathlib import Path

_configured_root: Path | None = None


def configure_root(path: str | Path) -> Path:
    """显式声明项目根。应在 import 任何 webx 配置模块**之前**调用。

    典型用法（业务项目的 run.py 首行）::

        from pathlib import Path
        from webx.paths import configure_root
        configure_root(Path(__file__).parent)
    """
    global _configured_root
    _configured_root = Path(path).expanduser().resolve()
    # 同步写回环境变量：spawn 出的子进程（Sanic 多 worker）能继承到同一个根
    os.environ["WEBX_ROOT"] = str(_configured_root)
    return _configured_root


def project_root() -> Path:
    """返回当前项目根（按 configure_root → WEBX_ROOT → CWD 回退）。"""
    if _configured_root is not None:
        return _configured_root
    env_root = os.environ.get("WEBX_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path.cwd().resolve()


def resolve_path(value: str | Path) -> Path:
    """把可能是相对路径的配置值锚定到项目根。

    绝对路径与 ``~`` 展开保持原样；相对路径一律相对【项目根】而非进程 CWD——
    服务可能由 systemd（WorkingDirectory 任意）、启动脚本、裸 python 等多种方式
    拉起，CWD 不稳定，锚定项目根后语义在所有启动方式下一致。
    """
    p = Path(value).expanduser()
    if not p.is_absolute():
        p = project_root() / p
    return p.resolve()
