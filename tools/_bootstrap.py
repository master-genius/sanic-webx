"""CLI 共享引导：定位项目根、读取配置、建立/关闭数据库连接。

本仓库**不做成安装包**——拉下来就是项目，脚本直接 ``python tools/xxx.py`` 跑。
所以每个脚本先把项目根塞进 ``sys.path``，再 import ``webx`` 与 ``app``。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import sys
from pathlib import Path
from typing import Awaitable, Callable

# 项目根 = tools/ 的上一级。放在 import webx 之前，脚本从任何目录执行都成立。
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("WEBX_ROOT", str(PROJECT_ROOT))


def load_settings(modules: str | None = None) -> None:
    """加载业务配置，并按约定合并各业务模块的模型模块。

    用 ``import_module`` 而非 ``import app.config``：这里要的**只是 import 副作用**
    （模块内的 ``configure_settings(Settings())``），不需要拿到模块对象，
    写成前者能让这个意图对读者和静态检查器都一目了然。

    Args:
        modules: 逗号分隔的业务模块包名。``None`` 时取环境变量 ``WEBX_MODULES``。

    ⚠️ 模块解析必须与服务进程**同源**：服务用 ``WEBX_MODULES``（见 run.py / server.sh），
    CLI 若只读 ``app/config.py`` 的硬编码，就会出现「``WEBX_MODULES=app,appx`` 起的
    服务路由齐全，但 ``dbsync`` 只建了 app 的表」——接口一查库就 no such table，
    且建表那一步毫无征兆。故这里读同一个来源，并调用同一个 ``apply_module_models``。

    ⚠️ 用 ``env.str`` 而非 ``os.environ``：environs>=15 的 ``read_env()`` 不回写
    ``os.environ``，用后者会让 ``.env`` 里的 ``WEBX_MODULES`` 静默失效。
    ``env.str`` 两个来源都认，且 shell 环境变量优先于 ``.env``。
    """
    import importlib

    importlib.import_module("app.config")

    from webx.config import apply_module_models, env

    spec = modules if modules is not None else env.str("WEBX_MODULES", "")
    names = [m.strip() for m in spec.split(",") if m.strip()]

    # webx 自己也按同一套约定算：只要没显式关掉内置路由，就带上 webx.models
    # （auth/api/admin 组都要查 User/Permission 那几张表）。判定必须与服务端一致，
    # 否则又回到「服务跑得起来但表没建」。
    if env.str("WEBX_BUILTIN", "").strip().lower() not in ("false", "0", "no", "none", "off"):
        names.append("webx")

    if names:
        apply_module_models(names)


@contextlib.asynccontextmanager
async def database():
    """连库的 async 上下文，退出时保证断开。

    校验模型列表非空——空列表下所有查询都会以晦涩的 "no such table" 失败，
    不如在这里直接说清楚。
    """
    from tortoise import Tortoise

    from webx.config import settings

    config = settings.tortoise_config()
    if not config["apps"]["models"]["models"]:
        raise SystemExit(
            "❌ 没有解析出任何模型模块。检查 WEBX_MODULES 指定的模块里是否有 "
            "<pkg>/models（改名成 _models.py 会被跳过），或用 --modules 显式指定。"
        )

    await Tortoise.init(config=config)
    try:
        yield
    finally:
        await Tortoise.close_connections()


def run(args: argparse.Namespace, handler: Callable[[argparse.Namespace], Awaitable[int | None]]) -> None:
    """CLI 主入口的统一收尾：加载配置 → 连库 → 执行 → 按返回值退出。

    handler 返回 None 视为成功（退出码 0）。
    脚本若定义了 ``--modules``，其值在这里被用于合并模型模块（未定义则取环境变量）。
    """
    load_settings(getattr(args, "modules", None))

    async def _main() -> int:
        async with database():
            return await handler(args) or 0

    sys.exit(asyncio.run(_main()))


def fail(message: str) -> None:
    """打印错误并以非零码退出。"""
    print(f"❌ {message}", file=sys.stderr)
    raise SystemExit(1)
