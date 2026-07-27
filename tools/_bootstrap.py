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


def load_settings() -> None:
    """加载业务配置。

    用 ``import_module`` 而非 ``import app.config``：这里要的**只是 import 副作用**
    （模块内的 ``configure_settings(Settings())``），不需要拿到模块对象，
    写成前者能让这个意图对读者和静态检查器都一目了然。
    """
    import importlib

    importlib.import_module("app.config")


@contextlib.asynccontextmanager
async def database():
    """连库的 async 上下文，退出时保证断开。

    校验 ``MODELS_MODULES`` 非空——空模型列表下所有查询都会以晦涩的
    "no such table" 失败，不如在这里直接说清楚。
    """
    from tortoise import Tortoise

    from webx.config import settings

    config = settings.tortoise_config()
    if not config["apps"]["models"]["models"]:
        raise SystemExit("❌ MODELS_MODULES 为空：请在 app/config.py 中设置。")

    await Tortoise.init(config=config)
    try:
        yield
    finally:
        await Tortoise.close_connections()


def run(args: argparse.Namespace, handler: Callable[[argparse.Namespace], Awaitable[int | None]]) -> None:
    """CLI 主入口的统一收尾：加载配置 → 连库 → 执行 → 按返回值退出。

    handler 返回 None 视为成功（退出码 0）。
    """
    load_settings()

    async def _main() -> int:
        async with database():
            return await handler(args) or 0

    sys.exit(asyncio.run(_main()))


def fail(message: str) -> None:
    """打印错误并以非零码退出。"""
    print(f"❌ {message}", file=sys.stderr)
    raise SystemExit(1)
