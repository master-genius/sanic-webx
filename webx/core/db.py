"""数据库初始化。

配置来源是 ``settings.tortoise_config()``；模型模块列表由装配的模块按约定扫出来
（``<pkg>/models``），框架不假设业务模型在哪，也没有对应的配置项。
"""

from __future__ import annotations

import logging
import os

from tortoise import Tortoise

from webx.config import settings

logger = logging.getLogger(__name__)


async def init_db(app=None) -> None:
    """初始化数据库连接（在 ``before_server_start`` 钩子中调用）。

    多进程模式下每个 Sanic worker 子进程各自执行一次，各持独立连接池。

    ``_enable_global_fallback=True``：``before_server_start`` 运行在后台任务中、
    请求处理在另一批任务中，开启全局 fallback 才能跨任务访问同一 Tortoise 实例。
    """
    config = settings.tortoise_config()
    if not config["apps"]["models"]["models"]:
        logger.warning(
            "未解析出任何模型模块，ORM 将没有任何表可用。"
            "检查装配的模块里是否有 <pkg>/models（改名成 _models 就会被跳过）"
        )
    await Tortoise.init(config=config, _enable_global_fallback=True)
    logger.info("Tortoise ORM 初始化完成 (PID=%s)", os.getpid())


async def close_db(app=None) -> None:
    """关闭数据库连接（在 ``after_server_stop`` 钩子中调用）。"""
    if Tortoise._inited:
        await Tortoise.close_connections()
        logger.info("Tortoise ORM 连接已关闭")
