"""存储服务应用工厂。

复用 ``webx.core.app.create_app`` 的日志、异常处理、健康检查与启动钩子，
只关掉内置路由与数据库——存储节点不需要用户体系，也没有 DB。
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

from sanic import Blueprint, Sanic

from webx.config import settings
from webx.core.app import create_app
from webx.services.storage_service import StorageService
from webx_storage.policy import KeyPolicy, StorageContext, build_policies
from webx_storage.routes import mount_core

logger = logging.getLogger(__name__)


def create_storage_app(
    policies: Optional[Iterable[KeyPolicy]] = None, *, name: Optional[str] = None
) -> Sanic:
    """装配一个存储服务应用。

    Args:
        policies: 键策略实例；None 则按 ``STORAGE_POLICIES`` 配置实例化。
        name: Sanic 应用名，默认取 ``settings.APP_NAME``。
    """
    svc = StorageService()
    pols = list(policies) if policies is not None else build_policies(settings.STORAGE_POLICIES)

    if not settings.STORAGE_INTERNAL_TOKEN:
        logger.warning(
            "⚠️ STORAGE_INTERNAL_TOKEN（及回退的 INTERNAL_TOKEN）均未配置：所有 /internal 请求将被拒绝"
        )

    async def prepare(app: Sanic) -> None:
        # 启动时建目录并回收超龄分片会话（多 worker 各跑一次是安全的）
        try:
            svc.ensure_dirs()
            svc.cleanup_stale_uploads()
        except OSError as e:
            logger.error("存储初始化失败: %s", e)

    app = create_app(
        name=name,
        use_db=False,
        builtin_controller=False,
        on_startup=[prepare],
    )

    ctx = StorageContext(
        svc=svc,
        internal=Blueprint("storage_internal", url_prefix="/internal"),
        public=Blueprint("storage_public", url_prefix="/public"),
        policies=pols,
    )
    mount_core(ctx)
    for policy in pols:
        policy.mount(ctx)
        logger.info("存储键策略已启用: %s", policy.name)

    app.blueprint(ctx.internal)
    app.blueprint(ctx.public)
    return app
