"""领取服务应用装配。

复用 ``webx.core.app.create_app`` 的日志、异常处理与数据库初始化，关掉内置路由与默认健康端点
（本服务有自己的 ``/health``），只挂领取相关的两个蓝图。

数据库不可达**不中止启动**，只响亮告警：分发服务扛住 DB 短暂抖动是有价值的——挂起的领取请求会拿到 503，
worker 按退避重连，DB 恢复后自愈；崩溃重启只会让所有 worker 同时重连造成惊群。
必须告警，因为配置写错（连错库 / 端口）与"DB 临时抖动"现象一样，不告警的话服务会一直"健康地"什么也不做。
"""

from __future__ import annotations

import logging

from sanic import Sanic
from tortoise import Tortoise

from webx.config import apply_module_models, settings
from webx.core.app import create_app
from webx.services.lease import ensure_claim_index
from task_server.routes import health_bp, lease_bp

logger = logging.getLogger(__name__)


async def _probe_db() -> str:
    try:
        await Tortoise.get_connection("default").execute_query("SELECT 1")
        return "ok"
    except Exception as e:  # noqa: BLE001
        return f"unreachable: {e}"


def create_task_server_app(name: str | None = None) -> Sanic:
    # 模型来源按约定算：领取服务只用 webx.models（Task 表）。必须在 create_app 之前登记——
    # builtin_controller=False 时 create_app 不会替我们登记
    apply_module_models(["webx"])

    async def startup(app: Sanic) -> None:
        db_state = await _probe_db()
        index_state = await ensure_claim_index() if db_state == "ok" else "跳过（DB 不可达）"
        logger.info(
            "task_server 启动：listen=%s:%s hold=%ss | 认领短租约=%ss 常规租约=%ss+grace(α=%s, %s~%ss) 重投上限=%s",
            settings.TASK_SERVER_HOST, settings.TASK_SERVER_PORT, settings.TASK_LEASE_HOLD_SECONDS,
            settings.LEASE_CLAIM_SECONDS, settings.LEASE_SECONDS, settings.LEASE_GRACE_ALPHA,
            settings.LEASE_GRACE_MIN_SECONDS, settings.LEASE_GRACE_MAX_SECONDS, settings.LEASE_REQUEUE_LIMIT,
        )
        if db_state == "ok":
            level = logger.error if index_state.startswith("失败") else logger.info
            level("认领热点索引 %s：%s", "idx_tasks_claim", index_state)
        else:
            logger.error(
                "⚠️ 数据库不可达（服务照常启动，/task/leases 将返回 503 让 worker 退避）：%s:%s/%s → %s",
                settings.DATABASE_HOST, settings.DATABASE_PORT, settings.DATABASE_NAME, db_state,
            )

    app = create_app(
        name=name or settings.APP_NAME,
        use_db=True,
        builtin_controller=False,
        health_endpoints=False,
        on_startup=[startup],
    )
    app.blueprint(lease_bp)
    app.blueprint(health_bp)
    return app
