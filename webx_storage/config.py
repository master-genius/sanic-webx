"""存储服务配置：在 webx 的 ``BaseSettings`` 上只追加存储节点自己的字段。

读取项目根的 ``.env``（项目根按 ``WEBX_ROOT`` > 当前目录定位，见 ``webx.paths``），
关键项用 ``STORAGE_*`` 专属键，并回退到通用键，这样「与主服务同机部署」时零额外配置，
「独立机部署」时用 ``STORAGE_*`` 覆盖。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from webx.config import BaseSettings, configure_settings, env


@dataclass
class StorageSettings(BaseSettings):
    APP_NAME: str = field(default_factory=lambda: env.str("APP_NAME", "webx-storage"))

    # ==================== 监听 ====================
    SERVER_HOST: str = field(default_factory=lambda: env.str("STORAGE_HOST", "0.0.0.0"))
    SERVER_PORT: int = field(default_factory=lambda: env.int("STORAGE_PORT", 9010))
    STORAGE_WORKERS: int = field(default_factory=lambda: env.int("STORAGE_WORKERS", 1))

    # ==================== 内部鉴权 ====================
    # 服务间调用的 X-Internal-Token 用 BaseSettings.STORAGE_INTERNAL_TOKEN（回退通用 INTERNAL_TOKEN）。
    # ⚠️ 两者都为空时**拒绝一切内部请求**（空配置绝不能等价于放行），并在启动时告警。

    # ==================== 键策略 ====================
    # 启用哪些键策略（逗号分隔）。通用的 /internal/objects 恒有；策略在其上追加
    # 业务形状的路由。默认 task_results：tasks/<task_id>/results/<file> 的任务产物布局。
    # 置空 = 只提供通用对象接口。
    STORAGE_POLICIES: list[str] = field(
        default_factory=lambda: env.list("STORAGE_POLICIES", default=["task_results"])
    )

    # ==================== 上传 ====================
    # 整传的策略上限（流式落盘，管的是磁盘与合理性而非内存），更大的文件走分片上传
    UPLOAD_MAX_SIZE: int = field(
        default_factory=lambda: env.int(
            "UPLOAD_MAX_SIZE", env.int("STORAGE_REQUEST_MAX_SIZE", 2 * 1024 * 1024 * 1024)
        )
    )
    # 服务间场景要保留任务名这类原始文件名，默认放宽键规则
    STORAGE_KEY_RULE: str = field(default_factory=lambda: env.str("STORAGE_KEY_RULE", "lenient"))

    # ==================== 保留期清理（可选插件，默认关闭）====================
    # ⚠️ 默认关闭，且独立进程运行（python -m webx_storage retention）：存储服务多 worker 运行，
    # 挂在应用里的后台任务会每个 worker 各跑一份、互相删对方正在检查的东西。
    # 纯 mtime 判据是兜底；有"该不该留"的权威方时，请实现 RetentionPolicy.confirm 向它确认，
    # 并守住一条：权威方不可达时绝不删除（查不到 ≠ 不存在，删除不可逆）。
    STORAGE_RETENTION_ENABLED: bool = field(
        default_factory=lambda: env.bool("STORAGE_RETENTION_ENABLED", False)
    )
    STORAGE_RETENTION_DAYS: float = field(
        default_factory=lambda: env.float("STORAGE_RETENTION_DAYS", 90.0)
    )
    STORAGE_RETENTION_INTERVAL_HOURS: float = field(
        default_factory=lambda: env.float("STORAGE_RETENTION_INTERVAL_HOURS", 6.0)
    )
    # 单轮最多删多少个（0 = 不限）。防止一次配置失误删空整个存储，剩下的下一轮继续
    STORAGE_RETENTION_MAX_PER_ROUND: int = field(
        default_factory=lambda: env.int("STORAGE_RETENTION_MAX_PER_ROUND", 500)
    )


def configure() -> StorageSettings:
    """创建并注册存储服务配置为全局单例（入口调用一次）。"""
    return configure_settings(StorageSettings())  # type: ignore[return-value]
