"""独立存储服务：把 webx 的存储原语包成一个可单独部署的 HTTP 服务。

与主站内置的 ``/api/files``（用户令牌、bucket/name 两段键）不同，这是**服务间**使用的
存储节点：内部 Token 鉴权、任意层级的对象键、可插拔的键策略（KeyPolicy）、可选的保留期清理。

依赖关系：本包依赖 webx 的基础服务（存储原语、签名、配置、应用工厂），
webx 核心**不依赖**本包。入口见 ``python -m webx_storage`` 与 ``webx_storage.main:app``。
"""

from webx_storage.policy import KeyPolicy, StorageContext, register_policy
from webx_storage.retention import MtimeRetention, RetentionPolicy, sweep
from webx_storage.task_results import TaskResultsPolicy

__all__ = [
    "KeyPolicy",
    "MtimeRetention",
    "RetentionPolicy",
    "StorageContext",
    "TaskResultsPolicy",
    "register_policy",
    "sweep",
]
