"""业务服务层。"""

from webx.services.admin_service import admin_service
from webx.services.init_permission import init_permissions
from webx.services.storage_service import storage_service
from webx.services.permission_service import user_permission_service, user_role_service
from webx.services.user_service import user_service
from webx.services.verifycode_service import (
    VC_REASON_STATUS,
    vc_status,
    verifycode_service,
)

__all__ = [
    "user_service", "admin_service", "verifycode_service",
    "vc_status", "VC_REASON_STATUS",
    "user_permission_service", "user_role_service",
    "init_permissions",
    "storage_service",
]
