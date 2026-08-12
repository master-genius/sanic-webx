"""数据模型。

只要没显式关掉内置路由（``WEBX_BUILTIN=false``），本模块就会被自动挂接，跑 dbsync 建表。
不需要用户体系的项目不列它即可，框架其余部分不依赖这些表。
"""

from webx.models.admin import Admin
from webx.models.rbac import (
    Organization,
    Permission,
    RolePermission,
    UserOrg,
    UserPermission,
    UserRole,
)
from webx.models.user import User
from webx.models.verify_code import (
    VerifyCode,
    VerifyCodeBizType,
    VerifyCodeChannel,
    VerifyCodeReason,
    VerifyCodeState,
)

__all__ = [
    # 身份
    "User", "Admin",
    # 验证码
    "VerifyCode", "VerifyCodeState", "VerifyCodeBizType",
    "VerifyCodeChannel", "VerifyCodeReason",
    # 组织 / 角色 / 权限
    "Organization", "UserOrg", "UserRole",
    "Permission", "RolePermission", "UserPermission",
]
