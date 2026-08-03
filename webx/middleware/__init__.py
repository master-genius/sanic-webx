"""中间件包 —— 一个中间件一个模块，由 controller 的 ``__mid__.py`` 按需引入。

为什么独立成包而不放 ``core/``：

  - ``core/`` 是地板层（应用工厂 / 蓝图扫描 / ORM / 异常 / 日志），应当**稳定**；
    中间件随业务增长，塞进 core 会让「加一条中间件」变成「改 core」。
  - 更硬的理由：中间件是唯一需要依赖 ``models`` / ``services`` 的鉴权代码。
    搬出来后 ``core`` 不再有这条依赖，包级成环的风险从结构上消失。

分模块口径：**一个中间件一个模块**，唯一的例外是 ``perm.py``——
``role_perm_middleware`` 写 ``request.ctx.permissions``，``require_permission`` 与
``has_perm`` 读它，三者共用同一份数据契约，拆开只会变成互相 import。

框架自带的通用件都在本目录；业务自己的中间件放在项目的 ``app/middlewares/``，
结构完全相同——两边都只是普通函数，通过 ``__mid__.py`` 的 ``MIDDLEWARES`` 挂载，
**加中间件永远不需要改框架文件**。

认证三阶段::

    阶段 1  auth_middleware       只验令牌、注入 ctx.user（不查库）
    阶段 2  role_perm_middleware  查库加载组织角色权限 + 用户单独权限
    阶段 3  @require_permission   路由级判定（组织超管 / super 角色自动放行）

挂载示例（``controller/<组>/__mid__.py``）::

    from webx.middleware import auth_middleware, lang, request_id
    from app.middlewares.audit import audit_middleware      # 业务自己的

    MIDDLEWARES = [request_id, lang, auth_middleware, audit_middleware]

⚠️ 顺序不可颠倒：``role_perm_middleware`` 只读 ``ctx.user["user_id"]``，
令牌无效在上一道即已 401。
"""

from webx.middleware.admin import admin_auth_middleware
from webx.middleware.auth import (
    DEFAULT_CLAIMS,
    auth_middleware,
    make_auth_middleware,
    make_role_middleware,
)
from webx.middleware.client_ip import client_ip
from webx.middleware.cors import build_cors_headers, setup_cors
from webx.middleware.lang import lang
from webx.middleware.perm import (
    ORG_HEADER,
    SUPER_ROLE,
    has_perm,
    is_privileged,
    make_has_permission,
    require_permission,
    role_perm_middleware,
)
from webx.middleware.request_id import request_id

__all__ = [
    # 认证 / 授权
    "DEFAULT_CLAIMS",
    "auth_middleware",
    "admin_auth_middleware",
    "make_auth_middleware",
    "make_role_middleware",
    # 权限（阶段 2/3）
    "ORG_HEADER",
    "SUPER_ROLE",
    "role_perm_middleware",
    "require_permission",
    "is_privileged",
    "has_perm",
    "make_has_permission",
    # 通用件
    "client_ip",
    "lang",
    "request_id",
    # CORS（应用级装配）
    "build_cors_headers",
    "setup_cors",
]
