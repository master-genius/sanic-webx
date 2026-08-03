"""管理端认证中间件：标准认证之上再要求 ``role == admin``。

单独成模块是为了让 ``__mid__.py`` 的引入语义直白（管理端挂 admin、用户端挂 auth），
而实现只是 ``make_auth_middleware`` 的一次带参调用——参照项目在这里是**两份复制的
实现**，通用化后没有理由再复制一遍：要给管理端加二次验证，改的是工厂参数或在链上
再追加一条中间件，而不是维护两份几乎相同的 token 校验代码。
"""

from __future__ import annotations

from webx.middleware.auth import make_auth_middleware

admin_auth_middleware = make_auth_middleware(
    required_roles=["admin"], name="admin_auth_middleware"
)
"""管理端认证：在标准认证之上要求 role == admin。"""
