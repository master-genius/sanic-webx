from webx.core.middleware import admin_auth_middleware, role_perm_middleware

# 管理 API 组：管理员认证 + 权限加载
# 顺序不可颠倒——role_perm_middleware 只读 ctx.user["user_id"]
MIDDLEWARES = [admin_auth_middleware, role_perm_middleware]
