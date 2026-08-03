from webx.middleware import auth_middleware

# 用户 API 组：需要标准认证
MIDDLEWARES = [auth_middleware]
