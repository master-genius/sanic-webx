# 认证目录不挂中间件：登录/注册本就是公开的。
# 少数需要登录的端点（logout / refresh）用 @require_token 装饰器单独控制。

MIDDLEWARES = []
