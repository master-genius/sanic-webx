from webx.middleware import internal_auth_middleware

# 内部接口组（worker 回调、服务间调用）：X-Internal-Token。
# 令牌未配置则整组拒绝一切请求。写端点另有 fencing（路由级装饰器 fence_lease），
# 两者是不同的问题：前者回答"你是不是我们自己的服务"，后者回答"你是不是这个任务当前的租户"。
MIDDLEWARES = [internal_auth_middleware]
