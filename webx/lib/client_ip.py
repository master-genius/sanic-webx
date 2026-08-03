"""客户端真实 IP 解析（反代感知）。

放在 ``lib`` 而不是 ``core.guards``：中间件（``webx.middleware.client_ip``）与
路由装饰器（``webx.core.guards.with_client_ip``）都要用它，而中间件层在 ``core``
之下——解析逻辑必须沉到两者共同的下游，否则就是一条反向依赖。
"""

from __future__ import annotations

from sanic.request import Request


def get_client_ip(request: Request) -> str:
    """解析真实客户端 IP（服务通常部署在反向代理之后）。

    优先级：``X-Real-IP`` → ``X-Forwarded-For``（取最左 = 原始客户端）→
    ``request.ip``（socket 对端，兜底）。Sanic 的 headers 查找本身大小写不敏感。

    ⚠️ 这些头可被客户端伪造，只有在**你自己的反代确实会覆写它们**时才可信；
    直接暴露在公网的服务不要拿它做安全判定。

    健壮性：任何异常都降级返回，绝不外抛——IP 解析失败不该影响主流程。
    """
    try:
        ip = request.headers.get("x-real-ip")
        if not ip:
            xff = request.headers.get("x-forwarded-for")
            if xff:
                ip = xff.split(",")[0]
        return (ip or request.ip or "").strip()
    except Exception:
        try:
            return request.ip or ""
        except Exception:
            return ""
