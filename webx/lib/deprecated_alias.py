"""已废弃路径的兼容别名：只转发到规范处理器，响应打上 ``Deprecation`` 头。

规范见 ``docs/api-conventions.md`` 规则 9：别名**只做转发**，不得有自己的逻辑分支——
否则新旧两条路径迟早行为分叉，且废弃时才发现有人依赖了旧路径独有的行为。
"""

from __future__ import annotations

import functools
from typing import Awaitable, Callable, Iterable


def deprecated_alias(
    bp,
    handler: Callable[..., Awaitable],
    uri: str,
    *,
    methods: Iterable[str],
    successor: str,
    name: str,
    **route_kwargs,
) -> None:
    """把 ``handler`` 另挂到旧路径 ``uri`` 上。

    Args:
        bp: 旧路径所属的蓝图（路径相对它的 ``url_prefix``）。
        handler: 规范路径上已注册的处理器（含其校验等装饰器），原样复用。
        successor: 规范路径模板，路径参数用 ``{名字}`` 占位，如 ``/api/uploads/{upload_id}``，
            写进 ``Link: <...>; rel="successor-version"``。
        name: 路由名，需在蓝图内唯一（不能与 ``handler.__name__`` 相同）。
        route_kwargs: 透传给 ``add_route``，如 ``stream=True``（分片追加是流式 body）。
    """

    @functools.wraps(handler)   # 保留签名与 __module__：蓝图归属判定看 handler 出自哪个模块
    async def alias(request, **path_params):
        resp = await handler(request, **path_params)
        resp.headers["Deprecation"] = "true"
        resp.headers["Link"] = f'<{successor.format(**path_params)}>; rel="successor-version"'
        return resp

    bp.add_route(alias, uri, methods=frozenset(m.upper() for m in methods), name=name, **route_kwargs)
