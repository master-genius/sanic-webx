"""可插拔中间件目录。

**这是本框架相对原项目的主要结构改进。** 原项目的所有中间件挤在
``core/middleware.py`` 一个文件里，加一条就得改核心文件；这里改成
「一个中间件一个文件」的独立目录：

  - 框架自带的通用件放在本目录（``client_ip`` / ``lang`` / ``request_id`` …）
  - 业务自己的中间件放在项目的 ``app/middlewares/``，结构完全相同
  - 两边都只是普通函数，通过 ``__mid__.py`` 的 ``MIDDLEWARES`` 列表挂载

``core/middleware.py`` 只保留认证/授权这一类**框架必须提供的核心能力**
（且已依赖倒置、不 import 任何业务模型），其余一切都在这里加，不动核心。

挂载示例（业务项目 ``app/controller/__mid__.py``）::

    from webx.middleware import client_ip, lang, request_id
    from app.middlewares.audit import audit_middleware   # 业务自己的

    MIDDLEWARES = [request_id, client_ip, lang, audit_middleware]
"""

from webx.middleware.client_ip import client_ip
from webx.middleware.lang import lang
from webx.middleware.request_id import request_id

__all__ = ["client_ip", "lang", "request_id"]
