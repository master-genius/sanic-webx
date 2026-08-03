"""API 组中间件：在根级链条之上追加认证。

最终链条 = [request_id, client_ip, lang, access_note] + [auth_middleware]
（父级继承 + 本目录追加，由 auto_blueprint 去重保序）。
"""

from webx.middleware import auth_middleware

MIDDLEWARES = [auth_middleware]
