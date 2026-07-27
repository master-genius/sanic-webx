"""根级中间件：对全部控制器生效，子目录自动继承。

这里放的都是「与鉴权无关的上下文注入」——框架自带件与业务自有件混用，
挂载方式完全相同。
"""

from webx.middleware import client_ip, lang, request_id

from app.middlewares.access_note import access_note

MIDDLEWARES = [request_id, client_ip, lang, access_note]
