"""应用装配：一次 create_app 调用搞定日志/CORS/蓝图/DB/异常处理。

⚠️ 本模块最后把 ``app`` 这个名字绑给了 Sanic 实例，它会**遮蔽同名的 app 包**。
所以对本项目包的 import 一律写成 ``from app import xxx``（只绑定子模块名），
不要写 ``import app.xxx``——后者会把 ``app`` 绑成包、随后又被 Sanic 实例覆盖，
读代码的人无法一眼看出 ``app`` 到底是什么。
"""

import importlib

from webx.core.app import create_app
from webx.lib.admin_permissions import register_admin_permissions
from webx.services.init_permission import init_permissions

# 注册业务文案：要的只是 import 副作用（模块内的 register(...) 调用）。
# 业务自己的权限声明模块也在这里 import。
importlib.import_module("app.i18n")

# 内置管理端的权限定义写进注册表（纯内存，幂等）
register_admin_permissions()


async def sync_permissions(sanic_app):
    """启动时把权限定义同步进数据库（幂等，可安全重复执行）。"""
    await init_permissions(orgid="")


app = create_app("app.controller", on_startup=[sync_permissions])
