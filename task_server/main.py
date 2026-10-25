"""Sanic 入口模块：``sanic task_server.main:app --single-process``。

``app`` 必须在**模块级**创建：Sanic 起 worker 时会重新 import 本模块再按名字找 app。
"""

from task_server.config import configure

configure()  # 必须在创建 app 之前：框架内部通过全局 settings 取配置

from task_server.app import create_task_server_app  # noqa: E402

app = create_task_server_app()
