"""Sanic 入口模块：``sanic webx_storage.main:app``。

``app`` 必须在**模块级**创建：Sanic 起 worker 时会重新 import 本模块再按名字找 app，
藏进函数里 worker 就找不到（单 worker 也一样）。
"""

from webx_storage.config import configure

configure()  # 必须在创建 app 之前：框架内部通过全局 settings 取配置

from webx_storage.app import create_storage_app  # noqa: E402

app = create_storage_app()
