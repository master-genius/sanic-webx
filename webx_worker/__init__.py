"""webx_worker —— 异构算力 worker 框架。

与具体计算任务无关的通用执行机制：拉取式领取（Lane 槽位）、租约 + fencing、
子进程执行与取消、心跳与自熔、失败上报的持久重放、产物异步上传、启动恢复。

⚠️ 本包**完全独立**：只依赖 Python 标准库 + httpx，不得 import 任何服务端框架。
它要能被单独打包，发给没有 webx 服务端的机器。该约束由
`tests/test_layering.py::test_worker_is_fully_independent_of_the_server_framework` 守护。

保持本文件轻量：任务子进程（可能跑在另一个解释器里）会 import `webx_worker.sdk`，
包的 `__init__` 不能拖进任何重依赖。

规范文档见 docs/worker.md；与任务服务之间的 HTTP 协议见 docs/worker-protocol.md。
"""

__version__ = "0.1.0"
