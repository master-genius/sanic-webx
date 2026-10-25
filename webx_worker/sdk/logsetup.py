"""任务子进程的日志配置 —— 任务脚本共用的单一入口。

级别与剔除是**互补的两道**，谁也替代不了谁：

- **不进前端**由 worker 侧的 redactors（见 ``log_writer`` / ``plugin.redactors``）保证——
  命中规则的行在交付出口整行剔除，**与级别无关**。级别是配置、配置会被改，
  把保证建立在「默认没人开 DEBUG」上等于没有保证。
- **但剔除发生在截断之后**：``get_truncated_content`` 取的是 ``TruncatedLogWriter``
  已经截好的「首 N + 尾 M」，那个窗口按**所有写入的行**计数。内部细节行若真的被打出来，
  会先占满窗口、再在交付时被丢掉——用户拿到的是一份被掏空的日志。
  **级别护的是截断窗口，剔除护的是交付边界。**

本机日志刻意不脱敏（那是排障的唯一来源），故级别也顺带决定 systemd 日志的信噪比。

级别策略
--------
- **内部实现细节一律 debug**：缓存的取/存、降级到哪一层、HTTP 请求打到哪、返回了什么码。
  它们量大且没有上下文——在截断窗口里挤掉真正的内容，在本机日志里淹没真正的错误。
- **失败才 info 以上**：真正影响任务结果的错误必须一眼看见。

默认级别 INFO ⟹ debug 行**根本不会产生**；
需要排查时把 ``WEBX_WORKER_TASK_LOG_LEVEL=DEBUG`` 注入子进程即可全部回来。

⚠️ ``.env`` / 覆盖层里写这个变量**没用**：配置文件层只对 worker 主进程可见，
子进程只认「真环境变量」。要走 channel 配置的 ``run_env``（与其它子进程级参数同一条
注入通道），或直接设为真环境变量。
"""

from __future__ import annotations

import logging
import os

from webx_worker import envnames as E

#: 子进程日志级别的环境变量名。见模块 docstring 中关于 run_env 的说明。
LOG_LEVEL_ENV = E.ENV_TASK_LOG_LEVEL

_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

#: 第三方 HTTP 客户端的 logger。httpx 在 **INFO** 级别就会为每个请求打一行
#: ``HTTP Request: GET <url> "HTTP/1.1 404 Not Found"``——每探测一次就是一行。
#: 它是第三方库打的，只能靠级别压住；而且级别压住才不会占满截断窗口（剔除发生在截断之后）。
_HTTP_LOGGERS = ("httpx", "httpcore", "urllib3")


def configure_task_logging() -> int:
    """配置任务子进程的根 logger，返回生效的级别。

    在 ``if __name__ == "__main__":`` 里、``main()`` 之前调用。
    """
    raw = (os.environ.get(LOG_LEVEL_ENV) or "").strip().upper()
    # 无效值静默回落 INFO：日志配置不该成为任务失败的原因
    level = getattr(logging, raw, None)
    if not isinstance(level, int):
        level = logging.INFO

    logging.basicConfig(level=level, format=_FORMAT)
    # basicConfig 在 root 已有 handler 时会**整个跳过**（连 level 都不设）。子进程正常
    # 启动时 root 是干净的，走不到这条；但只要有任何一环提前碰过 logging，级别就会
    # 静默失效——debug 行会毫无征兆地涌进本机日志，不能靠"正常情况下没人碰过"。
    logging.getLogger().setLevel(level)

    # 显式请求 DEBUG 时连第三方往返一起放出来（排查缓存/下载问题正需要它们）；
    # 其余情况一律压到 WARNING，只在真出错时才发声。
    http_level = level if level <= logging.DEBUG else logging.WARNING
    for name in _HTTP_LOGGERS:
        logging.getLogger(name).setLevel(http_level)

    return level
