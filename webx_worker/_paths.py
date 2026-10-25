"""包位置相关的小工具 —— 让 worker 拉起的子进程能 import 本包。

任务脚本可能由**另一个解释器**（conda 环境 / uv venv）以 cwd=任务目录启动，
那个解释器的 sys.path 里未必有 `webx_worker`。executor 因此必须把「包所在目录」
放进子进程的 PYTHONPATH；否则脚本里 `import webx_worker.sdk` 会直接 ImportError。
（子进程里只会用到 `webx_worker.sdk` 与 `webx_worker.envnames`，二者只依赖标准库 + httpx。）
"""

import os
from pathlib import Path

# 本包的父目录：把它放进 sys.path 就能 `import webx_worker`
PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent)


def with_package_on_pythonpath(env: dict[str, str]) -> dict[str, str]:
    """返回一份 env 副本，PYTHONPATH 前置本包所在目录（保留已有内容，去重）。"""
    out = dict(env)
    parts = [p for p in (out.get("PYTHONPATH") or "").split(os.pathsep) if p]
    if PACKAGE_PARENT not in parts:
        parts.insert(0, PACKAGE_PARENT)
    out["PYTHONPATH"] = os.pathsep.join(parts)
    return out
