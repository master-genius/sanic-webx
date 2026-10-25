"""领取服务入口。

    python -m task_server                       # 监听 TASK_SERVER_HOST:TASK_SERVER_PORT（默认 127.0.0.1:9020）
    python -m task_server --port 9020 --host 127.0.0.1
    python -m task_server verify --url http://127.0.0.1:9020 --token ...   # 验证运行中的服务（不改数据）

⚠️ **固定单进程**，不提供 ``--workers``：挂起池必须集中在一个进程里（见包 docstring）。
默认只绑 127.0.0.1 是有意的——``/task/pokes`` 不鉴权；对 worker 暴露请走反代。
"""

from __future__ import annotations

import argparse
import os
import sys


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        from task_server.verify import main as verify_main

        sys.exit(verify_main(sys.argv[2:]))

    p = argparse.ArgumentParser(prog="python -m task_server", description="任务领取服务（单进程）")
    p.add_argument("--host", default=None, help="监听地址，覆盖 TASK_SERVER_HOST")
    p.add_argument("--port", type=int, default=None, help="监听端口，覆盖 TASK_SERVER_PORT")
    p.add_argument("--access-log", action="store_true", help="开启访问日志（默认关闭）")
    p.add_argument("--dev", action="store_true", help="开发模式")
    args = p.parse_args()

    if args.host:
        os.environ["TASK_SERVER_HOST"] = args.host
    if args.port is not None:
        os.environ["TASK_SERVER_PORT"] = str(args.port)

    from task_server.config import TaskServerSettings

    cfg = TaskServerSettings()  # 只为读取最终生效的 host / port，不注册为全局
    cmd = [
        sys.executable, "-m", "sanic", "task_server.main:app",
        "-H", cfg.TASK_SERVER_HOST, "-p", str(cfg.TASK_SERVER_PORT),
        "--single-process", "--no-motd",
        "--access-logs" if args.access_log else "--no-access-logs",
    ]
    if args.dev:
        cmd.append("--dev")
    os.execv(sys.executable, cmd)


if __name__ == "__main__":
    main()
