"""独立存储服务入口。

    python -m webx_storage                         # 启动服务（单 worker）
    python -m webx_storage --workers 4             # 多 worker
    python -m webx_storage --port 9010 --storage-dir /data/storage
    python -m webx_storage retention --dry-run     # 保留期清理（独立进程，见 retention.py）
    python -m webx_storage verify --url http://127.0.0.1:9010 --token ...   # 对运行中的服务做端到端验证

服务模式只做「参数翻译 + 环境覆盖」，随后 exec 进 Sanic CLI 跑 ``webx_storage.main:app``：
多 worker 走 Sanic 官方 worker manager，子进程按 module:app 重导入天然能找到 app。

优先级：命令行 > 环境变量 / .env > 默认值。
"""

from __future__ import annotations

import argparse
import os
import sys


def serve(argv: list[str]) -> None:
    p = argparse.ArgumentParser(prog="python -m webx_storage", description="独立存储服务")
    p.add_argument("--host", default=None, help="监听地址，覆盖 STORAGE_HOST")
    p.add_argument("--port", type=int, default=None, help="监听端口，覆盖 STORAGE_PORT")
    p.add_argument("--workers", type=int, default=None, help="worker 进程数，覆盖 STORAGE_WORKERS")
    p.add_argument("--fast", action="store_true", help="worker 数拉满到 CPU 核数")
    p.add_argument("--single-process", action="store_true", help="强制单进程（容器/受限环境）")
    p.add_argument("--storage-dir", default=None, help="存储根目录，覆盖 STORAGE_DIR")
    p.add_argument("--access-log", action="store_true", help="开启访问日志（默认关闭）")
    p.add_argument("--dev", action="store_true", help="开发模式（debug + 自动重载）")
    args = p.parse_args(argv)

    # 命令行覆盖：预置环境变量（environs 不覆盖已存在的 os.environ），exec 后子进程继承
    if args.storage_dir:
        os.environ["STORAGE_DIR"] = args.storage_dir
    if args.host:
        os.environ["STORAGE_HOST"] = args.host
    if args.port is not None:
        os.environ["STORAGE_PORT"] = str(args.port)
    if args.workers is not None:
        os.environ["STORAGE_WORKERS"] = str(args.workers)

    from webx_storage.config import StorageSettings

    cfg = StorageSettings()  # 只为读取最终生效的 host/port/workers，不注册为全局
    cmd = [
        sys.executable, "-m", "sanic", "webx_storage.main:app",
        "-H", cfg.SERVER_HOST, "-p", str(cfg.SERVER_PORT), "--no-motd",
    ]
    if args.single_process:
        cmd.append("--single-process")
    elif args.fast:
        cmd.append("--fast")
    else:
        cmd += ["-w", str(max(1, cfg.STORAGE_WORKERS))]
    cmd.append("--access-logs" if args.access_log else "--no-access-logs")
    if args.dev:
        cmd.append("--dev")

    os.execv(sys.executable, cmd)


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] == "retention":
        from webx_storage.retention import main as retention_main

        sys.exit(retention_main(argv[1:]))
    if argv and argv[0] == "verify":
        from webx_storage.verify import main as verify_main

        sys.exit(verify_main(argv[1:]))
    serve(argv)


if __name__ == "__main__":
    main()
