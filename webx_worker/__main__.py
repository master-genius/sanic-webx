"""`python -m webx_worker` 入口 —— 子命令分发。

    python -m webx_worker [run] [--home DIR] [--config NAME] ...   运行 worker（缺省子命令）
    python -m webx_worker cleanup [--status ... --timeout ...]      清理过期任务目录
    python -m webx_worker uploader                                   独立上传进程（由 worker 拉起）
    python -m webx_worker service one|multi [...]                    生成 systemd 单元

不带子命令（或第一个参数以 `-` 开头）时按 `run` 处理，便于部署包装脚本直接透传 flag。
"""

import sys

_SUBCOMMANDS = ("run", "cleanup", "uploader", "service")


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    cmd = "run"
    if argv and not argv[0].startswith("-"):
        cmd = argv.pop(0)

    if cmd == "run":
        from .app import main as run_main
        run_main(argv)
    elif cmd == "cleanup":
        from .cleanup import main as cleanup_main
        cleanup_main(argv)
    elif cmd == "service":
        from .service import main as service_main
        service_main(argv)
    elif cmd == "uploader":
        from .uploader import main as uploader_main
        uploader_main(argv)
    else:
        print(f"未知子命令: {cmd!r}（可用: {', '.join(_SUBCOMMANDS)}）", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
