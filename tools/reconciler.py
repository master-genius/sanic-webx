"""独立补偿器进程。

    python tools/reconciler.py
    python tools/reconciler.py --scan-interval 300 --lease-interval 3

设计：
  • 独立进程，不依赖 Sanic Web 服务，自行初始化数据库连接——与单 / 多进程 Web 服务解耦
    （``--workers N`` 时补偿器不会跑 N 份）
  • 分发是 worker 主动领取，本进程**不需要任何投递通道**：回收租约只是一次 UPDATE，随后发个内网 poke
    （失败也只损失延迟）
  • 支持 systemd 守护运行；收到 SIGTERM / SIGINT 优雅退出

优先级：命令行 > .env > 默认值。所有参数默认 None（= 不覆盖）。

准入 / 巡检钩子：补偿器本身不认识任务类型，具体动作由拥有任务知识的模块注册
（``webx.services.reconciler.register_sweep``）。本入口 import 了任务服务模块以完成默认注册；
业务自己的钩子在业务模块里注册，并通过 ``--modules`` / ``WEBX_MODULES`` 带进来。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _bootstrap  # noqa: E402

_INTERVAL_ARGS = (
    ("scan-interval", "SECONDS", "慢扫描间隔（秒），覆盖 RECONCILER_SCAN_INTERVAL"),
    ("stale-threshold-minutes", "MIN", "RUNNING 心跳超时阈值（分钟）"),
    ("queued-timeout-minutes", "MIN", "QUEUED 超时阈值（分钟）"),
    ("abandon-threshold-hours", "HOURS", "放弃阈值（小时）"),
    ("none-task-timeout-hours", "HOURS", "none 空任务清理阈值（小时）"),
    ("admit-interval", "SECONDS", "准入 / 巡检循环间隔（秒）"),
    ("lease-interval", "SECONDS", "租约回收循环间隔（秒）"),
)


async def _run(args: argparse.Namespace) -> int:
    # 业务钩子靠 import 注册；任务服务模块自带的默认钩子也在此加载（存在才导入）
    import importlib.util

    if importlib.util.find_spec("webx.services.task") is not None:
        importlib.import_module("webx.services.task")

    from webx.services.reconciler import build_reconciler

    overrides = {a[0].replace("-", "_"): getattr(args, a[0].replace("-", "_")) for a in _INTERVAL_ARGS}
    rec = build_reconciler(**overrides)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # 非 POSIX 平台
            pass

    await rec.start()
    logging.info("补偿器进程已就绪，等待退出信号")
    await stop.wait()
    await rec.stop()

    from webx.lib.task_poke import close_poke_client

    await close_poke_client()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="webx 补偿器（命令行 > .env > 默认值）")
    for name, metavar, help_ in _INTERVAL_ARGS:
        parser.add_argument(f"--{name}", type=int, default=None, metavar=metavar, help=help_)
    parser.add_argument("--modules", default=None, help="逗号分隔的业务模块包名（其钩子需要被 import）")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    _bootstrap.run(parser.parse_args(), _run)


if __name__ == "__main__":
    main()
