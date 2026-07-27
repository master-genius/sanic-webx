"""启动入口。

⚠️ 三处时序，改动前先读懂：

  1. ``configure_root()`` 必须最先执行——``webx.config`` 在 import 期就要按项目根
     定位并读取 .env。
  2. ``settings.apply_flags()`` 必须在 import ``app.main`` 之前——``create_app()``
     在模块级执行，其中 ``setup_logging(debug=...)`` 读的是**那一刻**的值。
  3. ``app`` 必须在**模块级**创建，不能藏进 ``main()``。Sanic 启动时会重新 import
     本模块、再按应用名去注册表里找它；藏在函数里 worker 就找不到，报
     ``Sanic app name 'xxx' not found``——单 worker 也一样，因为它同样走 worker 管理器。

所以这里在模块级就 parse 一次参数：worker 重新 import 时会再 parse 一次，
拿到同样的 argv、得到同样的结果，幂等无副作用。
"""

import argparse
from pathlib import Path

from webx.paths import configure_root

configure_root(Path(__file__).parent)  # ← 必须最先执行

from app.config import settings  # noqa: E402  (import 即注册全局配置)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="webx server")
    parser.add_argument("--host", default=settings.SERVER_HOST)
    parser.add_argument("--port", type=int, default=settings.SERVER_PORT)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dev", action="store_true")
    # parse_known_args：worker 子进程的 argv 可能带 Sanic 自己的参数，不能因此崩掉
    return parser.parse_known_args()[0]


ARGS = parse_args()
settings.apply_flags(debug=ARGS.debug, dev=ARGS.dev)

from app.main import app  # noqa: E402  (须在 apply_flags 之后，且必须是模块级)


def main() -> None:
    app.run(
        host=ARGS.host,
        port=ARGS.port,
        workers=ARGS.workers,
        debug=ARGS.debug,
        access_log=True,
    )


if __name__ == "__main__":
    main()
