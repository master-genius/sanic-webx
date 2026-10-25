"""启动入口 —— 业务不需要再写 ``app/main.py``。

装配全部交给 ``create_app(modules=[...])``：模块包只要摆成约定的样子
（``<pkg>/controller/`` 必须有，``<pkg>/i18n.py`` 可选），就会被自动扫描装配。
想按部署切分模块组合，改 ``MODULES`` 或传 ``--modules app,appx`` 即可。

⚠️ 三处时序，改动前先读懂：

  1. ``configure_root()`` 必须最先执行——``webx.config`` 在 import 期就要按项目根
     定位并读取 .env。
  2. ``settings.apply_flags()`` 必须在 ``create_app()`` **之前**——其中的
     ``setup_logging(debug=...)`` 读的是**那一刻**的值。
  3. ``app`` 必须在**模块级**创建，不能藏进 ``main()``。Sanic 启动时会重新 import
     本模块、再按应用名去注册表里找它；藏在函数里 worker 就找不到，报
     ``Sanic app name 'xxx' not found``——单 worker 也一样，因为它同样走 worker 管理器。
     同理 ``app.run()`` 只能在 ``__main__`` 守卫内调用：worker 重新 import 时若再调
     一次，会以 ``RuntimeError: context has already been set`` 起不来。

所以这里在模块级就 parse 一次参数：worker 重新 import 时会再 parse 一次，
拿到同样的 argv、得到同样的结果，幂等无副作用。
"""

import argparse
from pathlib import Path

from webx.paths import configure_root

configure_root(Path(__file__).parent)  # ← 必须最先执行

from app.config import settings  # noqa: E402  (import 即注册全局配置)
from webx.config import env  # noqa: E402
from webx.core.app import create_app  # noqa: E402

# 本部署装配哪些业务模块。一个进程可以只跑一部分，按需切分到不同服务。
#
# ⚠️ 必须用 ``env.str`` 而非 ``os.environ``：environs>=15 的 ``read_env()`` 只把
# .env 写进 Env 实例内部、**不回写 os.environ**（见 webx.config 模块 docstring），
# 用 os.environ 读会让 .env 里的配置静默失效。``env.str`` 两个来源都认，
# 且 shell 环境变量优先于 .env（实测），正好符合「命令行/环境覆盖配置文件」的直觉。
#
# 三条传参口子，优先级由高到低：
#   python run.py --modules app,appx        ← 自己的 argparse
#   WEBX_MODULES=app,appx sanic run:app     ← sanic CLI **严格拒绝未知参数**
#                                             （实测 unrecognized arguments），
#                                             多 worker 部署只能走环境变量
#   .env 里的 WEBX_MODULES=app              ← 常规配置位
DEFAULT_MODULES = env.str("WEBX_MODULES", "app")

# 挂载哪些**内置**路由组。与 WEBX_MODULES 是两个独立开关：前者管业务模块，
# 这个管框架自带的 auth / api / admin / public / internal 五组，且内置组恒排在业务模块**之前**加载。
#   不配 / 留空 / 无法识别   四组全挂（默认，等同 builtin_controller=True）
#   false / 0 / no / none    一组都不挂，纯业务服务
#   auth,api                 只挂列出的组；写错的组名剔除并告警
DEFAULT_BUILTIN = env.str("WEBX_BUILTIN", "")

_BUILTIN_OFF = ("false", "0", "no", "none", "off")
_BUILTIN_ON = ("", "true", "1", "yes", "on", "all")


def parse_builtin(spec: str) -> "bool | list[str]":
    """把 ``WEBX_BUILTIN`` / ``--builtin`` 的字符串形态翻成 ``builtin_controller`` 参数。

    **只有显式关闭才不加载**，其余一律加载——「没配」「留空」「值看不懂」三种情况
    都回退成全加载。理由是可用性优先：静默关掉全部内置路由会让 ``/auth/login``
    一起消失，比多挂几组更难排查，而真不要内置路由的人会显式写 ``false``。

    组名拼错不在这里拦（那样会丢掉「auth 有效、nope 无效」这类信息），
    交给 `_builtin_packages` 逐个剔除并告警。
    """
    s = spec.strip().lower()
    if s in _BUILTIN_OFF:
        return False
    if s in _BUILTIN_ON:
        return True
    groups = [g.strip() for g in spec.split(",") if g.strip()]
    return groups or True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="webx server")
    parser.add_argument("--host", default=settings.SERVER_HOST)
    parser.add_argument("--port", type=int, default=settings.SERVER_PORT)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dev", action="store_true")
    parser.add_argument(
        "--modules",
        default=DEFAULT_MODULES,
        help="逗号分隔的业务模块包名，如 app 或 app,appx（默认 %(default)s，"
        "亦可用环境变量 WEBX_MODULES）",
    )
    parser.add_argument(
        "--builtin",
        default=DEFAULT_BUILTIN,
        help="挂载哪些内置路由组，逗号分隔（可选 auth,api,admin,public,internal）；"
        "不给则全挂。亦可用环境变量 WEBX_BUILTIN",
    )
    parser.add_argument(
        "--no-builtin",
        action="store_true",
        help="一个内置路由组都不挂（优先级高于 --builtin 与 WEBX_BUILTIN）",
    )
    # parse_known_args：worker 子进程的 argv 可能带 Sanic 自己的参数，不能因此崩掉
    return parser.parse_known_args()[0]


ARGS = parse_args()
settings.apply_flags(debug=ARGS.debug, dev=ARGS.dev)

# 必须是模块级（见上文时序 3）
app = create_app(
    modules=[m for m in ARGS.modules.split(",") if m],
    # --no-builtin 是"我确定不要"的显式表达，压过其余一切来源
    builtin_controller=False if ARGS.no_builtin else parse_builtin(ARGS.builtin),
)


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
