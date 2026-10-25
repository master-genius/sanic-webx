"""测试夹具：sqlite 内存库 + 装配好的应用。

为什么绕开 ``settings.tortoise_config()``：它写死 asyncpg engine（框架面向 PG）。
测试直接用 ``db_url="sqlite://:memory:"`` + ``generate_schemas()`` 建表，
就能真跑注册/登录/发码/权限全链路，不需要本地起 PostgreSQL，
也**不需要**为此把 engine 做成可配置——这是测试专用路径，框架不用动。

（`schema_sync` 只对 PG，测试不用它；它逐字沿用自线上验证过的实现。）
"""

import sys
from pathlib import Path

# 仓库根即项目根（本仓库不装成包，靠 sys.path 直接 import webx 与 app）
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from webx.paths import configure_root  # noqa: E402

configure_root(PROJECT_ROOT)

import importlib  # noqa: E402

# 只为 import 副作用：模块内的 configure_settings(Settings()) 与文案注册
importlib.import_module("app.config")
importlib.import_module("app.i18n")

import pytest  # noqa: E402
from sanic import Sanic  # noqa: E402
from tortoise import Tortoise  # noqa: E402

# 测试用到的全部模型模块
TEST_MODELS = ["app.models", "webx.models"]


@pytest.fixture
async def db():
    """每个用例一套全新的内存库，用例之间互不污染。"""
    await Tortoise.init(db_url="sqlite://:memory:", modules={"models": TEST_MODELS})
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise._drop_databases()
        await Tortoise.close_connections()


def _install_fake_sender() -> tuple[list, callable]:
    """把邮件通道换成"截获到列表"的假实现，返回 (列表, 还原函数)。

    也顺带演示了 senders 注册表的用法：换通道不需要改任何服务代码。
    """
    from webx.lib import senders

    sent: list[dict] = []
    original = senders.get_sender("email")

    async def fake_sender(identifier, code, *, valid_minutes, biz_type="", **_):
        sent.append({"identifier": identifier, "code": code, "biz_type": biz_type})

    senders.register_sender("email", fake_sender)

    def restore():
        senders.register_sender("email", original)

    return sent, restore


@pytest.fixture
def captured_codes():
    """函数级：截获本用例发出的验证码。"""
    sent, restore = _install_fake_sender()
    yield sent
    restore()


@pytest.fixture(scope="module")
def captured_codes_module():
    """module 级：供共用一个应用的 HTTP 端到端测试使用。"""
    sent, restore = _install_fake_sender()
    yield sent
    restore()


@pytest.fixture(scope="session")
def app_with_db(tmp_path_factory):
    """会话级应用 + sqlite。

    **一个进程只能装配一个应用**：蓝图是模块级单例（与原项目同款写法），
    重复注册进第二个 app 会 RouteExists。故整个测试会话共用这一个，
    各用例靠互不相同的账号标识保持隔离。

    另两个坑，都是 Sanic 测试客户端的机制决定的：

    - **夹具必须同步**。``test_client`` 自带事件循环，若在 pytest-asyncio 的循环里
      建 Tortoise 连接，请求期就跨循环死锁（表现为整个测试挂住）。故建连放在
      应用的 ``before_server_start`` 钩子里，天然落在正确的循环上。
    - **用文件 sqlite 而非 :memory:**。test_client 每发一个请求就起停一次应用，
      内存库会随连接一起消失，多步流程（发码 → 注册 → 登录）就断了。

    ``use_db=False`` 只是绕开 create_app 里连 PG 的 init_db，应用其余装配
    （蓝图扫描、中间件链、异常处理）全部真实。
    """
    from webx.core.app import create_app

    db_dir = tmp_path_factory.mktemp("e2e")
    db_url = f"sqlite://{db_dir}/e2e.db"

    async def init_sqlite(sanic_app):
        # _enable_global_fallback 与 webx.core.db.init_db 同款：启动钩子跑在后台任务里、
        # 请求处理在另一批任务里，不开这个开关请求期就拿不到连接
        await Tortoise.init(
            db_url=db_url, modules={"models": TEST_MODELS}, _enable_global_fallback=True
        )
        await Tortoise.generate_schemas(safe=True)  # 幂等，每次起服都可安全执行

    async def close_sqlite(sanic_app):
        await Tortoise.close_connections()

    # 存储根指向临时目录，避免测试往仓库里写文件
    from webx.config import settings
    settings.STORAGE_DIR = str(tmp_path_factory.mktemp("storage"))

    Sanic.test_mode = True
    application = create_app(
        "app.controller",
        name="webx_test",
        use_db=False,
        on_startup=[init_sqlite],
        on_shutdown=[close_sqlite],
    )
    # 暴露库文件路径：同步的 HTTP 用例不能直接跑 ORM，需要 stdlib sqlite3 改库时用
    application.ctx.sqlite_path = db_dir / "e2e.db"
    try:
        yield application
    finally:
        Sanic._app_registry.clear()
