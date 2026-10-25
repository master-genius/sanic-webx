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


def _free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def reset_orm_caches() -> None:
    """清掉 Tortoise 按 (连接名, schema, 表名) 缓存的 INSERT / UPDATE 语句。

    这个缓存**不区分数据库方言**：同一进程里先用 sqlite 建过模型、再切到 PostgreSQL，
    模型会拿着 sqlite 风格的 SQL 去打 PG（反过来亦然），报莫名其妙的语法错误。
    生产里一个进程只连一种库，不受影响；只有本测试套件 sqlite 与 PG 混用时才需要。
    """
    from tortoise.backends.base.executor import EXECUTOR_CACHE

    EXECUTOR_CACHE.clear()


@pytest.fixture
async def db():
    """每个用例一套全新的内存库，用例之间互不污染。"""
    reset_orm_caches()
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


@pytest.fixture
def fake_sms(monkeypatch):
    """走**真实默认路径**发短信：配置选中一个假驱动，截获 (to, template, params)。

    链路是 验证码服务 → senders 里默认注册的 mobile 通道 → 网关 → 假驱动，
    中间没有任何手工 register_sender，因此证明的正是「配好 .env 即可用」。
    返回对象的 ``.sent`` 是截获列表，``.fail`` 置为异常实例则下次发送抛它。
    """
    from webx.config import get_settings
    from webx.lib.sms import SmsDriver, gateway, register_driver

    class Fake(SmsDriver):
        name = "fake"
        sent: list = []
        fail = None
        configured = True

        def is_configured(self):
            return self.configured

        async def send(self, to, template_code, params=None, validity_period=None):
            if self.fail:
                raise self.fail
            self.sent.append({"to": to, "template": template_code, "params": params})
            return "fake-id"

    driver = Fake()
    driver.sent = []
    register_driver("fake", lambda: driver)
    cfg = get_settings()
    monkeypatch.setattr(cfg, "SMS_ENABLED", True)
    monkeypatch.setattr(cfg, "SMS_PROVIDER", "fake")
    monkeypatch.setattr(cfg, "SMS_TEMPLATES", {"verify_code": "SMS_TEST"})
    try:
        yield driver
    finally:
        gateway._FACTORIES.pop("fake", None)
        gateway._INSTANCES.pop("fake", None)


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
        reset_orm_caches()
        await Tortoise.init(
            db_url=db_url, modules={"models": TEST_MODELS}, _enable_global_fallback=True
        )
        await Tortoise.generate_schemas(safe=True)  # 幂等，每次起服都可安全执行

    async def close_sqlite(sanic_app):
        await Tortoise.close_connections()

    # 存储根指向临时目录，避免测试往仓库里写文件
    # ⚠️ 必须设在真实配置对象上：`webx.config.settings` 是只读代理，对它 setattr 只会在代理自己身上
    # 加个影子属性，而 ``settings.storage_path`` 这类属性读的是底层对象——曾因此让 e2e 一直把文件
    # 写进仓库的 ./storage，而不是临时目录
    from webx.config import get_settings
    get_settings().STORAGE_DIR = str(tmp_path_factory.mktemp("storage"))
    # 其它用例可能已经用默认目录创建过全局存储门面，换目录后要丢掉它缓存的 store
    from webx.services.storage_service import storage_service
    storage_service._store = storage_service._chunked = storage_service._signer = None

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


# ==================== PostgreSQL（认领 / 租约的 SQL 是 PG 专有的）====================


@pytest.fixture(scope="session")
def pg_dsn(tmp_path_factory):
    """会话级：拿到一个可用的 PG 的 DSN（见 tests/pg_server.py 的获取顺序）。

    拿不到时测试**失败**而不是跳过——跳过会让认领逻辑"没被验证却显示绿色"。
    确需跳过请显式设 WEBX_SKIP_PG_TESTS=1。
    """
    import os

    if os.environ.get("WEBX_SKIP_PG_TESTS") == "1":
        pytest.skip("WEBX_SKIP_PG_TESTS=1：显式跳过需要 PostgreSQL 的测试")

    url = os.environ.get("WEBX_TEST_PG_URL")
    if url:
        yield url
        return

    from tests.pg_server import PgUnavailable, start_pg

    try:
        inst = start_pg(tmp_path_factory.mktemp("pg"))
    except PgUnavailable as e:
        pytest.fail(f"需要 PostgreSQL 才能验证认领 / 租约逻辑，但环境里拿不到:\n{e}", pytrace=False)
    try:
        yield inst.dsn
    finally:
        inst.stop()


@pytest.fixture
async def pgdb(pg_dsn):
    """函数级：连上 PG、建表、建认领索引、清空数据。每个用例拿到一个干净的库。"""
    from webx.models.task import Task

    reset_orm_caches()
    await Tortoise.init(db_url=pg_dsn, modules={"models": TEST_MODELS}, _enable_global_fallback=True)
    await Tortoise.generate_schemas(safe=True)
    conn = Tortoise.get_connection("default")
    await conn.execute_query(Task.CLAIM_INDEX_DDL)
    tables = [m._meta.db_table for app in Tortoise.apps.values() for m in app.values()]
    quoted = ", ".join(f'"{t}"' for t in tables)
    await conn.execute_query(f"TRUNCATE {quoted} RESTART IDENTITY CASCADE")
    try:
        yield conn
    finally:
        await Tortoise.close_connections()


@pytest.fixture(scope="session")
def pg_app(pg_dsn):
    """会话级：一个专用的、连真实 PG 的完整应用（认领 / 租约的 SQL 是 PG 专有的）。

    为什么不复用会话级的 ``app_with_db``：
      1. 它被大量 sqlite 用例反复起停过，留下的状态会让后来的 ``ReusableClient.run()`` 在
         ``_start_servers`` 里抛 "Cannot name task outside of a running application"；
      2. sanic-testing 每起停一次就往路由器上叠中间件和信号路由，共用的 app 累计到 ~870 次起停后
         ``sanic_routing`` 生成路由树时递归超限——专用 app 把 PG 用例的开销隔离在自己名下。
    同一个进程里装配两个 app 共用同一批蓝图是可以的（当前 Sanic 版本实测），但**再多装配一个**
    （比如每个模块各一个）会在路由注册时撞 "Route already registered"——所以整个会话共用这一个，
    每个用例通过 ``ReusableClient`` 只起停它一次。
    """
    from webx.config import get_settings
    from webx.core.app import create_app

    async def init_pg(sanic_app):
        reset_orm_caches()
        await Tortoise.init(db_url=pg_dsn, modules={"models": TEST_MODELS}, _enable_global_fallback=True)
        await Tortoise.generate_schemas(safe=True)

    async def close_pg(sanic_app):
        await Tortoise.close_connections()

    get_settings().STORAGE_DIR = str(__import__("tempfile").mkdtemp(prefix="webx-test-storage-"))
    from webx.services.storage_service import storage_service
    storage_service._store = storage_service._chunked = storage_service._signer = None

    Sanic.test_mode = True
    name = "webx_pg_e2e"
    # 只装配内置路由：任务接口都在内置组里，不需要业务示例 app.controller
    # （它的模块级蓝图在前序用例里被反复装配 / 重新 import，再装一次会撞 "Route already registered"）
    application = create_app(name=name, use_db=False, on_startup=[init_pg], on_shutdown=[close_pg])
    try:
        yield application
    finally:
        Sanic._app_registry.pop(name, None)


@pytest.fixture
def use_pg(pg_app, pg_dsn):
    """本用例内连干净的真实 PG，返回一个**可复用的**测试客户端（每个用例只起停一次应用）。

    不用 ``app.test_client``：它每发一个请求就把应用完整起停一次，请求数多的用例既慢又会
    加速 sanic-testing 的累积。
    """
    from sanic_testing.reusable import ReusableClient
    from tests.pg_helpers import ensure_schema, reset_schema

    reset_schema(pg_dsn)
    ensure_schema(pg_dsn)  # 直接调服务层的用例没有"应用启动时建表"这一步
    # ReusableClient 默认用 randint(5000, 65000) 选端口，偶尔撞上已被占用的端口（Errno 98），
    # 一次失败还会让应用状态错乱、引发后面一连串 error。改由操作系统分配确定空闲的端口，
    # 并对"刚探测完又被别人抢走"的极小窗口做重试
    last = None
    for _ in range(5):
        client = ReusableClient(pg_app, port=_free_port())
        try:
            client.run()
            break
        except OSError as e:
            last = e
            try:
                client.stop()
            except Exception:  # noqa: BLE001 —— 启动都没成功，停止失败不重要
                pass
    else:
        raise last
    try:
        yield client
    finally:
        client.stop()
