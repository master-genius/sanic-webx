"""webx_worker 测试夹具。

核心是一台**真的**本地假服务端（`webx_worker.fake_server.FakeTaskServer`，线程化 HTTP），
而不是一堆 mock：worker 的 HTTP 行为（路径、方法、fencing 头、重试）只有对着真的
socket 才测得出来。假服务端对写端点强制 fencing、对旧路径/错方法回 404/405，
所以「漏注入令牌」「用了旧路径」这类协议违规一定会在这里红。

提供：
  · `server`        一台假服务端
  · `harness`       在测试事件循环里跑一个完整的 worker（领取循环 + 执行器），
                    配置已指向 `server`；可注册 channel 脚本
  · autouse 重置    全局单例（fencing 注册表、在跑任务表、HTTP 客户端、链路记账）
"""

import asyncio
import sys
import textwrap
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from webx_worker import config as wconfig  # noqa: E402
from webx_worker import fencing, heartbeat as hb_mod, lease as lease_mod  # noqa: E402
from webx_worker.callback import callback_client  # noqa: E402
from webx_worker.fake_server import FakeTaskServer  # noqa: E402
from webx_worker.http_file_client import http_file_client  # noqa: E402
from webx_worker.link_health import link_health  # noqa: E402


def _reset_clients() -> None:
    # httpx.AsyncClient 绑定创建它的事件循环；每个用例有自己的循环，必须丢弃旧客户端
    callback_client._client = None
    callback_client._fence_rejected.clear()
    lease_mod.lease_client._client = None
    http_file_client._client = None


@pytest.fixture(autouse=True)
def _reset_worker_globals():
    fencing._leases.clear()
    lease_mod._running_tasks.clear()
    link_health._kind = None
    link_health._count = 0
    wconfig.reset_file_layers()
    _reset_clients()
    yield
    fencing._leases.clear()
    lease_mod._running_tasks.clear()
    wconfig.reset_file_layers()
    _reset_clients()


@pytest.fixture
def server():
    srv = FakeTaskServer().start()
    yield srv
    srv.stop()


def write_script(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


class Harness:
    """在测试事件循环里跑一个完整 worker（不含 CLI 与信号处理）。"""

    def __init__(self, server: FakeTaskServer, tmp_path: Path, monkeypatch) -> None:
        self.server = server
        self.tmp = tmp_path
        self.work_dir = tmp_path / "work"
        self.scripts = tmp_path / "scripts"
        self.scripts.mkdir()
        self.channels: dict[str, dict] = {}
        self._mp = monkeypatch
        self.dispatcher = None
        self.executor = None
        self._loop_task: asyncio.Task | None = None

        s = wconfig.worker_settings
        for name, value in {
            "WEB_BASE_URL": server.url, "TASK_SERVER_URL": "", "INTERNAL_TOKEN": server.token,
            "WORK_DIR": str(self.work_dir), "WORKER_ID": "test-host-e2e",
            "STORAGE_URL": server.url, "STORAGE_INTERNAL_URL": "",
            "STORAGE_INTERNAL_TOKEN": "", "LEASE_HOLD_SECONDS": 1.0,
        }.items():
            monkeypatch.setattr(s, name, value)
        # 心跳节拍压短：默认 5,7,11… 秒，测试里取消/自熔不能等那么久
        monkeypatch.setattr(hb_mod, "_HEARTBEAT_INTERVALS", (0.15,))
        monkeypatch.setattr(lease_mod, "_ACK_FAIL_COOLDOWN_SEC", 0.05)
        monkeypatch.setattr(lease_mod, "_NO_SLOT_SLEEP_SEC", 0.05)

    def channel(self, name: str, body: str, **extra) -> Path:
        """注册一个 channel：脚本内容写进 scripts/<name>.py，用当前解释器直接执行。"""
        script = write_script(self.scripts / f"{name}.py", body)
        self.channels[name] = {"script": str(script), "env": "", "executor": sys.executable, **extra}
        return script

    async def start(self, lanes=None, plugin=None) -> None:
        from webx_worker.app import build_executor
        from webx_worker.lane_manager import LaneManager

        self.executor = build_executor(
            self.channels, "", plugin=plugin, script_root=self.scripts,
        )
        lm = LaneManager(lanes or [], channel_types=self.channels.keys())
        self.dispatcher = lease_mod.LeaseDispatcher(lm, self.executor)
        self._loop_task = asyncio.create_task(self.dispatcher.loop())

    async def stop(self) -> None:
        """与 app.async_main 的停机三步同序：捕获令牌 → 同步取消 → 归还。"""
        tokens = lease_mod.held_fencing_headers()
        lease_mod.cancel_running_tasks()
        await lease_mod.release_held_leases(tokens)
        if self.dispatcher is not None:
            self.dispatcher.stop()
        if self._loop_task is not None:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
        if self.executor is not None:
            await self.executor.heartbeat.shutdown_all()
        await callback_client.close()
        await lease_mod.lease_client.close()
        await http_file_client.close()

    async def wait_for(self, predicate, timeout: float = 20.0) -> bool:
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            if predicate():
                return True
            await asyncio.sleep(0.05)
        return predicate()

    def ok_json(self, task_id: str) -> dict | None:
        import json

        p = self.work_dir / task_id / "ok.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


@pytest.fixture
def harness(server, tmp_path, monkeypatch):
    return Harness(server, tmp_path, monkeypatch)
