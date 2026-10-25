"""真起一个存储服务进程，用验证脚本（走真实 HTTP）对它跑一遍。

进程内的测试客户端验不出部署层面的问题；这里启动的是 ``python -m webx_storage`` 本体，
覆盖入口参数翻译、Sanic CLI 启动、真实 socket 上的 Range / 分片 / 签名全链路，
同时也是验证脚本自己的回归测试。
"""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
TOKEN = "live-test-token"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server(tmp_path_factory):
    port = _free_port()
    storage = tmp_path_factory.mktemp("live_storage")
    env = {
        **os.environ,
        "STORAGE_INTERNAL_TOKEN": TOKEN,
        "STORAGE_SIGN_SECRET": "live-sign-secret",
        "WEBX_ROOT": str(storage),  # 项目根指向临时目录：不读仓库里的 .env
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "webx_storage", "--single-process", "--host", "127.0.0.1",
         "--port", str(port), "--storage-dir", str(storage)],
        cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                if httpx.get(url + "/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            proc.kill()
            pytest.fail("存储服务未能在 10 秒内启动:\n" + proc.stdout.read().decode(errors="replace")[-2000:])
        yield url, storage
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_verify_script_passes_against_a_real_server(live_server, capsys):
    from webx_storage.verify import run

    url, storage = live_server
    assert run(url, TOKEN) == 0, capsys.readouterr().out
    out = capsys.readouterr().out
    assert "失败 0 项" in out and "SKIP" not in out, "默认配置下 task_results 策略应当启用"
    leftovers = [p for p in storage.rglob("*") if p.is_file() and "__verify__" in p.parts]
    assert leftovers == [], "验证脚本必须清理自己写入的数据"


def test_verify_script_fails_loudly_with_a_wrong_token(live_server):
    from webx_storage.verify import run

    url, _ = live_server
    assert run(url, "wrong-token") == 1


def test_verify_script_reports_unreachable_service():
    from webx_storage.verify import run

    assert run(f"http://127.0.0.1:{_free_port()}", TOKEN, timeout=2) == 2
