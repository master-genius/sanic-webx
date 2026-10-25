"""`doctor`（对真实服务的部署自检）与 `selftest`（本机假服务端跑完整生命周期）。

doctor 的核心安全性质：**探测绝不会领取任务**——领取通道用的是空 slots 请求，
服务端按协议以 400 拒绝（可达且鉴权通过）；401 = 令牌错；404 = 路由没转发。
"""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from webx_worker import doctor
from webx_worker._paths import PACKAGE_PARENT

_EXAMPLES = Path(doctor.__file__).resolve().parent / "examples"


@pytest.fixture
def home(tmp_path, server, monkeypatch):
    """放好示例配置、环境变量指向假服务端的主目录。"""
    h = tmp_path / "home"
    (h / "channel_config").mkdir(parents=True)
    shutil.copy(_EXAMPLES / "channels.py", h / "channel_config" / "default.py")
    shutil.copy(_EXAMPLES / "demo_task.py", h / "channel_config" / "demo_task.py")
    for k, v in {
        "WEBX_WORKER_WEB_BASE_URL": server.url, "WEBX_WORKER_INTERNAL_TOKEN": server.token,
        "WEBX_WORKER_STORAGE_INTERNAL_URL": server.url, "WEBX_WORKER_WORK_DIR": str(tmp_path / "work"),
        "WEBX_WORKER_PREFIX": "doc",
    }.items():
        monkeypatch.setenv(k, v)
    for k in ("WEBX_WORKER_TASK_SERVER_URL", "WEBX_WORKER_STORAGE_URL", "WEBX_WORKER_STORAGE_INTERNAL_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    return h


def _doctor(home, *extra):
    return doctor.main(["--home", str(home), "--timeout", "3", *extra])


def test_healthy_setup_passes_and_never_claims_a_task(home, server, capsys):
    pending = server.add_task("demo")
    assert _doctor(home) == 0
    out = capsys.readouterr().out
    for must in ("领取通道", "回调通道", "存储服务", "channel 配置", "工作目录", "令牌", "Lane 0"):
        assert must in out, must
    assert "[失败]" not in out
    # 安全性质：空 slots 探测，任务仍是 pending
    assert server.task(pending)["status"] == "pending"
    probe = server.calls_to("POST", r"^/task/leases$")[0]
    assert probe["json"]["slots"] == [] and probe["json"]["worker_id"] == "doc-default"
    assert probe["status"] == 400, "服务端对空 slots 按协议回 400"


def test_wrong_token_is_reported_on_every_channel(home, server, monkeypatch, capsys):
    monkeypatch.setenv("WEBX_WORKER_INTERNAL_TOKEN", "wrong")
    assert _doctor(home) == 1
    out = capsys.readouterr().out
    assert out.count("401") >= 3 and "令牌错误" in out


def test_missing_lease_route_is_diagnosed_as_a_proxy_problem(home, server, capsys):
    server.inject("POST", r"^/task/leases$", 404, times=5, body={"description": "Not Found"})
    assert _doctor(home) == 1
    out = capsys.readouterr().out
    assert "404：该地址上没有 /task/leases" in out and "反代" in out


def test_unreachable_server_is_reported_not_raised(home, monkeypatch, capsys):
    monkeypatch.setenv("WEBX_WORKER_WEB_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("WEBX_WORKER_STORAGE_INTERNAL_URL", "http://127.0.0.1:1")
    assert _doctor(home) == 1
    assert "连接失败" in capsys.readouterr().out


def test_dedicated_task_server_url_is_used_for_the_lease_probe(home, server, monkeypatch, capsys):
    from webx_worker.fake_server import FakeTaskServer

    with FakeTaskServer(token=server.token) as claim_only:
        monkeypatch.setenv("WEBX_WORKER_TASK_SERVER_URL", claim_only.url)
        assert _doctor(home) == 0
    assert claim_only.calls_to("POST", r"^/task/leases$")
    assert not server.calls_to("POST", r"^/task/leases$"), "领取探测应打到专用的 TASK_SERVER_URL"


def test_storage_token_is_checked_separately(home, server, monkeypatch, capsys):
    server.storage_token = "storage-only"
    assert _doctor(home) == 1                                  # 回退主令牌 → 存储端 401
    out = capsys.readouterr().out
    assert "存储服务" in out and "WEBX_WORKER_STORAGE_INTERNAL_TOKEN" in out
    monkeypatch.setenv("WEBX_WORKER_STORAGE_INTERNAL_TOKEN", "storage-only")
    assert _doctor(home) == 0


def test_no_storage_configured_is_a_warning_not_a_failure(home, monkeypatch, capsys):
    monkeypatch.delenv("WEBX_WORKER_STORAGE_INTERNAL_URL")
    assert _doctor(home) == 0
    assert "[警告] 存储服务" in capsys.readouterr().out


def test_channel_script_and_interpreter_problems_are_failures(home, capsys):
    (home / "channel_config" / "bad.py").write_text(
        "CHANNELS = {'nos': {'script': 'nope.py'}, 'noi': {'script': 'demo_task.py',"
        " 'executor': '/no/such/python'}, 'cc': {'script': 'demo_task.py', 'env': 'conda'}}\n"
        "LANES = [['nos']]\n")
    shutil.copy(_EXAMPLES / "demo_task.py", home / "channel_config" / "demo_task.py")
    assert _doctor(home, "--config", "bad") == 1
    out = capsys.readouterr().out
    assert "脚本不存在" in out and "解释器不在 PATH 上: /no/such/python" in out
    assert "env_name" in out                                   # conda 缺 env_name
    assert "不在任何 Lane 里" in out                            # LANES 漏写 noi / cc


def test_missing_channel_config_and_missing_token_and_bad_workdir(tmp_path, home, monkeypatch, capsys):
    (home / "channel_config" / "default.py").unlink()
    monkeypatch.setenv("WEBX_WORKER_INTERNAL_TOKEN", "")
    blocker = tmp_path / "file-not-dir"
    blocker.write_text("x")
    monkeypatch.setenv("WEBX_WORKER_WORK_DIR", str(blocker / "sub"))
    assert _doctor(home) == 1
    out = capsys.readouterr().out
    assert "不存在" in out and "未配置" in out and "不可写" in out


def test_ext_env_must_exist(home, tmp_path, capsys):
    assert _doctor(home, "--ext-env", str(tmp_path / "nope_env")) == 1
    assert "--ext-env 指定的文件不存在" in capsys.readouterr().out


def test_doctor_subcommand_via_python_dash_m(home, server):
    import os

    env = {**os.environ, "PYTHONPATH": PACKAGE_PARENT}
    out = subprocess.run([sys.executable, "-m", "webx_worker", "doctor", "--home", str(home)],
                         capture_output=True, text=True, env=env, timeout=60)
    assert out.returncode == 0, out.stdout + out.stderr


# ─────────────────────────── selftest / examples ───────────────────────────

@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 信号")
def test_selftest_passes_end_to_end():
    import os

    env = {**os.environ, "PYTHONPATH": PACKAGE_PARENT}
    out = subprocess.run([sys.executable, "-m", "webx_worker", "selftest"],
                         capture_output=True, text=True, env=env, timeout=150)
    assert out.returncode == 0, out.stdout[-3000:] + out.stderr[-1000:]
    assert "15/15 项通过" in out.stdout and "[失败]" not in out.stdout


def test_examples_contain_no_business_and_load_cleanly():
    from webx_worker.channel_loader import load_channel_config

    cfg = load_channel_config(_EXAMPLES / "channels.py")
    assert list(cfg["CHANNELS"]) == ["demo"] and cfg["LANES"] == []
    assert (cfg["SCRIPT_ROOT"] / cfg["CHANNELS"]["demo"]["script"]).exists()
    text = (_EXAMPLES / "demo_task.py").read_text() + (_EXAMPLES / "channels.py").read_text()
    for w in ("gromacs", "docking", "molecule", "forcefield"):
        assert w not in text.lower()
