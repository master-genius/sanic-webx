"""systemd 单元生成器（`python -m webx_worker service one|multi`）。

只验生成物的内容与不变量（本测试不碰 /etc/systemd，也不调用 systemctl）。
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from webx_worker import service as S
from webx_worker._paths import PACKAGE_PARENT


def _run(*args, home: Path):
    env = {**os.environ, "PYTHONPATH": PACKAGE_PARENT}
    return subprocess.run(
        [sys.executable, "-m", "webx_worker", "service", *args, "--home", str(home)],
        capture_output=True, text=True, env=env, timeout=60,
    )


def test_one_generates_a_direct_exec_unit(tmp_path):
    out = _run("one", "--unit-name", "demo-worker", "--user", "svc", home=tmp_path)
    assert out.returncode == 0, out.stderr
    unit = (tmp_path / "deploy" / "demo-worker.service").read_text()
    assert f"WorkingDirectory={tmp_path}" in unit
    assert f"Environment=WEBX_WORKER_HOME={tmp_path}" in unit
    assert f"ExecStart={sys.executable} -m webx_worker run $WORKER_ARGS" in unit
    assert "User=svc" in unit and "Group=svc" in unit
    assert "TimeoutStopSec=30" in unit and "PrivateTmp=true" in unit
    assert "SyslogIdentifier=demo-worker" in unit
    assert not re.search(r"__[A-Z_]+__", unit), "模板占位符没替换干净"
    assert not list((tmp_path / "deploy").glob("*.sh")), "未使用 conda 就不该生成启动脚本"


def test_multi_unit_uses_instance_name_as_config_and_groups_under_a_target(tmp_path):
    out = _run("multi", "--unit-name", "demo-worker", "--instances", "gpu0,gpu1", home=tmp_path)
    assert out.returncode == 0, out.stderr
    d = tmp_path / "deploy"
    svc = (d / "demo-worker@.service").read_text()
    tgt = (d / "demo-worker.target").read_text()
    # 一名三用：%i == --config == 覆盖层名；unit 不承载业务配置
    assert 'Environment="WORKER_ARGS=--config %i"' in svc
    assert "PartOf=demo-worker.target" in svc and "WantedBy=demo-worker.target" in svc
    assert "SyslogIdentifier=demo-worker-%i" in svc
    assert "WantedBy=multi-user.target" in tgt
    assert "--debug" not in svc and 'WORKER_ARGS=--config %i"' in svc, "业务参数不该写进 unit"
    # 每个实例一份覆盖层样例，工作目录互不相同
    ov0 = (tmp_path / "channel_config" / "gpu0_env").read_text()
    ov1 = (tmp_path / "channel_config" / "gpu1_env").read_text()
    w0 = re.search(r"^WEBX_WORKER_WORK_DIR=(.+)$", ov0, re.M).group(1)
    w1 = re.search(r"^WEBX_WORKER_WORK_DIR=(.+)$", ov1, re.M).group(1)
    assert w0 != w1 and Path(w0).is_absolute()


def test_multi_never_overwrites_an_existing_overlay(tmp_path):
    (tmp_path / "channel_config").mkdir()
    keep = tmp_path / "channel_config" / "gpu0_env"
    keep.write_text("# hand edited\n")
    out = _run("multi", "--instances", "gpu0", home=tmp_path)
    assert keep.read_text() == "# hand edited\n" and "已存在，跳过" in out.stdout


@pytest.mark.parametrize("bad", ["a@b", "a/b", "a b", "", ".service"])
def test_unit_name_validation_rejects_dangerous_values(tmp_path, bad):
    out = _run("one", "--unit-name", bad, home=tmp_path)
    assert out.returncode == 2, (bad, out.stdout, out.stderr)


def test_unit_suffix_is_stripped_not_duplicated(tmp_path):
    out = _run("one", "--unit-name", "x.service", home=tmp_path)
    assert out.returncode == 0 and (tmp_path / "deploy" / "x.service").exists()
    assert "已剥去后缀" in out.stdout


def test_stale_units_of_another_basename_are_called_out(tmp_path):
    d = tmp_path / "deploy"
    d.mkdir()
    (d / "old-name@.service").write_text("x")
    out = _run("multi", "--unit-name", "new-name", home=tmp_path)
    assert "old-name@.service" in out.stdout and "systemctl disable --now old-name" in out.stdout


def test_launch_script_for_conda_bakes_in_root_and_forwards_args(tmp_path):
    fake_conda = tmp_path / "conda"
    (fake_conda / "etc" / "profile.d").mkdir(parents=True)
    (fake_conda / "etc" / "profile.d" / "conda.sh").write_text("")
    out = _run("multi", "--unit-name", "cw", "--conda-env", "myenv", "--conda-root", str(fake_conda),
               "--env", "/opt/dev.env", home=tmp_path)
    assert out.returncode == 0, out.stderr
    script = tmp_path / "deploy" / "cw-launch.sh"
    sh = script.read_text()
    assert f'CONDA_ROOT="${{CONDA_ROOT:-{fake_conda}}}"' in sh
    assert f'WORKER_HOME="{tmp_path}"' in sh
    assert "exec python -m webx_worker" in sh and 'source "/opt/dev.env"' in sh
    assert script.stat().st_mode & 0o111
    svc = (tmp_path / "deploy" / "cw@.service").read_text()
    assert "Environment=CONDA_ENV=myenv" in svc
    assert f"ExecStart={script} --conda-env ${{CONDA_ENV}} $WORKER_ARGS" in svc
    assert subprocess.run(["bash", "-n", str(script)]).returncode == 0     # 脚本语法可被 bash 解析


def test_conda_root_resolution_fails_fast_when_nothing_found(monkeypatch, tmp_path):
    for k in ("CONDA_EXE", "CONDA_PREFIX"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(SystemExit) as e:
        S.resolve_conda_root(None)
    assert "--conda-root" in str(e.value)


def test_pythonpath_only_added_when_package_is_not_installed(monkeypatch):
    monkeypatch.setattr(S, "PACKAGE_PARENT", "/usr/lib/python3/site-packages")
    assert S._needs_pythonpath() is False
    monkeypatch.setattr(S, "PACKAGE_PARENT", "/home/me/repo")
    assert S._needs_pythonpath() is True
