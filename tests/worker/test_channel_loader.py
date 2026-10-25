"""channel 配置加载：路径解析、模板、能力声明、插件、命令前缀。"""

import pytest

from webx_worker import channel_loader as CL
from webx_worker.plugin import TaskKindPlugin


def test_default_and_bare_name_resolve_under_home_not_package_dir(tmp_path):
    home = tmp_path / "h"
    assert CL.resolve_config_path(None, home) == home / "channel_config" / "default.py"
    assert CL.resolve_config_path("gpu0", home) == (home / "channel_config" / "gpu0.py").resolve()
    assert CL.resolve_config_path("gpu0.py", home) == (home / "channel_config" / "gpu0.py").resolve()
    p = tmp_path / "x" / "my.py"
    assert CL.resolve_config_path(str(p), home) == p.resolve()


def test_missing_config_writes_minimal_template_and_asks_to_restart(tmp_path, capsys):
    p = tmp_path / "channel_config" / "default.py"
    assert CL.ensure_config_exists(p) is False
    text = p.read_text(encoding="utf-8")
    assert "CHANNELS" in text and "LANES" in text and "PLUGIN" in text
    assert CL.ensure_config_exists(p) is True
    with pytest.raises(ValueError, match="CHANNELS"):
        CL.load_channel_config(p)            # 模板里 CHANNELS 为空：必须先填
    assert "重新启动" in capsys.readouterr().out


def test_template_has_no_business_content():
    t = CL._TEMPLATE.lower()
    for word in ("gromacs", "mds", "docking", "forcefield"):
        assert word not in t


def test_load_returns_declared_capabilities_lanes_plugin_and_script_root(tmp_path):
    cfg = tmp_path / "c.py"
    cfg.write_text(
        "from webx_worker.plugin import TaskKindPlugin\n"
        "class P(TaskKindPlugin):\n    pass\n"
        "CHANNELS = {'a': {'script': 'a.py'}, 'b': {'script': '/abs/b.py'}}\n"
        "LANES = [['a'], ['b']]\nPLUGIN = P()\nCONDA_ROOT = '/opt/c'\n"
    )
    got = CL.load_channel_config(cfg)
    assert list(got["CHANNELS"]) == ["a", "b"] and got["LANES"] == [["a"], ["b"]]
    assert isinstance(got["PLUGIN"], TaskKindPlugin) and got["CONDA_ROOT"] == "/opt/c"
    assert got["SCRIPT_ROOT"] == tmp_path.resolve()


def test_defaults_when_optional_names_are_absent(tmp_path):
    cfg = tmp_path / "c.py"
    cfg.write_text("CHANNELS = {'a': {'script': 'a.py'}}\n")
    got = CL.load_channel_config(cfg)
    assert got["LANES"] == [] and type(got["PLUGIN"]) is TaskKindPlugin


def test_plugin_must_be_a_plugin_instance(tmp_path):
    cfg = tmp_path / "c.py"
    cfg.write_text("CHANNELS = {'a': {'script': 'a.py'}}\nPLUGIN = object()\n")
    with pytest.raises(TypeError):
        CL.load_channel_config(cfg)


def test_cmd_prefix_variants():
    assert CL.build_cmd_prefix({"env": "conda", "env_name": "e"}, "") == \
        ["conda", "run", "--no-capture-output", "-n", "e"]
    assert CL.build_cmd_prefix({"env": "uv"}, "") == ["uv", "run"]
    assert CL.build_cmd_prefix({"env": "uv", "env_name": "/p/py"}, "")[:3] == ["uv", "run", "--python"]
    assert CL.build_cmd_prefix({"executor": "/usr/bin/python3"}, "") == ["/usr/bin/python3"]
    assert CL.build_cmd_prefix({}, "") == ["python"]
    with pytest.raises(ValueError):
        CL.build_cmd_prefix({"env": "conda"}, "")


def test_relative_script_resolves_against_script_root_and_missing_is_loud(tmp_path):
    from webx_worker.executor import GenericTaskExecutor
    from webx_worker.log_writer import ConcurrentLogWriter

    (tmp_path / "ok.py").write_text("print(1)")
    ex = GenericTaskExecutor(
        callback=None, log_writer=ConcurrentLogWriter(), heartbeat=None, http_client=None,
        work_base_dir=str(tmp_path / "w"), conda_root="", script_root=tmp_path,
        channels_config={"a": {"script": "ok.py", "executor": "py"},
                         "b": {"script": "nope.py"}, "c": {"script": "run.sh"}},
    )
    assert ex._build_cmd("a", "t1") == ["py", str(tmp_path / "ok.py"), "--task-id", "t1"]
    with pytest.raises(FileNotFoundError):
        ex._build_cmd("b", "t1")
    with pytest.raises(RuntimeError, match="未配置任务类型"):
        ex._build_cmd("zzz", "t1")
    (tmp_path / "run.sh").write_text("echo hi")
    assert ex._build_cmd("c")[0] == "bash"
