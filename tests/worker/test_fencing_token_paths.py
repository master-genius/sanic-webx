"""fencing 令牌的来路，逐条钉死 —— 任何一条断了都表现为「静默 403」。

令牌 = (worker_id, lease_seq)，两半必须与任务行完全相等才放行。worker 侧一共只有
三个地方产出它：

  ① 父进程内存注册表 `fencing._leases`  → 共享客户端的请求钩子（覆盖所有出口）
  ② 子进程环境变量 WEBX_WORKER_ID/_LEASE_SEQ → executor 注入，sdk.result_client 读取
  ③ 落盘条目里的快照                    → PendingReporter 重放走 body 通道（见 test_pending_reporter）

本文件盯 ①②。②用**行为**验证（真起子进程、对着强制 fencing 的假服务端），不靠 grep 源码。
"""

import sys

from webx_worker import envnames as E
from webx_worker import fencing
from webx_worker.config import derive_worker_id, worker_settings


def test_env_and_header_tokens_agree(monkeypatch):
    """同一个任务，走 env（子进程）与走 header（父进程）拿到的令牌必须一致。"""
    monkeypatch.setattr(worker_settings, "WORKER_ID", "host-gpu0")
    fencing.register("t9", 4)
    try:
        env = fencing.env_for("t9")
        hdr = fencing.headers_for("t9")
        assert env[E.ENV_WORKER_ID] == hdr[fencing.HEADER_WORKER_ID] == "host-gpu0"
        assert env[E.ENV_LEASE_SEQ] == hdr[fencing.HEADER_LEASE_SEQ] == "4"
        assert env[E.ENV_TASK_ID] == "t9"
    finally:
        fencing.unregister("t9")


def test_no_lease_yields_nothing_on_both_paths():
    """未持有租约 → 两条路都返回空：不伪造令牌，让服务端明确拒绝而不是被悄悄放行。"""
    assert fencing.env_for("never-claimed") == {}
    assert fencing.headers_for("never-claimed") == {}


def test_env_names_are_all_webx_worker_prefixed():
    """环境变量统一前缀（独立打包、不与其它项目的变量撞名）。"""
    names = [v for k, v in vars(E).items() if k.startswith("ENV_")]
    assert names and all(n.startswith("WEBX_WORKER_") for n in names), names


def test_worker_id_is_stable_and_deterministic(monkeypatch):
    """worker_id = {前缀}-{配置名}，同输入恒同输出；recovery 靠它跨重启归还租约。"""
    monkeypatch.setattr(worker_settings, "WORKER_PREFIX", "hostA")
    monkeypatch.setattr(worker_settings, "WORKER_ID", "")
    first = derive_worker_id("gpu0")
    monkeypatch.setattr(worker_settings, "WORKER_ID", "")
    second = derive_worker_id("gpu0")
    assert first == second == "hostA-gpu0"
    # 不同配置名 → 不同实例 ID（一 worker 一 unit 的区分轴）
    monkeypatch.setattr(worker_settings, "WORKER_ID", "")
    assert derive_worker_id("gpu1") == "hostA-gpu1"


def test_explicit_worker_id_wins(monkeypatch):
    """显式设置是逃生舱：原样采用，不再派生（容器场景靠它）。"""
    monkeypatch.setattr(worker_settings, "WORKER_ID", "manual-id")
    assert derive_worker_id("gpu0") == "manual-id"


def test_worker_id_falls_back_to_hostname_and_is_sanitized(monkeypatch):
    monkeypatch.setattr(worker_settings, "WORKER_PREFIX", "")
    monkeypatch.setattr(worker_settings, "WORKER_ID", "")
    import socket

    monkeypatch.setattr(socket, "gethostname", lambda: "My Host.example.com")
    assert derive_worker_id("a/b") == "My-Host-a-b"


def test_worker_id_has_no_random_or_pid_source():
    """派生源里不得混入 PID / 随机数：那会让重启后的租约归还全部失效。"""
    import inspect

    from webx_worker import config

    body = inspect.getsource(config.derive_worker_id)
    for banned in ("getpid", "random", "uuid", "time.time"):
        assert banned not in body, f"derive_worker_id 里出现了不稳定来源: {banned}"


async def test_executor_injects_fencing_into_the_child_environment(harness, server):
    """★ 行为验证：子进程环境里确有令牌，且就是认领时拿到的那一个。

    漏注入的后果是子进程的所有上报被服务端 403，而父进程的心跳与状态上报一切正常——
    极难联想。这条用「子进程把自己看到的环境写进文件」直接核对，再由
    test_e2e_lifecycle 用「对着强制 fencing 的服务端真上报」从另一个角度钉死。
    """
    dump = harness.tmp / "child_env.json"
    harness.channel("demo", f"""
        import json, os
        keys = ["{E.ENV_WORKER_ID}", "{E.ENV_LEASE_SEQ}", "{E.ENV_TASK_ID}",
                "{E.ENV_INTERNAL_TOKEN}", "{E.ENV_WEB_BASE_URL}", "{E.ENV_WORK_DIR}"]
        open({str(dump)!r}, "w").write(json.dumps({{k: os.environ.get(k) for k in keys}}))
    """)
    tid = server.add_task("demo")
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
    finally:
        await harness.stop()
    import json

    seen = json.loads(dump.read_text())
    assert seen[E.ENV_WORKER_ID] == "test-host-e2e"
    assert seen[E.ENV_LEASE_SEQ] == "1"
    assert seen[E.ENV_TASK_ID] == tid
    assert seen[E.ENV_INTERNAL_TOKEN] == server.token
    assert seen[E.ENV_WEB_BASE_URL] == server.url


async def test_plugin_cannot_override_the_fencing_token(harness, server):
    """插件的 extend_env 在令牌之前注入——令牌最后写入，插件覆盖不了它。"""
    from webx_worker.plugin import TaskKindPlugin

    class Evil(TaskKindPlugin):
        def extend_env(self, task_type, task_id, env):
            return {E.ENV_LEASE_SEQ: "999", "EXTRA_FROM_PLUGIN": "yes"}

    dump = harness.tmp / "child_env2.json"
    harness.channel("demo", f"""
        import json, os
        open({str(dump)!r}, "w").write(json.dumps(
            [os.environ["{E.ENV_LEASE_SEQ}"], os.environ.get("EXTRA_FROM_PLUGIN")]))
    """)
    tid = server.add_task("demo")
    await harness.start(plugin=Evil())
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
    finally:
        await harness.stop()
    import json

    assert json.loads(dump.read_text()) == ["1", "yes"]


def test_child_python_path_lets_the_sdk_be_imported(tmp_path):
    """子进程可能由另一个解释器启动：executor 必须把包所在目录放进 PYTHONPATH。"""
    import os
    import subprocess

    from webx_worker._paths import with_package_on_pythonpath

    env = with_package_on_pythonpath({"PATH": os.environ.get("PATH", ""), "PYTHONPATH": "/x"})
    assert env["PYTHONPATH"].endswith(os.pathsep + "/x") or env["PYTHONPATH"].split(os.pathsep)[1] == "/x"
    out = subprocess.run(
        [sys.executable, "-c", "import webx_worker.envnames as e; print(e.ENV_WORKER_ID)"],
        cwd=tmp_path, env=env, capture_output=True, text=True,
    )
    assert out.stdout.strip() == E.ENV_WORKER_ID, out.stderr
