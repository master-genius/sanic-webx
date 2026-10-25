"""测试公用：把完整 worker 作为真实子进程拉起（`python -m webx_worker` 同一入口）。"""

import os
import subprocess
import sys
import textwrap

from webx_worker._paths import PACKAGE_PARENT

# 引导脚本：把心跳节拍压短（默认 5,7,11… 秒），然后进入真实的 app.main
BOOT = textwrap.dedent("""
    import sys
    import webx_worker.heartbeat as h
    h._HEARTBEAT_INTERVALS = (0.2,)
    from webx_worker.app import main
    main(sys.argv[1:])
""")


def launch_worker(tmp_path, server, channels_py: str, extra_env=None, config="e2e", prefix="sigbox"):
    """在 tmp_path/home 下放好 channel 配置并拉起 worker 子进程，返回 Popen。"""
    home = tmp_path / "home"
    (home / "channel_config").mkdir(parents=True, exist_ok=True)
    (home / "channel_config" / f"{config}.py").write_text(channels_py, encoding="utf-8")
    env = {
        **os.environ,
        "WEBX_WORKER_WEB_BASE_URL": server.url, "WEBX_WORKER_INTERNAL_TOKEN": server.token,
        "WEBX_WORKER_STORAGE_URL": server.url, "WEBX_WORKER_LEASE_HOLD_SECONDS": "1",
        "WEBX_WORKER_WORK_DIR": str(tmp_path / "work"), "WEBX_WORKER_PREFIX": prefix,
        "WEBX_WORKER_UPLOADER_ENABLED": "false",
        "PYTHONPATH": PACKAGE_PARENT,
        **(extra_env or {}),
    }
    return subprocess.Popen(
        [sys.executable, "-c", BOOT, "--home", str(home), "--config", config],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def channels_for(script_path, name="job") -> str:
    """生成只含一个 channel 的配置文件文本（不写 LANES —— 顺带端到端覆盖空 LANES 修复）。"""
    return textwrap.dedent(f"""
        import sys
        CHANNELS = {{{name!r}: {{"script": {str(script_path)!r}, "env": "",
                                 "executor": {sys.executable!r}}}}}
    """)
