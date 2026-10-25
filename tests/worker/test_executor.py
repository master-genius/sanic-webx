"""合并后的唯一一份 executor：子进程读取路径、进程组清理、插件接缝、调用顺序。

旧实现有 executor.py（py≥3.11）与 executor_o.py（py<3.11）两份，只因 Python 版本二选一，
历史上漂移过三次（令牌不注入子进程 / 日志 key 口径 / 心跳注册顺序）。现在只有一份，
原来「两份必须等价」的 parity 测试改成**单份的行为断言**：

  · 心跳注册早于输入下载；
  · 步骤日志上传早于 complete；
  · 两个「静默 return」都有告警；
  · 被打断的一轮不写已确认的终态 ok.json。
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time

import pytest

from webx_worker.executor import GenericTaskExecutor
from webx_worker.log_writer import ConcurrentLogWriter
from webx_worker.plugin import TaskKindPlugin

# 子进程：一行普通输出 → 200 次 \r 进度刷新 → 收尾行 → 中文行（8192 定长读可能切断多字节字符）
_CHILD = r"""
import sys
sys.stdout.write("starting 'system'\n"); sys.stdout.flush()
for i in range(200):
    sys.stdout.write("step %d, remaining wall clock time: %4d s\r" % (i * 100, 200 - i))
    sys.stdout.flush()
sys.stdout.write("\nWriting final coordinates.\n"); sys.stdout.flush()
sys.stdout.write("分子 'M01' 计算完成，耗时 12.3 秒\n"); sys.stdout.flush()
"""

_EXPECTED = [
    "starting 'system'",
    "step 19900, remaining wall clock time:    1 s",
    "Writing final coordinates.",
    "分子 'M01' 计算完成，耗时 12.3 秒",
]


class _StubCallback:
    """run_subprocess 只读这几个字段用于注入子进程环境变量。"""

    internal_token = "t"
    base_url = "http://localhost"
    storage_url = "http://localhost"
    storage_internal_url = "http://localhost"


def _make(tmp_path, writer=None, **kw):
    return GenericTaskExecutor(
        callback=kw.pop("callback", _StubCallback()),
        log_writer=writer or ConcurrentLogWriter(),
        heartbeat=kw.pop("heartbeat", None), http_client=kw.pop("http_client", None),
        work_base_dir=str(tmp_path), channels_config=kw.pop("channels_config", {}),
        conda_root="", **kw,
    )


async def test_drain_stdout_smoke_cr_progress_collapses_to_one_line(tmp_path):
    """真实子进程 + 真实 ConcurrentLogWriter：\\r 进度刷新在截断缓冲里只留一行，中文不乱码。"""
    writer = ConcurrentLogWriter()
    work_dir = tmp_path / "n"
    work_dir.mkdir()
    ex = _make(tmp_path, writer)
    rc = await ex.run_subprocess(
        [sys.executable, "-c", _CHILD], "t_n", work_dir, log_filename="run.log", timeout=60,
    )
    assert rc == 0
    lines = (writer.get_truncated_content("t_n") or "").split("\n")
    assert lines == _EXPECTED, lines
    # 本地文件不做覆盖，逐条追加活动行快照 → 至少包含首尾真实行；不钉死条数（取决于 read(8192) 次数）
    local = (work_dir / "run.log").read_text(encoding="utf-8").splitlines()
    assert local[0] == _EXPECTED[0] and local[-1] == _EXPECTED[-1]
    assert len(local) >= len(_EXPECTED)
    await writer.close_task("t_n")


def _pid_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    # 存在但可能是僵尸：读 /proc 判断
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "Z"
    except OSError:
        return True


async def _spawn_sleeper_tree(tmp_path):
    """子进程再起一个孙子进程（同进程组）；返回写了两者 pid 的文件路径与命令。"""
    pidfile = tmp_path / "pids.json"
    code = (
        "import subprocess, sys, json, time\n"
        "g = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        f"open({str(pidfile)!r}, 'w').write(json.dumps([__import__('os').getpid(), g.pid]))\n"
        "time.sleep(60)\n"
    )
    return pidfile, [sys.executable, "-c", code]


@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 进程组")
async def test_double_cancel_still_kills_the_whole_process_group(tmp_path):
    """★ worker-cancel-task-fix 方案 A：外层 task 被**连续取消两次**，子进程组也必须已死。

    旧 finally 先 `await gather(...)` 再 `proc.kill()`：二次取消会让那个 await 再抛一次
    CancelledError，把后面的 kill 整段跳过，占着 GPU 的子进程成为孤儿。
    现在 finally 里先**同步** killpg，再做任何 await。
    孙子进程（同进程组）同样必须被带走——这也是用 killpg 而非 proc.kill() 的理由。
    """
    ex = _make(tmp_path)
    work_dir = tmp_path / "w"
    work_dir.mkdir()
    pidfile, cmd = await _spawn_sleeper_tree(tmp_path)

    t = asyncio.create_task(ex.run_subprocess(cmd, "t1", work_dir, timeout=60))
    for _ in range(100):
        if pidfile.exists() and pidfile.read_text():
            break
        await asyncio.sleep(0.05)
    child, grandchild = json.loads(pidfile.read_text())

    t.cancel()
    await asyncio.sleep(0)       # 让第一次取消落进 asyncio.wait
    t.cancel()                   # 第二次取消：打在 finally 的 await 上
    with pytest.raises(asyncio.CancelledError):
        await t

    assert await _wait_gone(child) and await _wait_gone(grandchild), "子进程组泄漏"


async def _wait_gone(pid: int, timeout: float = 5.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if _pid_gone(pid):
            return True
        await asyncio.sleep(0.05)
    return _pid_gone(pid)


@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 进程组")
async def test_timeout_kills_process_group_and_raises(tmp_path):
    ex = _make(tmp_path)
    work_dir = tmp_path / "w"
    work_dir.mkdir()
    pidfile, cmd = await _spawn_sleeper_tree(tmp_path)
    with pytest.raises(RuntimeError, match="计算超时"):
        await ex.run_subprocess(cmd, "t1", work_dir, timeout=1.0)
    child, grandchild = json.loads(pidfile.read_text())
    assert await _wait_gone(child) and await _wait_gone(grandchild)


@pytest.mark.skipif(sys.platform == "win32", reason="依赖 POSIX 进程组")
async def test_cancel_event_kills_silent_child_and_returns_minus_one(tmp_path):
    """取消走 Event，与子进程是否在输出无关：一个完全静默的子进程也要立刻被杀。"""
    cancel = asyncio.Event()

    class _HB:
        def get_cancel_event(self, tid):
            return cancel

        def get_server_status(self, tid):
            return "cancelled"

    ex = _make(tmp_path)
    work_dir = tmp_path / "w"
    work_dir.mkdir()
    t = asyncio.create_task(ex.run_subprocess(
        [sys.executable, "-c", "import time; time.sleep(60)"], "t1", work_dir,
        timeout=60, heartbeat=_HB(),
    ))
    await asyncio.sleep(0.3)
    t0 = time.monotonic()
    cancel.set()
    assert await asyncio.wait_for(t, 10) == -1
    assert time.monotonic() - t0 < 5


async def test_child_is_a_session_leader(tmp_path):
    """子进程在新会话里（start_new_session=True，替代 preexec_fn=os.setsid）。"""
    ex = _make(tmp_path)
    work_dir = tmp_path / "w"
    work_dir.mkdir()
    out = tmp_path / "ids.txt"
    rc = await ex.run_subprocess(
        [sys.executable, "-c",
         f"import os; open({str(out)!r}, 'w').write(f'{{os.getpid()}} {{os.getsid(0)}} {{os.getpgid(0)}}')"],
        "t1", work_dir, timeout=30,
    )
    assert rc == 0
    pid, sid, pgid = map(int, out.read_text().split())
    assert pid == sid == pgid


# ───────────────────────── 单份 executor 的顺序/告警不变量 ─────────────────────────

class _OrderRecorder:
    def __init__(self):
        self.events: list[str] = []


async def test_heartbeat_registers_before_download_and_log_uploads_before_complete(tmp_path):
    """行为断言（取代旧 parity 测试里的源码 index 断言）：

      · 心跳必须在下载**之前**注册：认领后的预算有限，下载期不续租会被回收给别人；
      · 先传步骤日志、后报 complete：反过来会撞 fencing 的 already_final 分支。
    """
    rec = _OrderRecorder()

    class _CB(_StubCallback):
        async def get_task_status(self, tid):
            from webx_worker.callback import ServerResult
            return ServerResult(data={"status": "running"})

        async def upload_result_file(self, tid, name, content, timeout=60.0):
            rec.events.append("upload_log")
            return "/public/x"

        async def update_task_fields(self, *a, **k):
            rec.events.append("fields")
            return True

        async def complete_task(self, tid):
            rec.events.append("complete")
            return True

    class _HB:
        async def register(self, tid, on_beat=None):
            rec.events.append("hb_register")

        async def unregister(self, tid):
            pass

        def get_cancel_event(self, tid):
            return None

        def get_server_status(self, tid):
            return "running"

    class _Files:
        async def download_file(self, tid, name, path):
            rec.events.append("download")
            open(path, "wb").write(b"x")
            return True

    script = tmp_path / "s.py"
    script.write_text("print('hello')")
    ex = _make(
        tmp_path, callback=_CB(), heartbeat=_HB(), http_client=_Files(),
        channels_config={"demo": {"script": str(script), "env": "", "executor": sys.executable}},
        script_root=tmp_path,
    )
    await ex.execute("tt1", {"task_type": "demo", "files_json": json.dumps([{"name": "a.txt"}])})

    ev = rec.events
    assert ev.index("hb_register") < ev.index("download"), ev
    assert ev.index("upload_log") < ev.index("complete"), ev
    assert ev.count("hb_register") == 1


async def test_silent_returns_in_log_upload_are_diagnosed(tmp_path, caplog):
    """两个静默 return 都要有告警——它们是「不知道是没传还是失败」的来源。"""
    class _CB(_StubCallback):
        async def upload_result_file(self, *a, **k):
            return None

    ex = _make(tmp_path, callback=_CB())
    # 1) 内容为空
    with caplog.at_level(logging.WARNING):
        await ex._upload_step_log("t1", "log", "run.log")
    assert "步骤日志为空，未上传" in caplog.text
    # 2) 上传没有返回 URL
    await ex.log_writer.write("t2", str(tmp_path / "t2" / "run.log"), "x\n")
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        await ex._upload_step_log("t2", "log", "run.log")
    assert "步骤日志上传未返回 URL" in caplog.text


async def test_interrupted_run_writes_no_confirmed_terminal_marker(harness, server):
    """★ 被打断的一轮**不得**写 ok.json（尤其不能是 server_confirmed=True 的终态）。

    recovery 的第一句是 `if server_confirmed: return False`——写了就让重启扫描整个跳过该目录，
    既不归还租约（从几秒退化成等补偿器），也不做崩溃目录清理。
    这条分支只有「进程正在退出」能触达（同步取消在跑任务），这一轮什么都没确认。
    """
    harness.channel("hang", "import time; time.sleep(60)")
    tid = server.add_task("hang")
    await harness.start()
    assert await harness.wait_for(lambda: server.task(tid)["status"] == "running")
    await asyncio.sleep(0.3)
    await harness.stop()
    assert harness.ok_json(tid) is None, "被打断的一轮写了 ok.json"
    assert not server.calls_to("POST", rf"/internal/tasks/{tid}/fail$"), "停机不得上报失败"


async def test_user_cancel_writes_confirmed_cancelled_marker(harness, server):
    """用户取消（心跳得到 status=cancelled）与进程退出是两条分支：前者那一轮确实被服务端确认取消。"""
    harness.channel("hang", "import time; time.sleep(60)")
    tid = server.add_task("hang")
    await harness.start()
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "running")
        await asyncio.sleep(0.3)
        server.cancel_task(tid)
        assert await harness.wait_for(lambda: (harness.ok_json(tid) or {}).get("status") == "cancelled")
    finally:
        await harness.stop()
    ok = harness.ok_json(tid)
    assert ok["server_confirmed"] is True
    assert not server.calls_to("POST", r"/complete$") and not server.calls_to("POST", r"/fail$")


# ───────────────────────── 插件接缝 ─────────────────────────

async def test_plugin_log_target_and_input_filter_and_env(harness, server):
    """三个接缝：日志 key/文件名、输入文件筛选、环境变量注入——都由插件决定。"""
    class P(TaskKindPlugin):
        def log_target(self, task_type, sub_step, steps_done):
            return f"k{len(steps_done)}", f"k{len(steps_done)}.log"

        def select_inputs(self, task_type, files):
            return [f for f in files if f["name"].endswith(".keep")]

        def extend_env(self, task_type, task_id, env):
            return {"PLUGIN_FLAG": "on"}

    dump = harness.tmp / "o.txt"
    harness.channel("demo", f"""
        import os
        open({str(dump)!r}, "w").write(os.environ.get("PLUGIN_FLAG", "") + "|" +
                                       ",".join(sorted(os.listdir("in"))))
        print("some output so a step log exists")
    """)
    tid = server.add_task("demo", files={"a.keep": b"1", "b.skip": b"2"}, steps=["s1", "s2"],
                          sub_step="s3")
    await harness.start(plugin=P())
    try:
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
    finally:
        await harness.stop()
    assert dump.read_text() == "on|a.keep"
    assert (tid, "k2.log") in server.storage
    assert list(server.task(tid)["log_path"]) == ["k2"]


async def test_default_plugin_log_target_and_input_filter(tmp_path):
    p = TaskKindPlugin()
    assert p.log_target("t", "", []) == ("log", "run.log")
    assert p.log_target("t", "em", ["a"]) == ("em", "em.log")
    assert p.select_inputs("t", [{"name": "a"}, {"name": ""}, {"x": 1}, "junk"]) == [{"name": "a"}]
    assert p.soft_clean_policy("t") is None and p.extend_env("t", "1", {}) == {}


def test_executor_module_is_the_only_one():
    """合并后只有一份 executor。"""
    import importlib.util

    assert importlib.util.find_spec("webx_worker.executor") is not None
    assert importlib.util.find_spec("webx_worker.executor_o") is None


def test_signal_module_available():           # 仅防 import 被优化掉；Windows 无 killpg 时由 skipif 兜
    assert hasattr(signal, "SIGKILL") or sys.platform == "win32"
