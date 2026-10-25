"""整栈端到端：真实 PostgreSQL + 五个**独立进程**（web / 领取服务 / 存储 / 补偿器 / worker）。

单测和协议替身都过了，不代表几个服务拼起来能跑：路径、鉴权头、令牌、响应形状、停机顺序、
租约回收这些“接缝”只有真实进程之间互相调用才暴露。这里把它们全部拉起来，走用户的视角：
提交任务 → worker 主动领取 → 子进程执行 → 产物上传存储 → 任务完成，
以及失败、取消、优雅停机、worker 被 kill -9 后由补偿器回收这几条不能出错的路径。

租约参数压得很短（常规租约 4s + grace 2s），心跳节拍与自熔余量由 worker 引导脚本同步压短，
否则默认的 100s+180s 要等几分钟。生产参数不受影响。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pytest

from tests.conftest import _free_port
from tests.pg_helpers import ensure_schema, fetch

REPO = Path(__file__).resolve().parent.parent
INTERNAL_TOKEN = "stack-e2e-internal-token"

# worker 引导：心跳节拍压到 0.5s、自熔余量压到 1s，与上面压短的租约配套
WORKER_BOOT = textwrap.dedent("""
    import sys
    import webx_worker.heartbeat as h
    h._HEARTBEAT_INTERVALS = (0.5,)
    h._SELF_FENCE_MARGIN_SEC = 1
    from webx_worker.app import main
    main(sys.argv[1:])
""")


def wait_until(fn, timeout=30.0, interval=0.2, what="条件"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last:
                return last
        except Exception as e:  # noqa: BLE001 —— 服务刚起、还没就绪时的连接错误
            last = e
        time.sleep(interval)
    raise AssertionError(f"等待超时（{timeout}s）：{what}；最后一次结果={last!r}")


class Proc:
    """带日志文件的子进程。失败时能直接看到各服务各自说了什么。"""

    def __init__(self, name, cmd, env, logdir: Path, cwd=REPO):
        self.name = name
        self.log = logdir / f"{name}.log"
        self._fh = open(self.log, "wb")
        self.p = subprocess.Popen(cmd, env=env, cwd=str(cwd), stdout=self._fh, stderr=subprocess.STDOUT,
                                  start_new_session=True)

    def tail(self, n=2500) -> str:
        try:
            return self.log.read_text(errors="replace")[-n:]
        except OSError:
            return ""

    def alive(self) -> bool:
        return self.p.poll() is None

    def stop(self, sig=signal.SIGTERM, timeout=20) -> int | None:
        if self.p.poll() is None:
            try:
                os.killpg(self.p.pid, sig) if sig == signal.SIGKILL else self.p.send_signal(sig)
            except ProcessLookupError:
                pass
            try:
                self.p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(self.p.pid, signal.SIGKILL)
                self.p.wait(timeout=10)
        self._fh.close()
        return self.p.returncode


class Stack:
    def __init__(self, dsn, tmp: Path):
        self.dsn, self.tmp = dsn, tmp
        u = urlparse(dsn)
        self.web_port, self.task_port, self.storage_port = _free_port(), _free_port(), _free_port()
        self.web = f"http://127.0.0.1:{self.web_port}"
        self.task = f"http://127.0.0.1:{self.task_port}"
        self.storage = f"http://127.0.0.1:{self.storage_port}"
        from webx.config import get_settings

        self.token_secret = get_settings().TOKEN_SECRET
        self.env = {
            **os.environ,
            "PYTHONPATH": str(REPO),
            "DATABASE_HOST": u.hostname, "DATABASE_PORT": str(u.port), "DATABASE_NAME": u.path.lstrip("/"),
            "DATABASE_USER": u.username or "postgres", "DATABASE_PASSWORD": u.password or "",
            "TOKEN_SECRET": self.token_secret, "INTERNAL_TOKEN": INTERNAL_TOKEN,
            # 签名密钥必须显式配置：回落到 TOKEN_SECRET 的默认值时签名器 fail-closed，storage verify 的签名一步会 503
            "STORAGE_SIGN_SECRET": "stack-e2e-sign-secret-0123456789",
            "STORAGE_INTERNAL_URL": self.storage, "TASK_SERVER_URL": self.task,
            "TASK_SERVER_HOST": "127.0.0.1", "TASK_SERVER_PORT": str(self.task_port),
            "SERVER_HOST": "127.0.0.1", "SERVER_PORT": str(self.web_port),
            "WEBX_PYTHON": sys.executable,
            "STORAGE_DIR": str(tmp / "web-storage"),
            # 短租约：常规 4s + grace 2s；认领的短租约 20s（足够 worker 起跑前的 ack）
            "LEASE_CLAIM_SECONDS": "20", "LEASE_SECONDS": "4", "LEASE_GRACE_MIN_SECONDS": "2",
            "LEASE_GRACE_ALPHA": "0.01",
            "TASK_LEASE_HOLD_SECONDS": "2", "TASK_LEASE_FALLBACK_SECONDS": "0.5",
            "RECONCILER_LEASE_INTERVAL": "1", "RECONCILER_ADMIT_INTERVAL": "2",
            "WEBX_LOG_DIR": str(tmp / "logs"), "WEB_LOG_DIR": str(tmp / "logs"),
        }
        self.procs: dict[str, Proc] = {}
        self.workers: list[Proc] = []
        self.http = httpx.Client(timeout=15)

    # ---- 起停 ----
    def start(self):
        """四个服务都经 ``server.sh <角色>`` 启动——上线用的就是这条入口，它自己也得被验证。
        web 不传 --host/--port，靠 server.sh 从配置（SERVER_HOST / SERVER_PORT）里解出来。"""
        sh = str(REPO / "server.sh")
        storage_env = {**self.env, "STORAGE_PORT": str(self.storage_port), "STORAGE_HOST": "127.0.0.1",
                       "STORAGE_DIR": str(self.tmp / "storage"), "STORAGE_INTERNAL_TOKEN": INTERNAL_TOKEN}
        self.procs["storage"] = Proc("storage", [sh, "storage", "--single-process"], storage_env, self.tmp)
        self.procs["task"] = Proc("task", [sh, "task"], self.env, self.tmp)
        self.procs["web"] = Proc("web", [sh, "web", "--workers", "1"], self.env, self.tmp)
        self.procs["reconciler"] = Proc("reconciler", [sh, "reconciler"], self.env, self.tmp)
        for name, url in (("storage", self.storage), ("task", self.task), ("web", self.web)):
            self._ready(name, url + "/health")

    def _ready(self, name, url):
        def ok():
            if not self.procs[name].alive():
                raise AssertionError(f"{name} 提前退出:\n{self.procs[name].tail()}")
            return self.http.get(url).status_code == 200

        try:
            wait_until(ok, timeout=60, what=f"{name} 就绪 {url}")
        except AssertionError:
            raise AssertionError(f"{name} 未就绪:\n{self.procs[name].tail()}") from None

    def start_worker(self, prefix="stackw") -> Proc:
        home = self.tmp / f"home-{prefix}-{len(self.workers)}"
        (home / "channel_config").mkdir(parents=True, exist_ok=True)
        src = REPO / "webx_worker" / "examples"
        (home / "channel_config" / "demo_task.py").write_text((src / "demo_task.py").read_text())
        (home / "channel_config" / "e2e.py").write_text((src / "channels.py").read_text())
        env = {
            **self.env,
            "WEBX_WORKER_WEB_BASE_URL": self.web, "WEBX_WORKER_INTERNAL_TOKEN": INTERNAL_TOKEN,
            "WEBX_WORKER_STORAGE_URL": self.storage, "WEBX_WORKER_TASK_SERVER_URL": self.task,
            "WEBX_WORKER_LEASE_HOLD_SECONDS": "2",
            "WEBX_WORKER_WORK_DIR": str(self.tmp / f"work-{prefix}-{len(self.workers)}"),
            "WEBX_WORKER_PREFIX": prefix, "WEBX_WORKER_UPLOADER_ENABLED": "false",
        }
        w = Proc(f"worker-{prefix}-{len(self.workers)}",
                 [sys.executable, "-c", WORKER_BOOT, "--home", str(home), "--config", "e2e"], env, self.tmp)
        self.workers.append(w)
        return w

    def stop_all(self):
        for w in self.workers:
            w.stop(timeout=15)
        for p in self.procs.values():
            p.stop(timeout=15)
        self.http.close()

    # ---- 用户视角的 API ----
    def user(self, uid="e2e-user"):
        from webx.lib.token import get_token

        tok = get_token().make_token({"user_id": uid, "username": uid, "role": "user"}, id_="user")
        return {"Authorization": f"Bearer {tok}"}

    def create(self, seconds=1, fail=False, **extra) -> str:
        r = self.http.post(f"{self.web}/api/tasks", headers=self.user(), json={
            "task_type": "demo", "name": "stack-e2e", "submit": True,
            "params_text": json.dumps({"seconds": seconds, "fail": fail}), **extra})
        assert r.status_code == 201, r.text
        return r.json()["data"]["id"]

    def get(self, tid) -> dict:
        r = self.http.get(f"{self.web}/api/tasks/{tid}", headers=self.user())
        assert r.status_code == 200, r.text
        return r.json()["data"]

    def row(self, tid, cols="status, sub_status, worker_id, lease_seq, requeues, lease_until, error_message"):
        return dict(fetch(self.dsn, f"SELECT {cols} FROM tasks WHERE id=$1", tid)[0])

    def wait_status(self, tid, *statuses, timeout=40):
        def ok():
            s = self.get(tid)["status"]
            return s if s in statuses else None

        try:
            return wait_until(ok, timeout=timeout, what=f"任务 {tid} 进入 {statuses}")
        except AssertionError as e:
            logs = "\n".join(f"--- {n} ---\n{p.tail(1500)}" for n, p in {**self.procs, **{w.name: w for w in self.workers}}.items())
            raise AssertionError(f"{e}\n当前: {self.row(tid)}\n{logs}") from None


@pytest.fixture(scope="module")
def stack(pg_dsn, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("stack")
    from tests.pg_helpers import reset_schema

    reset_schema(pg_dsn)
    ensure_schema(pg_dsn)
    s = Stack(pg_dsn, tmp)
    try:
        s.start()
        yield s
    finally:
        s.stop_all()


@pytest.fixture
def worker(stack):
    """函数级：每个用例结束都停掉自己起的 worker，免得抢后面用例的任务。"""
    started = []

    def make(prefix="stackw"):
        w = stack.start_worker(prefix)
        started.append(w)
        return w

    yield make
    for w in started:
        w.stop(timeout=15)


# ───────────────────────── 主路径 ─────────────────────────


def test_submit_claim_execute_deliver_complete(stack, worker):
    """★ 主路径：用户提交 → worker 长连接领取 → 子进程执行 → 产物传存储 → 任务 completed。"""
    worker()
    tid = stack.create(seconds=1)
    stack.wait_status(tid, "completed")

    task = stack.get(tid)
    result = task["result_path"]
    result = json.loads(result) if isinstance(result, str) else result
    url = result["outputs"]["report_url"]
    # 产物真的在存储服务里，且内容是这次任务写的
    dl = stack.http.get(url if url.startswith("http") else stack.storage + url)
    assert dl.status_code == 200 and dl.text == f"result of {tid}\n", (url, dl.status_code, dl.text[:200])

    row = stack.row(tid)
    assert row["status"] == "completed" and row["lease_until"] is None, row
    assert row["worker_id"].startswith("stackw"), f"worker_id 应保留最后执行者: {row}"
    assert row["requeues"] == 0, "正常路径不应出现被动回收"
    assert row["lease_seq"] >= 2, "提交进池 + 认领，至少 bump 两次"


def test_script_failure_marks_task_failed_with_reason(stack, worker):
    worker()
    tid = stack.create(seconds=0, fail=True)
    stack.wait_status(tid, "failed")
    row = stack.row(tid)
    assert "3" in (row["error_message"] or ""), f"失败原因应带脚本退出码: {row}"
    assert row["lease_until"] is None


def test_user_cancel_stops_the_child_and_frees_the_lane(stack, worker):
    """取消在跑的任务：状态变 cancelled，子进程被杀，Lane 空出来能领下一个任务（不用等 60s 睡完）。"""
    worker()
    long_id = stack.create(seconds=60)
    stack.wait_status(long_id, "running")
    r = stack.http.post(f"{stack.web}/api/tasks/{long_id}/cancel", headers=stack.user())
    assert r.status_code == 200, r.text
    stack.wait_status(long_id, "cancelled", timeout=10)

    t0 = time.time()
    quick = stack.create(seconds=0)
    stack.wait_status(quick, "completed", timeout=30)
    assert time.time() - t0 < 25, "Lane 没有被及时释放：取消后子进程应被终止"


def test_sigterm_returns_the_task_to_the_pool_and_another_worker_finishes_it(stack, worker):
    """★ 优雅停机：在跑的任务回池（不是 failed）；换一个 worker 能接着把它做完。"""
    w1 = worker("termw")
    tid = stack.create(seconds=6)
    stack.wait_status(tid, "running")
    time.sleep(1.0)                                    # 让子进程真正跑起来
    seq_before = stack.row(tid)["lease_seq"]

    assert w1.stop(signal.SIGTERM, timeout=20) == 0, w1.tail()
    row = stack.row(tid)
    assert row["status"] in ("pending", "queued"), f"优雅停机后任务应回池，实际 {row}\n{w1.tail()}"
    assert row["status"] != "failed" and not row["error_message"]
    assert row["lease_seq"] > seq_before, "回池必须 bump lease_seq，旧租户的在途回调才会被拒"

    worker("termw2")
    stack.wait_status(tid, "completed", timeout=45)


def test_killed_worker_is_reclaimed_by_reconciler_and_rerun(stack, worker):
    """★ worker 被 kill -9（没机会归还租约）：租约到期后补偿器回收，换个 worker 重跑；
    孤儿子进程之后的回调因 fencing 被拒，不会污染新一轮。"""
    w1 = worker("killw")
    tid = stack.create(seconds=5)
    stack.wait_status(tid, "running")
    time.sleep(1.0)
    seq_running = stack.row(tid)["lease_seq"]

    w1.p.kill()                                        # 只杀 worker，子进程成了孤儿
    w1.p.wait(timeout=10)

    # 租约 ≈ 4s + grace 2s，补偿器每秒点查：回收后任务回池且 lease_seq 变了
    def reclaimed():
        r = stack.row(tid)
        return r if r["lease_seq"] > seq_running and r["status"] in ("pending", "queued") else None

    r = wait_until(reclaimed, timeout=30, what="补偿器回收被杀 worker 的租约")
    assert r["requeues"] >= 1, f"RUNNING 被回收必须计入 requeues: {r}"

    worker("killw2")
    stack.wait_status(tid, "completed", timeout=45)
    final = stack.row(tid)
    assert not final["error_message"], final
    # 孤儿子进程仍会跑完并回调：带的是旧代次令牌，必须被 fencing 拒绝，任务终态不被它改写
    time.sleep(5)
    assert stack.row(tid)["status"] == "completed"


# ───────────────────────── 接缝与运维验证 ─────────────────────────


def test_internal_group_rejects_missing_and_wrong_token(stack):
    """真实进程里的 internal 组鉴权（令牌错 / 缺 → 401），且用户令牌不能当内部令牌用。"""
    url = f"{stack.web}/internal/tasks/nope"
    assert stack.http.get(url).status_code == 401
    assert stack.http.get(url, headers={"X-Internal-Token": "wrong"}).status_code == 401
    assert stack.http.get(url, headers=stack.user()).status_code == 401
    assert stack.http.get(url, headers={"X-Internal-Token": INTERNAL_TOKEN}).status_code == 404


def _run(args, env, timeout=60):
    return subprocess.run([sys.executable, *args], cwd=str(REPO), env=env, capture_output=True,
                          text=True, timeout=timeout)


def test_ops_verification_commands_pass_against_the_live_stack(stack, tmp_path):
    """文档里给运维的三条验证命令，对活栈必须全过（它们是上线后的体检手段，自己不能是坏的）。"""
    env = {**stack.env}
    r = _run(["-m", "task_server", "verify", "--url", stack.task, "--token", INTERNAL_TOKEN], env)
    assert r.returncode == 0, r.stdout + r.stderr
    r = _run(["-m", "webx_storage", "verify", "--url", stack.storage, "--token", INTERNAL_TOKEN], env)
    assert r.returncode == 0, r.stdout + r.stderr

    home = tmp_path / "doctor-home"
    (home / "channel_config").mkdir(parents=True)
    src = REPO / "webx_worker" / "examples"
    (home / "channel_config" / "demo_task.py").write_text((src / "demo_task.py").read_text())
    (home / "channel_config" / "e2e.py").write_text((src / "channels.py").read_text())
    wenv = {**env, "WEBX_WORKER_WEB_BASE_URL": stack.web, "WEBX_WORKER_INTERNAL_TOKEN": INTERNAL_TOKEN,
            "WEBX_WORKER_STORAGE_URL": stack.storage, "WEBX_WORKER_TASK_SERVER_URL": stack.task,
            "WEBX_WORKER_WORK_DIR": str(tmp_path / "doctor-work"), "WEBX_WORKER_PREFIX": "doctor"}
    r = _run(["-m", "webx_worker", "doctor", "--home", str(home), "--config", "e2e"], wenv)
    assert r.returncode == 0, r.stdout + r.stderr


def test_smoke_script_passes_against_the_live_stack(stack, worker, tmp_path):
    """上线冒烟脚本本身也要被验证：对活栈跑 tools/smoke.py，退出码 0 且打印“全链路通”。
    再用一个永远没有 worker 能跑的任务类型，确认它会以退出码 1 失败并给出“停在 pending”的排查提示。"""
    worker("smokew")
    tok = stack.user("smoke-user")["Authorization"].split()[1]
    r = _run(["tools/smoke.py", "--web", stack.web, "--token", tok, "--storage", stack.storage,
              "--timeout", "40"], stack.env, timeout=90)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "全链路通" in r.stdout and "产物可下载" in r.stdout, r.stdout

    r = _run(["tools/smoke.py", "--web", stack.web, "--token", tok, "--task-type", "no-such-worker-type",
              "--timeout", "4"], stack.env, timeout=60)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "pending" in r.stdout and "/task/leases" in r.stdout, r.stdout
