"""领取服务（task_server）：真起进程，走真实 HTTP + 真实 PostgreSQL。

长连接领取的核心行为——挂起、被 poke 唤醒、兜底轮询、广播唤醒下的互斥——只有真实进程能测准：
进程内的测试客户端每个请求都会起停一次应用，根本没有"挂着的请求"这回事。
"""

import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
TOKEN = "ts-token"
H = {"X-Internal-Token": TOKEN, "X-Worker-Id": "w1"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


from tests.pg_helpers import fetch, orm as _orm  # noqa: E402


@pytest.fixture(scope="module")
def schema(pg_dsn):
    """建好表与认领索引（只做一次）。"""
    from tests.pg_helpers import ensure_schema

    ensure_schema(pg_dsn)
    return pg_dsn


def seed(dsn, n=1, task_type="demo", **kw):
    from webx.models import Task, TaskStatus

    async def go():
        return [
            (await Task.create(task_type=task_type, status=TaskStatus.PENDING, user_id="u", **kw)).id
            for _ in range(n)
        ]

    return _orm(dsn, go)


def clear(dsn):
    from tests.pg_helpers import execute

    execute(dsn, "TRUNCATE tasks")


def start_server(dsn, *, hold, fallback, db_port=None):
    u = urlparse(dsn)
    port = _free_port()
    env = {
        **os.environ,
        "WEBX_ROOT": str(ROOT / "tests"),  # 项目根指向没有 .env 的目录
        "DATABASE_HOST": u.hostname, "DATABASE_PORT": str(db_port or u.port),
        "DATABASE_USER": u.username or "postgres", "DATABASE_PASSWORD": u.password or "",
        "DATABASE_NAME": u.path.lstrip("/"),
        "INTERNAL_TOKEN": TOKEN,
        "TASK_LEASE_HOLD_SECONDS": str(hold), "TASK_LEASE_FALLBACK_SECONDS": str(fallback),
        "TASK_SERVER_PORT": str(port), "TASK_SERVER_HOST": "127.0.0.1",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "task_server", "--port", str(port), "--host", "127.0.0.1"],
        cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    url = f"http://127.0.0.1:{port}"
    for _ in range(150):
        try:
            if httpx.get(url + "/health", timeout=1).status_code == 200:
                return proc, url
        except httpx.HTTPError:
            if proc.poll() is not None:
                break
            time.sleep(0.1)
    proc.kill()
    pytest.fail("task_server 未能启动:\n" + proc.stdout.read().decode(errors="replace")[-2000:])


def stop_server(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="module")
def poke_only(schema):
    """兜底轮询间隔拉到 60 秒：只有 poke 能在挂起期间唤醒请求。"""
    proc, url = start_server(schema, hold=6, fallback=60)
    yield url, schema
    stop_server(proc)


@pytest.fixture(scope="module")
def poll_only(schema):
    """兜底轮询 0.2 秒：不 poke 也能发现挂起期间新来的任务。"""
    proc, url = start_server(schema, hold=4, fallback=0.2)
    yield url, schema
    stop_server(proc)


@pytest.fixture(autouse=True)
def clean(schema):
    clear(schema)


def lease(url, slots=(("demo",),), headers=None, timeout=15, **extra):
    return httpx.post(url + "/task/leases", json={"slots": [list(s) for s in slots], **extra},
                      headers=H if headers is None else headers, timeout=timeout)


def in_thread(fn):
    box = {}

    def run():
        t0 = time.monotonic()
        box["resp"] = fn()
        box["elapsed"] = time.monotonic() - t0

    th = threading.Thread(target=run)
    th.start()
    return th, box


# ==================== 校验 ====================


def test_authentication_and_request_validation(poke_only):
    url, _ = poke_only
    assert lease(url, headers={"X-Worker-Id": "w1"}).status_code == 401
    assert lease(url, headers={"X-Internal-Token": "wrong", "X-Worker-Id": "w1"}).status_code == 401
    assert lease(url, headers={"X-Internal-Token": TOKEN}).status_code == 400, "缺 worker_id"
    for bad in ([], [[]], [[""]], "x", [["a"]] * 17, [["a"] * 33], [[1]]):
        r = httpx.post(url + "/task/leases", json={"slots": bad}, headers=H)
        assert r.status_code == 400 and r.json() == {"error": "invalid slots"}, bad
    for key, val in (("max_level", -1), ("max_level", "x"), ("max_compute_points", -5)):
        r = httpx.post(url + "/task/leases", json={"slots": [["demo"]], key: val}, headers=H)
        assert r.status_code == 400 and key in r.json()["error"], (key, val)


def test_worker_id_may_come_from_the_body_like_the_fencing_token(poke_only):
    url, dsn = poke_only
    seed(dsn)
    r = httpx.post(url + "/task/leases", json={"slots": [["demo"]], "worker_id": "from-body"},
                   headers={"X-Internal-Token": TOKEN})
    assert r.status_code == 200 and r.json()["tasks"][0]["worker_id"] == "from-body"


def test_poke_endpoint_is_unauthenticated_and_counted(poke_only):
    url, _ = poke_only
    before = httpx.get(url + "/health").json()["pokes"]
    assert httpx.post(url + "/task/pokes", json={}).status_code == 200
    h = httpx.get(url + "/health").json()
    assert h["ok"] and h["service"] == "task_server" and h["pokes"] == before + 1


# ==================== 领取 ====================


def test_a_pending_task_is_claimed_immediately_without_hanging(poke_only):
    url, dsn = poke_only
    (tid,) = seed(dsn, params_text='{"k": 1}')
    t0 = time.monotonic()
    r = lease(url, max_level=0)
    assert time.monotonic() - t0 < 2
    body = r.json()
    assert r.status_code == 200 and body["count"] == 1
    task = body["tasks"][0]
    assert task["id"] == tid and task["status"] == "queued" and task["lease_seq"] == 1
    assert task["worker_id"] == "w1" and task["params_text"] == '{"k": 1}'
    assert "user_id" not in task
    row = fetch(dsn, "SELECT status, worker_id, lease_seq FROM tasks WHERE id=$1", tid)[0]
    assert (row["status"], row["worker_id"], row["lease_seq"]) == ("queued", "w1", 1)


def test_empty_pool_hangs_for_the_hold_time_then_returns_an_empty_200(schema):
    proc, url = start_server(schema, hold=1.5, fallback=60)
    try:
        t0 = time.monotonic()
        r = lease(url)
        elapsed = time.monotonic() - t0
        assert r.status_code == 200 and r.json() == {"tasks": [], "count": 0}, "空返回也是 200：挂起期间没认领任何东西"
        assert 1.2 <= elapsed < 4, f"应挂满 hold 时间，实际 {elapsed:.2f}s"
    finally:
        stop_server(proc)


def test_a_poke_wakes_a_hanging_request_immediately(poke_only):
    """兜底轮询是 60 秒——请求能在 ~1 秒内拿到任务，只可能是 poke 唤醒的。"""
    url, dsn = poke_only
    th, box = in_thread(lambda: lease(url, timeout=20))
    time.sleep(0.8)  # 让请求先挂起
    (tid,) = seed(dsn)
    httpx.post(url + "/task/pokes", json={})
    th.join(15)
    assert box["resp"].json()["tasks"][0]["id"] == tid
    assert box["elapsed"] < 3.5, f"被 poke 唤醒应近乎立刻，实际 {box['elapsed']:.2f}s"


def test_without_a_poke_the_fallback_poll_still_finds_the_task(poll_only):
    url, dsn = poll_only
    th, box = in_thread(lambda: lease(url, timeout=20))
    time.sleep(0.8)
    (tid,) = seed(dsn)  # 故意不 poke：poke 丢了只损失延迟，不损失正确性
    th.join(15)
    assert box["resp"].json()["tasks"][0]["id"] == tid
    assert box["elapsed"] < 3.5


def test_a_broadcast_wakeup_still_hands_the_single_task_to_exactly_one_waiter(poke_only):
    """poke 是广播：所有挂起的请求都会醒来去抢；互斥由数据库保证，输的那个继续等。"""
    url, dsn = poke_only
    workers = [in_thread(lambda w=w: httpx.post(
        url + "/task/leases", json={"slots": [["demo"]]},
        headers={"X-Internal-Token": TOKEN, "X-Worker-Id": f"w{w}"}, timeout=20)) for w in range(4)]
    time.sleep(0.8)
    (tid,) = seed(dsn)
    httpx.post(url + "/task/pokes", json={})
    for th, _ in workers:
        th.join(20)
    winners = [b["resp"].json()["tasks"] for _, b in workers if b["resp"].json()["count"]]
    assert len(winners) == 1 and winners[0][0]["id"] == tid, "恰好一个等待者拿到"
    assert sum(b["resp"].json()["count"] for _, b in workers) == 1
    assert fetch(dsn, "SELECT lease_seq FROM tasks WHERE id=$1", tid)[0]["lease_seq"] == 1


def test_database_outage_returns_503_so_workers_back_off(schema):
    proc, url = start_server(schema, hold=1, fallback=60, db_port=_free_port())  # 指向一个没人监听的库端口
    try:
        assert httpx.get(url + "/health").status_code == 200, "DB 不可达不中止启动：分发服务要扛住短暂抖动"
        r = lease(url, timeout=30)
        assert r.status_code == 503 and r.json() == {"error": "claim failed"}
    finally:
        stop_server(proc)


# ==================== 验证脚本 ====================


def test_verify_script_passes_against_a_real_server_and_claims_nothing(schema):
    from task_server.verify import run

    proc, url = start_server(schema, hold=1, fallback=60)
    try:
        (tid,) = seed(schema)  # 池里有一个真实任务——验证脚本绝不能把它领走
        assert run(url, TOKEN, timeout=20) == 0
        row = fetch(schema, "SELECT status, worker_id FROM tasks WHERE id=$1", tid)[0]
        assert (row["status"], row["worker_id"]) == ("pending", ""), "验证脚本必须不改数据"
    finally:
        stop_server(proc)


def test_verify_script_fails_with_a_wrong_token_and_reports_unreachable(schema):
    from task_server.verify import run

    proc, url = start_server(schema, hold=1, fallback=60)
    try:
        assert run(url, "wrong-token", check_db=False, timeout=10) == 1
    finally:
        stop_server(proc)
    assert run(f"http://127.0.0.1:{_free_port()}", TOKEN, check_db=False, timeout=2) == 2



def test_malformed_lease_requests_are_400_or_clamped_not_500_or_503():
    """非对象 JSON 体曾 500；max_level 超过 smallint 曾让 asyncpg 拒绝参数、表现为永远 503（worker 无限退避）。"""
    from task_server.routes import parse_filters

    assert parse_filters({"max_level": 40000}) == (32767, None, None)
    assert parse_filters({"max_compute_points": 2**40}) == (None, 2**31 - 1, None)
    assert parse_filters({"max_level": -1})[2] == "invalid max_level"
