"""启动恢复：按本地完成标记（ok.json）分流，并与服务端协商（协议 §7.8）。

  · 已确认 → 跳过；
  · 未确认 → 重试回调（complete）/ 标记确认（cancelled）/ 不重试（failed，交服务端补偿器）；
  · 服务端 404 → 标记 not_found，一段时间后清理；网络故障 → 保留目录下次再试；
  · 无标记 → 问服务端：自己是租户就带令牌主动归还租约；终态/崩溃目录按插件策略清理。

业务软清理已换成接缝：`TaskKindPlugin.soft_clean_policy(task_type)`。
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

from webx_worker import fencing
from webx_worker.callback import callback_client
from webx_worker.plugin import TaskKindPlugin
from webx_worker.recovery import WorkerRecovery


@pytest.fixture
def rec(server, harness):
    def make(plugin=None):
        return WorkerRecovery(callback_client, str(harness.work_dir), plugin=plugin)
    return make


def _mkdir(harness, tid, ok=None, meta=None, extra=()):
    d = harness.work_dir / tid
    d.mkdir(parents=True, exist_ok=True)
    if ok is not None:
        (d / "ok.json").write_text(json.dumps(ok))
    if meta is not None:
        (d / "meta.json").write_text(json.dumps(meta))
    for name in extra:
        p = d / name
        p.mkdir() if name.endswith("/") else p.write_text("x")
    return d


def _running_for_me(server, tid=None, seq=2, worker="test-host-e2e", task_type="demo"):
    tid = server.add_task(task_type, task_id=tid)
    server.tasks[tid].update(status="running", worker_id=worker, lease_seq=seq)
    return tid


async def test_confirmed_marker_is_skipped_without_any_request(server, rec, harness):
    d = _mkdir(harness, "tdone0000001", ok={"status": "completed", "server_confirmed": True})
    await rec().recover()
    assert d.exists() and server.calls == []


async def test_dot_directories_are_not_tasks(server, rec, harness):
    """基础设施目录（如 uploader 的 .uploads/）不是任务：不得被拿去问服务端。"""
    (harness.work_dir / ".uploads" / "pending").mkdir(parents=True)
    await rec().recover()
    assert server.calls == []
    assert (harness.work_dir / ".uploads").exists()


async def test_completed_unconfirmed_is_confirmed_when_server_already_completed(server, rec, harness):
    tid = _running_for_me(server)
    server.set_status(tid, "completed")
    d = _mkdir(harness, tid, ok={"status": "completed", "server_confirmed": False})
    await rec().recover()
    assert json.loads((d / "ok.json").read_text())["server_confirmed"] is True


async def test_complete_retry_uses_the_token_recorded_in_ok_json(server, rec, harness):
    """崩溃后重启：注册表是空的，令牌靠 ok.json 里记下的 worker_id / lease_seq 临时登记。
    （旧实现重试是无令牌写，按强制 fencing 必被拒，已完成的任务永远停在 running 等租约回收。）"""
    from webx_worker import fencing

    tid = _running_for_me(server, seq=2)
    d = _mkdir(harness, tid, ok={
        "status": "completed", "server_confirmed": False,
        "worker_id": "test-host-e2e", "lease_seq": 2,
    })
    await rec().recover()
    assert server.task(tid)["status"] == "completed"
    assert json.loads((d / "ok.json").read_text())["server_confirmed"] is True
    assert fencing.lease_seq_of(tid) is None, "临时登记用完即注销"


async def test_complete_retry_ignores_a_token_recorded_by_another_worker(server, rec, harness):
    """请求头里的 worker_id 取自当前配置，别的实例留下的令牌拿来也过不了，不登记、保持未确认。"""
    tid = _running_for_me(server, seq=2)
    d = _mkdir(harness, tid, ok={
        "status": "completed", "server_confirmed": False,
        "worker_id": "some-other-worker", "lease_seq": 2,
    })
    await rec().recover()
    assert server.task(tid)["status"] == "running"
    assert json.loads((d / "ok.json").read_text())["server_confirmed"] is False
    assert server.calls_to("POST", r"/complete$")[0]["status"] == 403


async def test_complete_retry_with_stale_token_stays_unconfirmed(server, rec, harness):
    """令牌代次已过期（任务被回收重投过）：服务端拒绝，保持未确认，且不覆盖新一轮。"""
    tid = _running_for_me(server, seq=5)
    d = _mkdir(harness, tid, ok={
        "status": "completed", "server_confirmed": False,
        "worker_id": "test-host-e2e", "lease_seq": 2,
    })
    await rec().recover()
    assert server.task(tid)["status"] == "running"
    assert json.loads((d / "ok.json").read_text())["server_confirmed"] is False


async def test_complete_retry_without_recorded_token_is_rejected(server, rec, harness):
    """升级前遗留的 ok.json 没有令牌字段：行为不变（403，留待下次启动）。"""
    tid = _running_for_me(server)
    d = _mkdir(harness, tid, ok={"status": "completed", "server_confirmed": False})
    await rec().recover()
    assert server.task(tid)["status"] == "running"
    assert json.loads((d / "ok.json").read_text())["server_confirmed"] is False
    assert server.calls_to("POST", r"/complete$")[0]["status"] == 403


async def test_completed_unconfirmed_with_server_404_marks_not_found(server, rec, harness):
    d = _mkdir(harness, "tgone0000001", ok={"status": "completed", "server_confirmed": False})
    await rec().recover()
    ok = json.loads((d / "ok.json").read_text())
    assert ok["status"] == "not_found" and ok["not_found_at"]


async def test_cancelled_unconfirmed_gets_confirmed(server, rec, harness):
    d = _mkdir(harness, "tcanc0000001", ok={"status": "cancelled", "server_confirmed": False})
    await rec().recover()
    assert json.loads((d / "ok.json").read_text())["server_confirmed"] is True
    assert server.calls == []


async def test_failed_unconfirmed_is_never_retried(server, rec, harness):
    """故意不重试 fail_task：重启后服务端可能已被补偿器重投，补打过期 fail 会覆盖正在执行的工作。"""
    tid = _running_for_me(server)
    d = _mkdir(harness, tid, ok={"status": "failed", "server_confirmed": False, "error_message": "x"})
    await rec().recover()
    assert not server.calls_to("POST", r"/fail$")
    assert json.loads((d / "ok.json").read_text())["server_confirmed"] is False


async def test_not_found_marker_expires_after_seven_days(server, rec, harness):
    fresh = _mkdir(harness, "tnf000000001", ok={
        "status": "not_found", "not_found_at": datetime.now(timezone.utc).isoformat()})
    old = _mkdir(harness, "tnf000000002", ok={
        "status": "not_found",
        "not_found_at": (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()})
    await rec().recover()
    assert fresh.exists() and not old.exists()


async def test_no_marker_server_404_marks_not_found_but_keeps_directory(server, rec, harness):
    d = _mkdir(harness, "tnomark00001")
    await rec().recover()
    assert d.exists() and json.loads((d / "ok.json").read_text())["status"] == "not_found"


async def test_no_marker_network_failure_keeps_directory_untouched(server, rec, harness, monkeypatch):
    from webx_worker.config import worker_settings

    d = _mkdir(harness, "tnet00000001", extra=("a.txt",))
    monkeypatch.setattr(worker_settings, "WEB_BASE_URL", "http://127.0.0.1:1")
    await rec().recover()
    assert d.exists() and (d / "a.txt").exists() and not (d / "ok.json").exists()


async def test_no_marker_server_completed_keeps_directory(server, rec, harness):
    tid = _running_for_me(server)
    server.set_status(tid, "completed")
    d = _mkdir(harness, tid, extra=("out/",))
    await rec().recover()
    assert d.exists() and (d / "out").exists()


async def test_no_marker_i_am_the_lessee_releases_with_fencing_then_cleans(server, rec, harness):
    """★ 重启后 5 秒就能确知「这一轮没跑完」：自己是租户 → 带令牌主动归还，任务立刻回池。"""
    tid = _running_for_me(server, seq=2)
    d = _mkdir(harness, tid, meta={"task_type": "demo"}, extra=("scratch.txt",))
    await rec().recover()
    c = server.calls_to("DELETE", rf"/internal/tasks/{tid}/lease$")[0]
    assert c["status"] == 200
    assert c["headers"]["x-worker-id"] == "test-host-e2e" and c["headers"]["x-lease-seq"] == "2"
    assert server.task(tid)["status"] == "pending"
    assert not d.exists(), "默认策略：崩溃目录整个删除"
    assert fencing.lease_seq_of(tid) is None, "临时登记的令牌必须注销"


async def test_no_marker_someone_else_is_the_lessee_does_not_release(server, rec, harness):
    """租户不是本机 → 不发归还（发了也只会被拒）。"""
    tid = _running_for_me(server, worker="another-host-gpu1", seq=7)
    _mkdir(harness, tid)
    await rec().recover()
    assert not server.calls_to("DELETE", r"/lease$")
    assert server.task(tid)["worker_id"] == "another-host-gpu1"


@pytest.mark.parametrize("status", ["failed", "cancelled", "pending", "queued", "running"])
async def test_crashed_directory_is_removed_by_default(server, rec, harness, status):
    tid = server.add_task("demo")
    server.tasks[tid]["status"] = status
    d = _mkdir(harness, tid, extra=("in/", "params.json"))
    await rec().recover()
    assert not d.exists()


async def test_soft_clean_policy_plugin_keeps_only_listed_names(server, rec, harness):
    """★ 业务软清理接缝：插件说保留哪些昂贵产物，其余（含 ok.json / 元数据）清掉。"""
    class KeepCache(TaskKindPlugin):
        def soft_clean_policy(self, task_type):
            return frozenset({"cache", "index.json"}) if task_type == "heavy" else None

    heavy = server.add_task("heavy")
    server.tasks[heavy]["status"] = "failed"
    light = server.add_task("light")
    server.tasks[light]["status"] = "failed"
    dh = _mkdir(harness, heavy, extra=("cache/", "index.json", "in/", "params.json", "junk.tmp"))
    dl = _mkdir(harness, light, extra=("cache/", "params.json"))

    await rec(KeepCache()).recover()
    assert sorted(p.name for p in dh.iterdir()) == ["cache", "index.json"]
    assert not dl.exists(), "非 heavy 类型仍是整目录删除"


async def test_soft_clean_falls_back_to_task_type_in_local_meta(server, rec, harness):
    """task_type 优先取服务端权威值，缺失时回退本地 meta.json（容错）。"""
    class P(TaskKindPlugin):
        def soft_clean_policy(self, task_type):
            return frozenset({"keep.me"}) if task_type == "from-meta" else None

    tid = server.add_task("")
    server.tasks[tid]["status"] = "failed"
    d = _mkdir(harness, tid, meta={"task_type": "from-meta"}, extra=("keep.me", "drop.me"))
    await rec(P()).recover()
    assert sorted(p.name for p in d.iterdir()) == ["keep.me"]


async def test_known_limitation_recovery_removes_directory_regardless_of_ownership(
    server, rec, harness,
):
    """【已知限制·未修，审计 #14】release 分支比对了 worker_id，紧随其后的 rmtree 分支没有任何
    归属校验。今天安全，因为每个实例各自独占工作目录；若多实例共享工作目录，
    worker A 的恢复扫描会删掉 worker B 正在跑的任务目录。"""
    tid = _running_for_me(server, worker="another-host-gpu1", seq=7)
    d = _mkdir(harness, tid, extra=("data.bin",))
    await rec().recover()
    assert not d.exists()


async def test_one_bad_directory_does_not_stop_the_scan(server, rec, harness):
    bad = _mkdir(harness, "tbad00000001")
    (bad / "ok.json").write_text("{not json")
    good = _mkdir(harness, "tgood0000001", ok={"status": "cancelled", "server_confirmed": False})
    await rec().recover()
    assert json.loads((good / "ok.json").read_text())["server_confirmed"] is True
