"""lease 分发路径的执行前流程 —— 守住「任务一个都跑不了」这一类。

两样东西曾在改造中丢过，且都不是边角情况而是主路径彻底不工作：

  1. `executor.execute(task_id, payload)` 被误写成 `execute(_to_payload(task))`
     → 每个任务一领到就 TypeError；
  2. 「上报 running（3 次重试，失败即放弃）」整段消失
     → 任务在库里永远显示排队中，且 start_time 为空使 grace 恒取下限。

本测试盯执行前那几步的**调用形状**，用桩替换 callback，不碰真实 executor / HTTP。
（对着真实 HTTP 的版本见 test_e2e_lifecycle。）
"""

import inspect

from webx_worker import lease as lease_mod
from webx_worker.executor import GenericTaskExecutor

_ROW = {
    "id": "t-1", "task_type": "demo", "lease_seq": 7,
    "sub_step": "em", "steps": '["bs"]', "timeout_seconds": 100,
}


class _FakeLaneManager:
    num_lanes = 1
    task_to_lane = {"demo": 0}

    def types_of(self, _lane_id):
        return ["demo"]


class _RecordingExecutor:
    def __init__(self, calls):
        self._calls = calls

    async def execute(self, task_id, payload):      # 必须与真实签名同形
        self._calls.append(("execute", task_id, payload.get("task_type")))


async def _run(monkeypatch, callback_stub, calls):
    """用桩替换 callback_client，跑一次 _run_one。"""
    import webx_worker.callback as cb

    monkeypatch.setattr(cb, "callback_client", callback_stub)
    monkeypatch.setattr(lease_mod, "_ACK_FAIL_COOLDOWN_SEC", 0.01)    # 别让测试真等 5s
    d = lease_mod.LeaseDispatcher(_FakeLaneManager(), _RecordingExecutor(calls))
    await d._run_one(0, dict(_ROW))


def test_execute_signature_matches_call_site():
    """调用点传两个位置参数，executor.execute 就必须收两个。签名一改而调用点没跟上，立刻红。"""
    params = list(inspect.signature(GenericTaskExecutor.execute).parameters)[1:]
    assert params == ["task_id", "payload"], f"execute 形参变了: {params}"


async def test_happy_path_reports_running_then_executes(monkeypatch):
    """正常路径必须是：ack → 上报 running → execute(task_id, payload)。"""
    calls = []

    class _OK:
        async def ack_lease(self, tid):
            calls.append(("ack", tid)); return True

        async def update_status(self, tid, status):
            calls.append(("status", tid, status)); return True

        async def release_lease(self, tid):
            calls.append(("release", tid)); return True

    await _run(monkeypatch, _OK(), calls)

    assert calls[0][0] == "ack", f"ack 必须最先（实际 {calls}）"
    assert ("status", "t-1", "running") in calls, f"缺少上报 running（实际 {calls}）"
    assert ("execute", "t-1", "demo") in calls, f"execute 参数不对（实际 {calls}）"
    assert calls.index(("status", "t-1", "running")) < calls.index(("execute", "t-1", "demo")), \
        "上报 running 必须在执行之前"


async def test_running_report_failure_skips_execution_and_releases_lease(monkeypatch):
    """上报 running 连续失败 → 不执行，并主动归还租约让任务立刻回池。"""
    calls = []

    class _RunningFails:
        async def ack_lease(self, tid):
            calls.append(("ack", tid)); return True

        async def update_status(self, tid, status):
            calls.append(("status", tid, status)); return False

        async def release_lease(self, tid):
            calls.append(("release", tid)); return True

    await _run(monkeypatch, _RunningFails(), calls)

    assert not any(c[0] == "execute" for c in calls), "云端不知情就不该执行"
    assert ("release", "t-1") in calls, "应归还租约（否则要干等一个完整租约周期）"
    assert sum(1 for c in calls if c[0] == "status") == 3, "应重试 3 次"


async def test_ack_failure_skips_execution(monkeypatch):
    """ack 失败 → 直接放弃，连 running 都不该上报。"""
    calls = []

    class _AckFails:
        async def ack_lease(self, tid):
            calls.append(("ack", tid)); return False

        async def update_status(self, tid, status):
            calls.append(("status", tid, status)); return True

        async def release_lease(self, tid):
            calls.append(("release", tid)); return True

    await _run(monkeypatch, _AckFails(), calls)

    assert calls == [("ack", "t-1")], f"ack 失败后不该再做任何事（实际 {calls}）"


async def test_token_is_registered_before_ack(monkeypatch):
    """令牌先落注册表，再做任何上报——顺序反了的话 ack 就不带 fencing header。"""
    from webx_worker import fencing

    seen = {}

    class _Spy:
        async def ack_lease(self, tid):
            seen["seq_at_ack"] = fencing.lease_seq_of(tid)
            return False

        async def release_lease(self, tid):
            return True

    await _run(monkeypatch, _Spy(), [])
    assert seen["seq_at_ack"] == 7
    assert fencing.lease_seq_of("t-1") is None, "任务结束后必须注销令牌"


def test_payload_follows_the_protocol_task_shape():
    """`_to_payload` 把 §3.1 的 <Task> 整形成 executor 的 payload；字符串 steps 会被解析。"""
    p = lease_mod._to_payload({
        "id": "t9", "name": "n", "task_type": "demo", "files_json": "[]", "params_text": "{}",
        "steps": '["a", "b"]', "sub_step": "c", "end_step": "d", "timeout_seconds": 5,
        "allowed_retries": 2, "lease_seq": 3, "worker_id": "w", "status": "queued",
    })
    assert p["task_id"] == "t9" and p["steps"] == ["a", "b"] and p["timeout_seconds"] == 5
    assert "expected_output" not in p            # 协议已删除该字段


# ───────────────────────── 停机排空（K5 窗口） ─────────────────────────


async def test_claims_arriving_during_drain_are_given_back_not_executed(monkeypatch):
    """停机开始后才返回的长轮询回包：任务已被服务端认领，必须带令牌还回去、不得执行。

    否则它停在 queued，进程退出后没人 ack，只能等 60s 短租约（旧实现的随机失败源）。
    """
    import webx_worker.callback as cb
    from webx_worker import fencing

    calls = []

    class _CB:
        async def release_lease(self, tid, extra_headers=None):
            calls.append(("release", tid, dict(extra_headers or {})))
            return True

    async def acquire(_slots):
        lease_mod.begin_drain()                 # 模拟：长轮询挂起期间收到 SIGTERM
        return [dict(_ROW)]

    monkeypatch.setattr(cb, "callback_client", _CB())
    monkeypatch.setattr(lease_mod.lease_client, "acquire", acquire)
    monkeypatch.setattr(lease_mod.worker_settings, "WORKER_ID", "w-drain", raising=False)
    exec_calls = []
    d = lease_mod.LeaseDispatcher(_FakeLaneManager(), _RecordingExecutor(exec_calls))

    await d.loop()

    assert exec_calls == [], "排空期间不得执行新认领的任务"
    assert [c[:2] for c in calls] == [("release", "t-1")]
    hdr = {k.lower(): v for k, v in calls[0][2].items()}
    assert hdr.get("x-worker-id") == "w-drain" and hdr.get("x-lease-seq") == "7", "归还必须带 fencing 令牌"
    assert fencing.lease_seq_of("t-1") is None, "令牌用完即注销，不残留"
    assert not lease_mod._running_tasks

