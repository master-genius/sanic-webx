"""LaneManager：Lane 是槽位、task_type→Lane 映射是能力声明；空 LANES 不得空转。

已确认缺陷（本包修复）：旧实现在 LANES 为空时 `task_to_lane` 为空，领取循环每 5 秒
空转一次、永远不发领取请求——而默认配置里 LANES 恰好是注释掉的。
文档承诺的是「空 LANES = 所有已声明的 channel 归入一条 Lane」。
"""

import logging

from webx_worker import lease as lease_mod
from webx_worker.lane_manager import LaneManager


def test_empty_lanes_puts_every_declared_channel_into_one_lane():
    lm = LaneManager([], channel_types=["a", "b", "c"])
    assert lm.num_lanes == 1
    assert lm.types_of(0) == ["a", "b", "c"]
    assert lm.task_to_lane == {"a": 0, "b": 0, "c": 0}


def test_none_like_empty_lanes_behave_the_same():
    """配置里 LANES 缺省 / 为 None / 为 [] 都是「没配」。"""
    for lanes in ([], list()):
        assert LaneManager(lanes, ["x"]).types_of(0) == ["x"]


def test_explicit_lanes_are_respected_and_not_merged():
    lm = LaneManager([["a", "b"], ["c"]], channel_types=["a", "b", "c"])
    assert lm.num_lanes == 2
    assert lm.types_of(0) == ["a", "b"] and lm.types_of(1) == ["c"]
    assert lm.get_lane("c") == 1
    assert lm.get_lane("unknown") == 0           # 未配置的类型归 Lane 0（旧语义保持）


def test_declared_but_uncovered_channel_is_warned_not_silently_dropped(caplog):
    """非空 LANES 漏写了某个 channel：它永远领不到任务——至少要响亮地说出来（不擅自修）。"""
    with caplog.at_level(logging.WARNING):
        lm = LaneManager([["a"]], channel_types=["a", "forgotten"])
    assert "forgotten" in caplog.text
    assert lm.task_to_lane == {"a": 0}            # 行为不变：不替用户补进去


def test_duplicate_channel_names_do_not_duplicate_capabilities():
    assert LaneManager([], ["a", "a", "b"]).types_of(0) == ["a", "b"]


async def test_dispatcher_with_empty_lanes_actually_sends_a_lease_request(harness, server):
    """★ 回归：LANES 为空时领取循环必须发出带全部类型的领取请求，而不是进入空转分支。

    对着旧实现（lanes 为空 ⇒ slots 为空 ⇒ 每 `_NO_SLOT_SLEEP_SEC` 睡一次、永不发请求）
    这条会红：服务端一条 /task/leases 都收不到。
    """
    harness.channel("alpha", "print('a')")
    harness.channel("beta", "print('b')")
    tid = server.add_task("beta")
    await harness.start(lanes=[])
    try:
        assert await harness.wait_for(lambda: server.calls_to("POST", r"^/task/leases$"), timeout=5), \
            "空 LANES 下领取循环空转了：一条领取请求都没发"
        assert await harness.wait_for(lambda: server.task(tid)["status"] == "completed")
    finally:
        await harness.stop()
    first = server.calls_to("POST", r"^/task/leases$")[0]["json"]
    assert first["slots"] == [["alpha", "beta"]], "能力声明应是单条 Lane 装下所有 channel"


async def test_two_lanes_send_one_slot_per_idle_lane(harness, server):
    """多 Lane：每条空闲 Lane 一个槽位（没有 n 参数）。"""
    harness.channel("heavy", "print('h')")
    harness.channel("light", "print('l')")
    await harness.start(lanes=[["heavy"], ["light"]])
    try:
        assert await harness.wait_for(lambda: server.calls_to("POST", r"^/task/leases$"), timeout=5)
    finally:
        await harness.stop()
    first = server.calls_to("POST", r"^/task/leases$")[0]["json"]
    assert first["slots"] == [["heavy"], ["light"]]
    assert "n" not in first


def test_no_slot_sleep_is_a_patchable_module_constant():
    """测试要靠它把兜底空转压短；若有人把它写回字面量，上面的回归测试会变慢甚至误判。"""
    assert isinstance(lease_mod._NO_SLOT_SLEEP_SEC, float)
