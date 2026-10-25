"""uploader 的重试必须退避 —— 「永不放弃」不等于「每 3 秒重发一次几个 GB」。

队列在 `release()` 里一直记着 `attempts` 与本次时间 `queued_at`，但**从没有人读**。
于是恒定失败的条目（storage 侧 400、本地文件损坏、名字非法…）会：

  · 每个扫描周期（默认 3s）重来一次；整传路径每轮重发**整个文件**，
    几个 GB 的产物足以长期占满上行带宽，把正常任务的上传拖垮；
  · 每轮刷一条 warning，真正的异常被淹没。

判据：首次立即试；失败后按 5,10,20,…s 指数退避、封顶 300s；到期即试，绝不丢弃。

"""

from pathlib import Path


from webx_worker.sdk import upload_queue as q  # noqa: E402
from webx_worker import uploader as U  # noqa: E402

_NOW = 1_000_000.0


def _entry(attempts: int, last_try_ago: float) -> q.Entry:
    return q.Entry(
        task_id="t1", local_path="/tmp/x.bin", remote_name="x.bin", key_path=["a", "u"],
        size=1024, queued_at=_NOW - last_try_ago, attempts=attempts,
    )


def test_first_attempt_is_immediate():
    """从没试过的条目不受退避影响 —— 正常路径必须零延迟。"""
    assert U._is_due(_entry(0, 0.0), _NOW)


def test_backoff_is_exponential_and_capped():
    assert U._retry_delay(1) == 5.0
    assert U._retry_delay(2) == 10.0
    assert U._retry_delay(3) == 20.0
    assert U._retry_delay(99) == U._RETRY_BACKOFF_MAX_SEC == 300.0
    # 单调不减，且永不为负
    prev = -1.0
    for n in range(0, 40):
        d = U._retry_delay(n)
        assert d >= prev >= -1.0 and d <= 300.0
        prev = d


def test_not_due_before_delay_and_due_after():
    e = _entry(3, 19.0)                      # 第 3 次失败 → 需等 20s，只过了 19s
    assert not U._is_due(e, _NOW)
    assert U._is_due(_entry(3, 20.0), _NOW)  # 到点就试，不多等


def test_capped_entry_still_retries():
    """退避封顶后仍然会重试 —— 队列语义是永不放弃，退避只是放慢。"""
    assert U._is_due(_entry(50, 301.0), _NOW)
    assert not U._is_due(_entry(50, 299.0), _NOW)


def test_main_loop_consults_the_guard():
    """主循环必须真的用上这个判据（否则以上全是空断言）。"""
    src = (Path(U.__file__)).read_text(encoding="utf-8")
    loop = src[src.index("async def run("):]
    assert "_is_due(entry, now)" in loop, "主循环没有接入退避守卫"
    assert "_RETRY_ALERT_EVERY" in loop, "连续失败没有升级告警"

