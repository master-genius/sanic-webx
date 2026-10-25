"""上传队列的磁盘契约：原子性、崩溃恢复、两阶段状态不丢。

队列没有中心节点、没有锁、没有心跳——它的全部可靠性来自「同目录 rename 是原子的」
这一条。本组测试守的就是这条上面搭的那几件事：claim 互斥、release 保住 uploaded_url、
requeue 不用旧副本盖掉新状态。

pytest 可收集；亦可 ``python worker/tests/test_upload_queue.py`` 直跑。
"""

import json
import tempfile
from pathlib import Path


from webx_worker.sdk import upload_queue as q  # noqa: E402


def _setup(size: int = 1024):
    d = Path(tempfile.mkdtemp())
    work = d / "work"
    q.ensure_dirs(work)
    src = d / "big.dat"
    src.write_bytes(b"x" * size)
    return work, src


# ==================== 条目身份（文件名） ====================


def test_filename_is_derived_from_key_path():
    """★ 文件名从 (task_id, key_path) 派生，不是随机数——直观且天然幂等。"""
    cases = {
        ("step1", "out_url"): "t1__step1_out.json",
        ("step2", "log_url"): "t1__step2_log.json",
        ("solvent", "url"): "t1__solvent_url.json",   # 字段本身叫 url，不能缩成空
        ("bs", "ndx_url"): "t1__bs_ndx.json",
    }
    for kp, expect in cases.items():
        e = q.Entry(task_id="t1", local_path="/x", remote_name="a", key_path=list(kp))
        assert e.filename == expect, f"{kp} → {e.filename}，期望 {expect}"


def test_filename_supports_three_level_group_key():
    e = q.Entry(task_id="t1", local_path="/x", remote_name="a",
                key_path=["group", "LIG", "file"])
    assert e.filename == "t1__group_LIG_file.json"


def test_filename_cannot_escape_the_queue_dir():
    """★ 分子名已被 DTO 限定为安全字符集，这里再兜一层：**落点必须仍在队列目录内**。

    断言的是"解析后仍是 pending/ 的直接子文件"而不是"不含 .."——
    没有分隔符时 `..` 只是普通字符，禁它没有意义；真正要防的是逃逸。
    """
    work, _ = _setup()
    e = q.Entry(task_id="t1", local_path="/x", remote_name="a",
                key_path=["group", "../../etc/passwd", "file"])
    assert "/" not in e.filename and "\\" not in e.filename, e.filename
    target = (q.pending_dir(work) / e.filename).resolve()
    assert target.parent == q.pending_dir(work).resolve(), f"逃出队列目录: {target}"


def test_same_target_yields_same_entry_file():
    """★★ 幂等：同一个 (任务,步骤,字段) 重复投递 → 覆盖同一条，队列里不会并存两条。

    redo 重跑同一步时尤其重要：两条条目指向同一个本地路径，并存只会白传两遍。
    """
    work, src = _setup()
    assert q.enqueue(work, "t1", src, "step2.out", ["step2", "out_url"]) is True
    assert q.enqueue(work, "t1", src, "step2.out", ["step2", "out_url"]) is True
    assert len(q.iter_pending(work)) == 1


def test_different_fields_are_distinct_entries():
    work, src = _setup()
    q.enqueue(work, "t1", src, "step2.out", ["step2", "out_url"])
    q.enqueue(work, "t1", src, "step2.aux", ["step2", "aux_url"])
    assert len(q.iter_pending(work)) == 2


# ==================== 投递 ====================


def test_enqueue_creates_pending_entry():
    work, src = _setup()
    assert q.enqueue(work, "t1", src, "step2.out", ["step2", "out_url"]) is True
    items = q.iter_pending(work)
    assert len(items) == 1
    e = items[0]
    assert e.task_id == "t1" and e.remote_name == "step2.out"
    assert e.key_path == ["step2", "out_url"]
    assert e.size == 1024
    assert e.attempts == 0 and e.uploaded_url == ""


def test_enqueue_records_absolute_path():
    """条目只记路径不复制文件（几 GB 复制一份不可接受），故必须是绝对路径。"""
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "out_url"])
    assert Path(q.iter_pending(work)[0].local_path).is_absolute()


def test_enqueue_missing_file_returns_none():
    work, _ = _setup()
    assert q.enqueue(work, "t1", work / "nope.dat", "a.dat", ["a", "u"]) is False
    assert q.iter_pending(work) == []


def test_no_partial_files_visible():
    """★ 原子写：目录里只应出现完整的 .json，.tmp 不得残留。"""
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    names = [f.name for f in q.pending_dir(work).iterdir()]
    assert all(n.endswith(".json") for n in names), names


# ==================== claim / release / drop ====================


def test_claim_moves_to_inflight_and_is_exclusive():
    """★ claim 用 rename 实现互斥：第二次必然拿不到。"""
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    e = q.iter_pending(work)[0]

    got = q.claim(work, e)
    assert got is not None
    assert q.iter_pending(work) == [], "claim 后不该还在 pending"
    assert (q.inflight_dir(work) / e.filename).exists()

    assert q.claim(work, e) is None, "同一条目被 claim 了两次"


def test_release_increments_attempts_and_keeps_uploaded_url():
    """★ release 必须保住 uploaded_url —— 否则"上传成功、回写失败"会重传几个 GB。"""
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    e = q.claim(work, q.iter_pending(work)[0])
    e.uploaded_url = "/public/tasks/t1/results/a.dat"
    e.session_id = "sess-1"

    q.release(work, e)

    back = q.iter_pending(work)
    assert len(back) == 1
    assert back[0].attempts == 1
    assert back[0].uploaded_url == "/public/tasks/t1/results/a.dat"
    assert back[0].session_id == "sess-1"
    assert not (q.inflight_dir(work) / e.filename).exists()


def test_drop_clears_both_dirs():
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    e = q.claim(work, q.iter_pending(work)[0])
    q.drop(work, e)
    assert q.iter_pending(work) == []
    assert list(q.inflight_dir(work).glob("*.json")) == []


def test_update_keeps_entry_in_inflight():
    """update 只改内容不搬家——记 session_id 时用，条目仍在 inflight。"""
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    e = q.claim(work, q.iter_pending(work)[0])
    e.session_id = "s9"
    q.update(work, e)
    assert q.iter_pending(work) == []
    data = json.loads((q.inflight_dir(work) / e.filename).read_text(encoding="utf-8"))
    assert data["session_id"] == "s9"


# ==================== 崩溃恢复 ====================


def test_requeue_inflight_moves_back():
    """崩在半路的条目，启动时搬回 pending。"""
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    q.claim(work, q.iter_pending(work)[0])
    assert q.iter_pending(work) == []

    moved = q.requeue_inflight(work)
    assert moved == 1
    assert len(q.iter_pending(work)) == 1


def test_requeue_does_not_clobber_newer_pending():
    """★ pending 已有同名条目时，不能用 inflight 的旧副本盖回去。

    release() 刚写下的 pending 版本带着 uploaded_url；被 inflight 旧副本覆盖
    就会丢掉它，几个 GB 白传一遍。
    """
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    e = q.claim(work, q.iter_pending(work)[0])
    # 人为制造"两处都有"：inflight 是旧的，pending 是带 URL 的新的
    e.uploaded_url = "/public/x.dat"
    q.release(work, e)
    (q.inflight_dir(work) / e.filename).write_text(
        json.dumps({**e.to_dict(), "uploaded_url": ""}), encoding="utf-8"
    )

    q.requeue_inflight(work)

    items = q.iter_pending(work)
    assert len(items) == 1
    assert items[0].uploaded_url == "/public/x.dat", "被 inflight 旧副本覆盖了"
    assert list(q.inflight_dir(work).glob("*.json")) == []


def test_requeue_is_idempotent():
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    q.claim(work, q.iter_pending(work)[0])
    q.requeue_inflight(work)
    assert q.requeue_inflight(work) == 0
    assert len(q.iter_pending(work)) == 1


def test_corrupt_entry_is_dropped_not_looped():
    """损坏条目删掉即可——留着会让每轮扫描重复报错，淹没真问题。"""
    work, _ = _setup()
    bad = q.pending_dir(work) / "t1__step1_out.json"
    bad.write_text("{ not json", encoding="utf-8")
    assert q.iter_pending(work) == []
    assert not bad.exists()


# ==================== 观测 / 背压 ====================


def test_pending_bytes_counts_both_dirs():
    work, src = _setup(size=2048)
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    q.enqueue(work, "t2", src, "b.dat", ["b", "u"])
    assert q.pending_bytes(work) == 4096
    q.claim(work, q.iter_pending(work)[0])
    assert q.pending_bytes(work) == 4096, "在途的也要算进积压，否则背压会漏"


def test_referenced_paths_for_cleanup_guard():
    """清理侧据此跳过还没传完的产物。"""
    work, src = _setup()
    q.enqueue(work, "t1", src, "a.dat", ["a", "u"])
    assert str(src.resolve()) in q.referenced_paths(work)

