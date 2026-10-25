"""分片上传测试（独立可选功能）。

核心存储层的测试在 test_storage.py——那边全是无状态操作，
会话目录、meta.json、超龄回收这套只在本模块。
"""

import os
import time

import pytest

from webx.lib.chunked_upload import ChunkedUpload, OffsetConflict
from webx.lib.storage import ObjectStore, StoreError


@pytest.fixture
def store(tmp_path):
    s = ObjectStore(tmp_path / "storage")
    s.ensure_dirs()
    return s


@pytest.fixture
def chunked(store):
    c = ChunkedUpload(store)
    c.ensure_dirs()
    return c


def test_chunked_upload_happy_path(store, chunked):
    session = chunked.create("bucket/big.bin", total=9)
    uid = session["upload_id"]
    assert session["offset"] == 0

    assert chunked.append(uid, 0, b"abc") == 3
    assert chunked.append(uid, 3, b"def") == 6
    assert chunked.get(uid)["offset"] == 6
    assert chunked.append(uid, 6, b"ghi") == 9

    path, size, key = chunked.complete(uid)
    assert path.read_bytes() == b"abcdefghi"
    assert (size, key) == (9, "bucket/big.bin")
    assert chunked.get(uid) is None, "完成后会话应被清理"


def test_offset_conflict_reports_real_offset(store, chunked):
    """offset 对不上要回真实值，客户端据此重新对齐——这是断点续传的关键。"""
    uid = chunked.create("bucket/big.bin")["upload_id"]
    chunked.append(uid, 0, b"abc")

    with pytest.raises(OffsetConflict) as e:
        chunked.append(uid, 0, b"abc")      # 重发已收过的片
    assert e.value.current_offset == 3
    assert e.value.status == 409

    # 按真实 offset 续发即可恢复
    assert chunked.append(uid, 3, b"def") == 6


def test_complete_rejects_incomplete_bytes(store, chunked):
    uid = chunked.create("bucket/big.bin", total=10)["upload_id"]
    chunked.append(uid, 0, b"short")

    with pytest.raises(StoreError) as e:
        chunked.complete(uid)
    assert e.value.status == 400
    assert not store.exists("bucket/big.bin"), "不完整的文件绝不能落位"


def test_unknown_session_is_404(store, chunked):
    assert chunked.get("deadbeef") is None
    with pytest.raises(StoreError) as e:
        chunked.append("deadbeef", 0, b"x")
    assert e.value.status == 404


def test_illegal_upload_id_rejected(store, chunked):
    with pytest.raises(StoreError):
        chunked.get("../../etc")


def test_abort_session_is_idempotent(store, chunked):
    uid = chunked.create("bucket/x.bin")["upload_id"]
    chunked.abort(uid)
    chunked.abort(uid)      # 再来一次不该报错
    assert chunked.get(uid) is None


def test_cleanup_stale_sessions(store, chunked):
    """超龄会话必须回收，否则 init 后不 complete 的就是磁盘泄漏。"""
    fresh = chunked.create("bucket/fresh.bin")["upload_id"]
    stale = chunked.create("bucket/stale.bin")["upload_id"]

    # 把一个会话的创建时间改老
    import json
    meta_file = chunked.sessions_dir / stale / "meta.json"
    meta = json.loads(meta_file.read_text())
    meta["created_ms"] -= int(48 * 3600 * 1000)
    meta_file.write_text(json.dumps(meta))
    old = time.time() - 48 * 3600
    os.utime(chunked.sessions_dir / stale / "part", (old, old))   # 陈旧 = 创建早且很久没有写入

    assert chunked.cleanup_stale(max_age_hours=24) == 1
    assert chunked.get(stale) is None
    assert chunked.get(fresh) is not None


def test_cleanup_keeps_an_old_session_that_is_still_receiving_data(store, chunked):
    """创建于 48 小时前、但刚刚还在追加分片的会话是活的：每个 worker 启动都会跑回收，
    按创建时间判龄会把慢速大文件的上传整个删掉，客户端续传得到 404。"""
    import json

    uid = chunked.create("bucket/slow.bin")["upload_id"]
    chunked.append(uid, 0, b"abc")
    meta_file = chunked.sessions_dir / uid / "meta.json"
    meta = json.loads(meta_file.read_text())
    meta["created_ms"] -= int(48 * 3600 * 1000)
    meta_file.write_text(json.dumps(meta))

    assert chunked.cleanup_stale(max_age_hours=24) == 0
    assert chunked.get(uid)["offset"] == 3


def test_append_after_complete_cannot_pollute_the_published_object(store, chunked, monkeypatch):
    """PATCH 已打开 part、尚未拿到锁时 complete 抢先落位：PATCH 拿到锁后必须发现 part 已不是会话的那个文件，
    返回 404，而不是把数据追加进已发布的对象。（旧实现：complete 回报 5 字节，文件实际 25 字节。）"""
    import fcntl

    uid = chunked.create("bucket/race.bin", 5)["upload_id"]
    chunked.append(uid, 0, b"12345")

    real_flock = fcntl.flock
    state = {"done": False}

    def flock_after_complete(fd, op):
        if op == fcntl.LOCK_EX and not state["done"]:
            state["done"] = True
            real_flock(fd, fcntl.LOCK_UN)
            chunked.complete(uid)          # 在 PATCH 打开文件之后、拿锁之前完成上传
        return real_flock(fd, op)

    monkeypatch.setattr(fcntl, "flock", flock_after_complete)
    with pytest.raises(StoreError) as ei:
        chunked.append(uid, 5, b"EXTRA-AFTER-COMPLETE")
    assert ei.value.status == 404
    assert store.exists("bucket/race.bin").read_bytes() == b"12345", "已发布对象不得被污染"


def test_cleanup_handles_broken_meta(store, chunked):
    """meta 损坏的目录也要能回收，不能因为一个坏目录卡住整轮清理。"""
    sdir = chunked.sessions_dir / "abcdef123456"
    sdir.mkdir(parents=True)
    (sdir / "meta.json").write_text("{ not json")

    # mtime 是当下，24 小时阈值下不该被清；用 0 阈值强制回收
    assert chunked.cleanup_stale(max_age_hours=0) >= 1
