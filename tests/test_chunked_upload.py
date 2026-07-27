"""分片上传测试（独立可选功能）。

核心存储层的测试在 test_storage.py——那边全是无状态操作，
会话目录、meta.json、超龄回收这套只在本模块。
"""

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

    assert chunked.cleanup_stale(max_age_hours=24) == 1
    assert chunked.get(stale) is None
    assert chunked.get(fresh) is not None


def test_cleanup_handles_broken_meta(store, chunked):
    """meta 损坏的目录也要能回收，不能因为一个坏目录卡住整轮清理。"""
    sdir = chunked.sessions_dir / "abcdef123456"
    sdir.mkdir(parents=True)
    (sdir / "meta.json").write_text("{ not json")

    # mtime 是当下，24 小时阈值下不该被清；用 0 阈值强制回收
    assert chunked.cleanup_stale(max_age_hours=0) >= 1
