"""存储层测试：路径守卫 / 流式落盘 / 原子落位 / Range / 签名。

分片上传是独立可选功能，测试在 test_chunked_upload.py。

这一块的失败模式都很严重（路径穿越、半截文件、并发串写），所以覆盖偏重
**攻击面与边界**，而不只是happy path。
"""

import pytest

from webx.lib.ranges import UNSATISFIABLE, ByteRange, parse_byte_range
from webx.lib.signer import UrlSigner
from webx.lib.storage import ObjectStore, StoreError, normalize_key


@pytest.fixture
def store(tmp_path):
    s = ObjectStore(tmp_path / "storage")
    s.ensure_dirs()
    return s


# ==================== 路径守卫 ====================


@pytest.mark.parametrize(
    "bad_key",
    [
        "../etc/passwd",           # 相对穿越
        "a/../../etc/passwd",      # 中段穿越
        "a/./b",                   # 单点段
        "",                        # 空键
        "/",                       # 只有分隔符
        "a//b/../..",              # 组合
        "a/b\x00c",                # NUL 注入
        "a b/c",                   # 空格（不在安全字符集内）
        "a;rm -rf/b",              # shell 元字符
    ],
)
def test_normalize_key_rejects_traversal(bad_key):
    with pytest.raises(StoreError):
        normalize_key(bad_key)


def test_normalize_key_accepts_normal():
    assert normalize_key("tasks/abc123/results/em.zip") == "tasks/abc123/results/em.zip"
    assert normalize_key("/a/b/") == "a/b"          # 首尾分隔符归一化
    assert normalize_key("a\\b") == "a/b"           # 反斜杠也当分隔符
    assert normalize_key("file-1.tar.gz") == "file-1.tar.gz"


def test_path_stays_inside_root(store):
    path = store.path_for("bucket/file.txt")
    assert str(path).startswith(str(store.root))


def test_symlink_escape_is_blocked(store, tmp_path):
    """第二道守卫：解析后必须仍在根内——逐段校验挡不住符号链接。"""
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")

    (store.root / "bucket").mkdir(parents=True, exist_ok=True)
    (store.root / "bucket" / "link").symlink_to(outside)

    with pytest.raises(StoreError) as e:
        store.path_for("bucket/link/secret.txt")
    assert e.value.status == 403


def test_exists_returns_none_for_illegal_key(store):
    """键非法返回 None 而不是抛异常——调用方直接当 404 处理。"""
    assert store.exists("../etc/passwd") is None


# ==================== 整传（流式 + 原子）====================


def test_writer_streams_and_finalizes(store):
    writer = store.open_writer("bucket")
    writer.write(b"hello ")
    writer.write(b"world")
    path = writer.finalize("greet.txt")

    assert path.read_bytes() == b"hello world"
    assert writer.size == 11
    assert store.exists("bucket/greet.txt") == path


def test_partial_file_is_invisible_until_finalize(store):
    """落位前不得被 list 看到——否则下载方可能拿到半截文件。"""
    writer = store.open_writer("bucket")
    writer.write(b"half")

    assert store.list_prefix("bucket") == [], "未 finalize 的中间态不该出现在列表里"

    writer.finalize("done.txt")
    assert store.list_prefix("bucket") == ["done.txt"]


def test_writer_abort_cleans_tmp(store):
    writer = store.open_writer("bucket")
    writer.write(b"junk")
    writer.abort()

    assert store.list_prefix("bucket") == []
    # 连隐藏的 .tmp 也不该残留
    assert list((store.root / "bucket").iterdir()) == []


def test_writer_rejects_illegal_filename(store):
    writer = store.open_writer("bucket")
    writer.write(b"x")
    with pytest.raises(StoreError):
        writer.finalize("../escape.txt")
    writer.abort()


def test_writer_context_manager_aborts_on_error(store):
    with pytest.raises(RuntimeError):
        with store.open_writer("bucket") as w:
            w.write(b"data")
            raise RuntimeError("boom")

    assert list((store.root / "bucket").iterdir()) == [], "异常退出应清掉 .tmp"


def test_save_bytes_roundtrip(store):
    store.save_bytes("bucket/small.bin", b"\x00\x01\x02")
    assert store.exists("bucket/small.bin").read_bytes() == b"\x00\x01\x02"
    assert store.size_of("bucket/small.bin") == 3


# ==================== 列表 / 删除 ====================


def test_list_and_delete(store):
    store.save_bytes("bucket/a.txt", b"a")
    store.save_bytes("bucket/b.txt", b"b")
    assert store.list_prefix("bucket") == ["a.txt", "b.txt"]

    assert store.delete("bucket/a.txt") is True
    assert store.delete("bucket/a.txt") is False, "重复删除应返回 False 而非报错"
    assert store.list_prefix("bucket") == ["b.txt"]


def test_delete_prefix(store):
    store.save_bytes("bucket/a.txt", b"a")
    assert store.delete_prefix("bucket") is True
    assert store.list_prefix("bucket") == []


# ==================== Range 解析 ====================


def test_range_none_when_absent_or_malformed():
    assert parse_byte_range(None, 100) is None
    assert parse_byte_range("", 100) is None
    assert parse_byte_range("items=0-10", 100) is None      # 非 bytes 单位
    assert parse_byte_range("bytes=0-10,20-30", 100) is None  # 多段 → 降级全量
    assert parse_byte_range("bytes=abc", 100) is None


def test_range_explicit_interval():
    r = parse_byte_range("bytes=10-19", 100)
    assert isinstance(r, ByteRange)
    assert (r.start, r.end, r.size, r.total) == (10, 19, 10, 100)


def test_range_open_ended():
    r = parse_byte_range("bytes=90-", 100)
    assert (r.start, r.end, r.size) == (90, 99, 10)


def test_range_suffix():
    r = parse_byte_range("bytes=-10", 100)
    assert (r.start, r.end, r.size) == (90, 99, 10)


def test_range_end_is_clamped():
    r = parse_byte_range("bytes=95-999", 100)
    assert (r.start, r.end, r.size) == (95, 99, 5)


def test_range_start_beyond_eof_is_416():
    """越界起点必须 416，不能被 end<start 误降级成全量 200。"""
    assert parse_byte_range("bytes=100-", 100) == UNSATISFIABLE
    assert parse_byte_range("bytes=200-300", 100) == UNSATISFIABLE


def test_range_negative_and_zero_suffix():
    assert parse_byte_range("bytes=-0", 100) is None
    assert parse_byte_range("bytes=--5", 100) is None


# ==================== 签名 ====================


def test_sign_roundtrip():
    signer = UrlSigner("a-very-long-signing-secret-key", 3600)
    key = "bucket/file.zip"
    sign = signer.issue(key)

    assert signer.verify(sign, key) is True


def test_sign_cannot_be_reused_for_another_object():
    """签名与对象键绑定——否则一个签名能拖走整个存储。"""
    signer = UrlSigner("a-very-long-signing-secret-key", 3600)
    sign = signer.issue("bucket/mine.zip")

    assert signer.verify(sign, "bucket/yours.zip") is False


def test_sign_expires():
    signer = UrlSigner("a-very-long-signing-secret-key", 3600)
    sign = signer.issue("bucket/file.zip", ttl_seconds=1)

    import time
    time.sleep(1.2)
    assert signer.verify(sign, "bucket/file.zip") is False


def test_sign_rejects_garbage():
    signer = UrlSigner("a-very-long-signing-secret-key", 3600)
    for bad in ("", "not-a-token", "aaa.bbb", "x" * 200):
        assert signer.verify(bad, "bucket/file.zip") is False


def test_sign_from_other_secret_is_rejected():
    a = UrlSigner("secret-number-one-long-enough", 3600)
    b = UrlSigner("secret-number-two-long-enough", 3600)
    assert b.verify(a.issue("bucket/f.zip"), "bucket/f.zip") is False


# ==================== 键规则 / 保留前缀 / 空目录回收 ====================

from webx.lib.storage import LENIENT, STRICT, get_key_rule  # noqa: E402


@pytest.mark.parametrize(
    "key", [".uploads/x", "a/.hidden", ".staging/a.bin", "a/.upload.1.tmp"]
)
def test_dot_prefixed_segments_are_reserved(key):
    """点开头的段是存储自己的保留区（.uploads/.staging/临时文件），任何规则都不许写。"""
    for rule in (STRICT, LENIENT):
        with pytest.raises(StoreError):
            normalize_key(key, rule)


def test_lenient_rule_keeps_user_filenames():
    assert normalize_key("t1/results/任务 A_(坐标)_ligand1.zip", LENIENT) == (
        "t1/results/任务 A_(坐标)_ligand1.zip"
    )
    with pytest.raises(StoreError):  # 严格规则仍然拒绝
        normalize_key("t1/任务.zip", STRICT)


@pytest.mark.parametrize("bad", ["a/b\x00c", "a/b\nc", "a/" + "x" * 300, "a/..", "a\\..\\b"])
def test_lenient_rule_still_blocks_dangerous_segments(bad):
    with pytest.raises(StoreError):
        normalize_key(bad, LENIENT)


def test_unknown_rule_name_fails_loudly():
    with pytest.raises(ValueError):
        get_key_rule("typo")
    assert get_key_rule(None) is STRICT and get_key_rule("LENIENT") is LENIENT


def test_store_uses_its_rule(tmp_path):
    s = ObjectStore(tmp_path / "s", key_rule="lenient")
    s.ensure_dirs()
    s.save_bytes("b/中文 名.txt", b"x")
    assert s.exists("b/中文 名.txt")
    assert ObjectStore(tmp_path / "s").exists("b/中文 名.txt") is None  # 严格规则看不见


def test_delete_prunes_empty_parents_but_never_the_root(store):
    store.save_bytes("a/b/c/f.txt", b"x")
    store.save_bytes("a/keep.txt", b"y")
    assert store.delete("a/b/c/f.txt") is True
    assert not (store.root / "a" / "b").exists(), "空的 c、b 应一并回收"
    assert (store.root / "a" / "keep.txt").exists(), "非空目录不动"

    assert store.delete("a/keep.txt") is True
    assert not (store.root / "a").exists()
    assert store.root.exists(), "存储根永不删除"


def test_delete_missing_object_returns_false(store):
    assert store.delete("nope/none.txt") is False


@pytest.mark.parametrize("bad", ["a/b", "../x", "a\\b", ""])
def test_writer_finalize_rejects_names_that_are_not_a_single_filename(store, bad):
    """filename 带路径分隔符会被当成不存在的子目录，落位时 FileNotFoundError → 接口 500。必须 400。"""
    w = store.open_writer("b")
    w.write(b"x")
    with pytest.raises(StoreError) as ei:
        w.finalize(bad)
    assert ei.value.status == 400
    w.abort()

