"""存储层审计修复的回归用例：全部小数据、毫秒级，只验证基本逻辑。"""

import asyncio

import pytest

from webx.lib.multipart_stream import stream_multipart_into
from webx.lib.storage import LENIENT, STRICT, StoreError, normalize_key


class _W:
    def __init__(self):
        self.buf, self.size, self.aborted = b"", 0, False

    def write(self, b):
        self.buf += b
        self.size = len(self.buf)

    def abort(self):
        self.aborted = True


def _run(body: bytes):
    chunks = [body, None]

    async def read():
        return chunks.pop(0)

    w = _W()
    return w, asyncio.run(stream_multipart_into(read, "multipart/form-data; boundary=B", w, 10**6))


HEAD = b'--B\r\nContent-Disposition: form-data; name="file"; filename="a.bin"\r\n\r\n'


def test_multipart_with_closing_boundary_is_accepted():
    w, r = _run(HEAD + b"DATA\r\n--B--\r\n")
    assert w.buf == b"DATA" and r.filename == "a.bin" and not w.aborted


def test_multipart_without_closing_boundary_is_rejected_and_aborted():
    """被截断的请求体不能当成功落位：否则半截内容会覆盖同名的好文件。"""
    w = _W()
    chunks = [HEAD + b"PARTIAL", None]

    async def read():
        return chunks.pop(0)

    with pytest.raises(StoreError) as ei:
        asyncio.run(stream_multipart_into(read, "multipart/form-data; boundary=B", w, 10**6))
    assert ei.value.status == 400 and w.aborted


def test_key_rules_reject_a_trailing_newline():
    """Python 的 $ 会放行末尾换行："a\\n" 曾能造出不可见的同名文件、污染日志。"""
    for rule in (STRICT, LENIENT):
        assert rule.check("a") is True
        assert rule.check("a\n") is False
    with pytest.raises(StoreError):
        normalize_key("t/x\n")


def test_task_id_rejects_a_trailing_newline():
    from webx_storage.task_results import TaskResultsPolicy

    with pytest.raises(StoreError):
        TaskResultsPolicy.check_task_id("t1\n")
    with pytest.raises(StoreError):
        TaskResultsPolicy.check_filename(["a"])      # 非字符串不再触发 500


def test_download_url_is_percent_encoded_and_round_trips():
    """名字里的 # ? % 必须编码；"100%25.txt" 与 "100%.txt" 是两个文件，链接不能串。"""
    from webx_storage.task_results import TaskResultsPolicy as P

    for name in ("plain.zip", "100%.txt", "100%25.txt", "a#b?c.txt", "中文 名.txt"):
        url = P.download_url("t1", name)
        assert "#" not in url and "?" not in url
        assert P.parse_url(url) == ("t1", name), name
    assert P.download_url("t1", "100%25.txt") != P.download_url("t1", "100%.txt")
    assert P.parse_url("http://[bad") is None            # 坏条目只标记，不让整批 500


def test_json_object_rejects_non_objects():
    from types import SimpleNamespace

    from webx.services.storage_http import json_object

    assert json_object(SimpleNamespace(json=None)) == {}
    assert json_object(SimpleNamespace(json={"a": 1})) == {"a": 1}
    for bad in ([1], "x", 123):
        with pytest.raises(StoreError) as ei:
            json_object(SimpleNamespace(json=bad))
        assert ei.value.status == 400


def test_parse_total_and_ttl_survive_overflow():
    from webx_storage.routes import parse_total, parse_ttl

    assert parse_total({"total": float("inf")}) == -1
    assert parse_ttl({"ttl_seconds": float("inf")}) is None


def test_retention_refuses_nonpositive_days_even_for_once(monkeypatch, capsys):
    """DAYS<=0 等于“所有对象都已过期”：--once 以前会删光刚上传的文件。main() 自己 configure()，
    所以用环境变量驱动，并在用完后还原全局配置单例。"""
    import webx.config as cfgmod
    from webx_storage import retention

    saved = cfgmod._settings
    monkeypatch.setenv("STORAGE_RETENTION_DAYS", "0")
    try:
        assert retention.main(["--once"]) == 2
        assert "拒绝" in capsys.readouterr().out
        assert retention.main(["--dry-run"]) == 2
    finally:
        cfgmod._settings = saved          # 无论原先是什么（含 None）都还原，别让存储配置污染后面的用例


def test_store_rejects_key_conflicting_with_existing_path(tmp_path):
    """键的父段是已存在的文件 / 目标是目录：客户端选错键，409 而不是 500。"""
    from webx.lib.storage import ObjectStore

    st = ObjectStore(tmp_path, "lenient")
    st.save_bytes("a/file", b"x")
    w = st.open_writer("")
    w.write(b"y")
    with pytest.raises(StoreError) as ei:
        w.finalize_to("a/file/child")
    assert ei.value.status == 409
    w.abort()
    assert not list(tmp_path.glob(".upload.*.tmp")), "失败后 tmp 必须清理"
