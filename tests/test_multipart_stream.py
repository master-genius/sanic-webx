"""流式 multipart 解析：file 逐块直写、字段收集、各类畸形输入。

用假的 ``read()`` 协程按指定块大小喂数据，专门测**块边界**——手写切分最容易在这里出错。
"""

import pytest

from webx.lib.multipart_stream import stream_multipart_into
from webx.lib.storage import ObjectStore, StoreError

B = "----t"
CT = f"multipart/form-data; boundary={B}"


def _body(*parts: tuple[dict, bytes]) -> bytes:
    out = b""
    for disp, data in parts:
        cd = "; ".join(f'{k}="{v}"' if v is not None else k for k, v in disp.items())
        out += f"--{B}\r\nContent-Disposition: form-data; {cd}\r\n\r\n".encode() + data + b"\r\n"
    return out + f"--{B}--\r\n".encode()


def _reader(data: bytes, step: int):
    chunks = [data[i:i + step] for i in range(0, len(data), step)] + [None]
    it = iter(chunks)

    async def read():
        return next(it)

    return read


@pytest.fixture
def writer(tmp_path):
    s = ObjectStore(tmp_path / "s")
    s.ensure_dirs()
    return s.open_writer("b")


@pytest.mark.parametrize("step", [1, 2, 7, 64, 100000])
async def test_file_and_fields_survive_any_chunking(tmp_path, step):
    """无论分块多碎（含 1 字节一块），内容必须逐字节一致。"""
    s = ObjectStore(tmp_path / "s")
    w = s.open_writer("b")
    payload = bytes(range(256)) * 20 + f"\r\n--{B}x\r\n".encode()  # 内含"像边界"的字节
    body = _body(
        ({"name": "filename"}, "改名.bin".encode()),
        ({"name": "file", "filename": "orig.bin"}, payload),
    )
    res = await stream_multipart_into(_reader(body, step), CT, w, max_size=10**9)
    assert res.fields == {"filename": "改名.bin"}
    assert res.filename == "orig.bin" and res.field_name == "file"
    assert w.finalize("out.bin").read_bytes() == payload


async def test_oversize_aborts_and_cleans_tmp(tmp_path):
    s = ObjectStore(tmp_path / "s")
    w = s.open_writer("b")
    body = _body(({"name": "file", "filename": "a"}, b"x" * 5000))
    with pytest.raises(StoreError) as ei:
        await stream_multipart_into(_reader(body, 512), CT, w, max_size=1000)
    assert ei.value.status == 413
    assert not list((tmp_path / "s" / "b").iterdir()), "超限必须清掉临时文件"


@pytest.mark.parametrize(
    "ctype, body, status",
    [
        ("multipart/form-data", b"x", 400),                                   # 缺 boundary
        (CT, _body(({"name": "f"}, b"just a field")), 400),                    # 没有 file 部分
        (CT, _body(({"name": "a", "filename": "1"}, b"1"),
                   ({"name": "b", "filename": "2"}, b"2")), 400),              # 两个 file 部分
    ],
)
async def test_malformed_requests_are_rejected_and_cleaned(tmp_path, ctype, body, status):
    s = ObjectStore(tmp_path / "s")
    w = s.open_writer("b")
    with pytest.raises(StoreError) as ei:
        await stream_multipart_into(_reader(body, 16), ctype, w, max_size=10**6)
    assert ei.value.status == status
    assert not list((tmp_path / "s" / "b").iterdir())


async def test_huge_form_field_is_rejected(tmp_path):
    """把巨量数据塞进"普通字段"不能绕开文件大小限制。"""
    s = ObjectStore(tmp_path / "s")
    w = s.open_writer("b")
    body = _body(({"name": "junk"}, b"x" * (1024 * 1024 + 10)),
                 ({"name": "file", "filename": "a"}, b"1"))
    with pytest.raises(StoreError) as ei:
        await stream_multipart_into(_reader(body, 4096), CT, w, max_size=10**9)
    assert ei.value.status == 413
