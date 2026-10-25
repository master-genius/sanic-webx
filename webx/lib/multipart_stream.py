"""流式 multipart 解析：file 部分逐块直写磁盘，普通字段收进 dict。

**为什么单独成模块**：整传是文件服务最容易出事故的路径。``request.files`` 会把整份
文件读进内存，10 个人同时传 100MB 就是 1GB 常驻；手写切分又容易在块边界上出错。
这里用 python-multipart 的推式解析器，峰值内存 = 单个分块。

与 Web 框架解耦：只要求一个 ``read()`` 协程（返回 ``bytes``，流结束返回 ``None``），
Sanic 的 ``request.stream.read`` 直接满足；存储服务和主站共用这一份实现。

``writer`` 只需满足 ``write(bytes)`` / ``abort()`` / ``size`` 三件事，
``ObjectWriter`` 直接可用。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from python_multipart.multipart import MultipartParser, parse_options_header

from webx.lib.storage import StoreError

logger = logging.getLogger(__name__)

#: 普通表单字段的总字节上限，防止有人把巨量数据塞进"字段"绕开文件大小限制
MAX_FIELDS_BYTES = 1024 * 1024


@dataclass
class MultipartResult:
    """解析结果。"""

    fields: dict[str, str] = field(default_factory=dict)
    #: 文件部分在请求里自称的文件名（未经校验，路径成分由存储层挡）
    filename: Optional[str] = None
    #: 文件部分的字段名（如 ``file``）
    field_name: Optional[str] = None


async def stream_multipart_into(
    read: Callable[[], Awaitable[Optional[bytes]]],
    content_type: str,
    writer,
    max_size: int,
) -> MultipartResult:
    """流式解析 multipart，**第一个** file 部分写入 ``writer``。

    出错时先 ``writer.abort()`` 清理半截临时文件，再抛 ``StoreError``（带建议状态码），
    调用方不需要重复清理。

    Raises:
        StoreError: 非 multipart / 缺 boundary / 多个 file 部分（400）、
            缺 file 部分（400）、超过 ``max_size``（413）、解析失败（400）。
    """
    _, options = parse_options_header(content_type or "")
    boundary = options.get(b"boundary")
    if not boundary:
        writer.abort()
        raise StoreError("非 multipart 请求或缺少 boundary", status=400)

    st = {
        "hdr_field": b"", "hdr_val": b"", "headers": {},
        "part_name": None, "part_filename": None, "is_file": False,
        "field_buf": bytearray(), "fields_bytes": 0,
        "result": MultipartResult(), "got_file": False, "error": None,
    }

    def on_part_begin():
        st.update(
            headers={}, hdr_field=b"", hdr_val=b"", part_name=None,
            part_filename=None, is_file=False, field_buf=bytearray(),
        )

    def on_header_field(data, start, end):
        st["hdr_field"] += data[start:end]

    def on_header_value(data, start, end):
        st["hdr_val"] += data[start:end]

    def on_header_end():
        st["headers"][st["hdr_field"].lower()] = st["hdr_val"]
        st["hdr_field"] = b""
        st["hdr_val"] = b""

    def on_headers_finished():
        _, opts = parse_options_header(st["headers"].get(b"content-disposition", b""))
        name, fname = opts.get(b"name"), opts.get(b"filename")
        st["part_name"] = name.decode("utf-8", "replace") if name else None
        st["part_filename"] = fname.decode("utf-8", "replace") if fname is not None else None
        # 有 filename 属性 → 视为 file 部分（与 RFC 7578 一致）
        st["is_file"] = st["part_filename"] is not None
        if st["is_file"]:
            if st["got_file"]:
                # 契约为单 file 部分；出现第二个即拒绝，避免串写污染同一 writer
                st["error"] = StoreError("多个 file 部分不被支持", status=400)
            else:
                st["got_file"] = True
                st["result"].filename = st["part_filename"]
                st["result"].field_name = st["part_name"]

    def on_part_data(data, start, end):
        if st["error"]:
            return
        if st["is_file"]:
            writer.write(data[start:end])
        elif st["part_name"]:
            st["fields_bytes"] += end - start
            if st["fields_bytes"] > MAX_FIELDS_BYTES:
                st["error"] = StoreError("表单字段过大", status=413)
                return
            st["field_buf"] += data[start:end]

    def on_part_end():
        if not st["is_file"] and st["part_name"]:
            st["result"].fields[st["part_name"]] = bytes(st["field_buf"]).decode("utf-8", "replace")

    parser = MultipartParser(
        boundary,
        {
            "on_part_begin": on_part_begin,
            "on_header_field": on_header_field,
            "on_header_value": on_header_value,
            "on_header_end": on_header_end,
            "on_headers_finished": on_headers_finished,
            "on_part_data": on_part_data,
            "on_part_end": on_part_end,
        },
    )

    try:
        while True:
            chunk = await read()
            if chunk is None:
                break
            parser.write(chunk)
            if st["error"]:
                raise st["error"]
            # 流式请求下 Sanic 的 REQUEST_MAX_SIZE 不会自动生效，这里手动兜住上限
            if writer.size > max_size:
                raise StoreError(
                    f"文件超过上限 {max_size // 1024 // 1024}MB，请改用分片上传", status=413
                )
        parser.finalize()
        if st["error"]:
            raise st["error"]
    except StoreError:
        writer.abort()
        raise
    except Exception as e:  # 解析/写盘异常一律清理临时文件，避免半截 .tmp 泄漏
        writer.abort()
        logger.error("整传流式解析失败: %s", e, exc_info=True)
        raise StoreError("上传解析失败", status=400) from e

    if not st["got_file"]:
        writer.abort()
        raise StoreError("请求体中未找到文件", status=400)
    return st["result"]
