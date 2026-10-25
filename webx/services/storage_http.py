"""存储相关的 HTTP 请求级处理，被内置文件路由与独立存储服务共用。

两处的鉴权、路径、响应外壳不同，但**分片追加 / 整传解析 / 错误翻译**的行为必须一致——
断点续传客户端（worker 的 uploader）同时面对两种部署，行为稍有差异就会让重同步错位。
所以把这部分抽成一份，而不是各复制一遍。
"""

from __future__ import annotations

from urllib.parse import unquote

from sanic.request import Request
from sanic.response import json

from webx.config import settings
from webx.lib.chunked_upload import ChunkedUpload, OffsetConflict
from webx.lib.multipart_stream import MultipartResult, stream_multipart_into
from webx.lib.storage import StoreError


def decode_param(value: str) -> str:
    """还原路径参数里的百分号编码（只解一次）。

    Sanic 把路径参数**原样**交给 handler，不做解码（实测 ``%E4%BB%BB`` 原样进来）。
    不解码的话，带中文 / 空格 / 括号的键在 lenient 规则下永远找不到。
    必须在**键规则校验之前**解码：``%2e%2e``、``%2F`` 这类编码过的穿越尝试
    解码后会被 ``normalize_key`` 当作 ``..`` / 分隔符拦下，而不是绕过校验。
    """
    return unquote(value)


def json_object(request: Request) -> dict:
    """请求体 JSON 对象；缺省（无体 / null）当空对象，**其它形状（数组、字符串、数字）一律 400**。

    各处理器原先写 ``request.json or {}`` 再 ``.get``：体是 ``[1]`` / ``"x"`` / ``123`` 时 AttributeError → 500。
    """
    body = request.json
    if body is None or body == {}:
        return {}
    if not isinstance(body, dict):
        raise StoreError("请求体必须是 JSON 对象", status=400)
    return body


def store_error_response(exc: StoreError):
    """把存储层异常翻成 HTTP 响应（异常自带建议状态码）。

    ``OffsetConflict`` 额外把服务端真实 offset 带回去：头与体各一份——
    断点续传客户端读头，普通 JSON 客户端读体。
    """
    body = {"error": exc.message}
    headers = {}
    if isinstance(exc, OffsetConflict):
        body["offset"] = exc.current_offset
        headers["Upload-Offset"] = str(exc.current_offset)
    return json(body, status=exc.status, headers=headers)


def with_offset(body: dict, offset: int, status: int = 200):
    """分片类响应：offset 同时放顶层字段与 ``Upload-Offset`` 头。"""
    return json({**body, "offset": offset}, status=status, headers={"Upload-Offset": str(offset)})


async def receive_multipart(request: Request, writer) -> MultipartResult:
    """流式解析整传请求体，file 部分写入 ``writer``。

    非 multipart 直接抛 ``StoreError(400)``；其余错误由解析函数清理 writer 后抛出。
    """
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        writer.abort()
        raise StoreError("请使用 multipart/form-data 上传", status=400)
    return await stream_multipart_into(
        request.stream.read, content_type, writer, settings.UPLOAD_MAX_SIZE
    )


async def append_chunk(request: Request, chunked: ChunkedUpload, upload_id: str):
    """处理一次分片追加（tus 语义），返回响应。

    ``Upload-Offset`` 必须等于服务端已收字节数，不等回 409 并带真实 offset；
    单片不得超过 ``UPLOAD_CHUNK_MAX_BYTES``（413）；空分片 400。
    被拒的分片不推进 offset——已追加的部分是"真实进度"，客户端按头重新对齐即可。
    """
    raw_offset = request.headers.get("upload-offset")
    if raw_offset is None:
        return json({"error": "缺少 Upload-Offset 头"}, status=400)
    try:
        offset = int(raw_offset)
    except ValueError:
        return json({"error": "Upload-Offset 必须是整数"}, status=400)
    if offset < 0:
        return json({"error": "Upload-Offset 非法"}, status=400)

    max_chunk = settings.UPLOAD_CHUNK_MAX_BYTES
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > max_chunk:
        return json({"error": f"单个分片不得超过 {max_chunk} 字节"}, status=413)

    try:
        new_offset = offset
        received = 0
        async for chunk in request.stream:
            if chunk is None:
                break
            received += len(chunk)
            if received > max_chunk:
                return json(
                    {"error": f"单个分片不得超过 {max_chunk} 字节", "offset": new_offset},
                    status=413, headers={"Upload-Offset": str(new_offset)},
                )
            new_offset = chunked.append(upload_id, new_offset, chunk)
    except StoreError as e:
        return store_error_response(e)

    if received == 0:
        return json({"error": "分片不能为空"}, status=400)
    return with_offset(
        {"upload_id": upload_id, "data": {"upload_id": upload_id, "offset": new_offset}},
        new_offset,
    )
