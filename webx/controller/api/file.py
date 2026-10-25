"""文件端点：上传（整传 / 分片）、下载、列表、删除。

本目录的 ``__mid__.py`` 已挂 ``auth_middleware``，故这些端点全部需要登录。
公开的签名下载在 ``webx/controller/public/download.py``。

**对象键由业务决定长什么样**。这里用 ``<bucket>/<name>`` 两段：bucket 通常是
业务实体 ID（任务 ID、用户 ID…），name 是文件名。不够用就把这个蓝图关掉
（``create_app(builtin_controller=[...])`` 去掉 ``api``）自己写——
底层的 ``webx.lib.storage`` 支持任意层级的键。

整传为什么要流式：``request.files`` 会把整份文件读进内存。10 个人同时传
100MB 就是 1GB 常驻。这里改用 Sanic 的流式 body + 手工 multipart 解析，
峰值内存 = 单个分块。
"""

from __future__ import annotations

import logging

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

from webx.i18n import get_lang_from_header, t
from webx.lib.storage import StoreError
from webx.services.storage_http import (
    append_chunk,
    decode_param,
    receive_multipart,
    store_error_response,
    with_offset,
)
from webx.services.storage_service import storage_service

logger = logging.getLogger(__name__)

file_bp = Blueprint("webx_file", url_prefix="/api/files")


def _key(bucket: str, name: str) -> str:
    return f"{bucket}/{name}"


def _store_error(request: Request, exc: StoreError):
    """保留原签名的薄包装（路由里到处在用），实现见 storage_http。"""
    return store_error_response(exc)


# ==================== 整传 ====================


@file_bp.post("/<bucket:str>", stream=True)
async def upload(request: Request, bucket: str):
    """整传上传（流式落盘）。

    请求体是 ``multipart/form-data``，取其中第一个文件部分；可选表单字段
    ``filename`` 覆盖文件部分自带的文件名。超过 ``UPLOAD_MAX_SIZE`` 立即中止并清理临时文件。
    """
    bucket = decode_param(bucket)
    lang = get_lang_from_header(request.headers)

    try:
        writer = storage_service.store.open_writer(bucket)
    except StoreError as e:
        return _store_error(request, e)

    try:
        parsed = await receive_multipart(request, writer)
    except StoreError as e:  # 解析函数已 abort 过 writer
        return _store_error(request, e)
    except Exception as e:
        writer.abort()
        logger.exception("上传失败: bucket=%s", bucket)
        return json({"error": f"上传失败: {e}"}, status=500)

    filename = (parsed.fields.get("filename") or parsed.filename or "").strip()
    size = writer.size
    try:
        path = writer.finalize(filename)
    except StoreError as e:
        writer.abort()
        return _store_error(request, e)

    key = _key(bucket, path.name)
    return json(
        {
            "message": t("success", lang),
            "data": {"key": key, "filename": path.name, "size": size},
        },
        status=201,
    )


# ==================== 分片上传 ====================


@file_bp.post("/<bucket:str>/uploads")
async def upload_init(request: Request, bucket: str):
    """创建分片上传会话。

    body: ``{"filename": "big.zip", "total": 12345}``（total 可省，省则不校验总长）
    """
    bucket = decode_param(bucket)
    body = request.json or {}
    filename = str(body.get("filename") or "").strip()
    if not filename:
        return json({"error": "filename 必填"}, status=400)

    try:
        session = storage_service.chunked.create(
            _key(bucket, filename), int(body.get("total", -1))
        )
    except StoreError as e:
        return _store_error(request, e)
    return json({"data": session}, status=201)


@file_bp.get("/uploads/<upload_id:str>")
async def upload_status(request: Request, upload_id: str):
    """查询会话当前 offset——断线重连后先查这里，再从该处续发。"""
    try:
        session = storage_service.chunked.get(upload_id)
    except StoreError as e:
        return _store_error(request, e)
    if session is None:
        return json({"error": "分片会话不存在或已过期"}, status=404)
    return with_offset({"data": session}, session["offset"])


@file_bp.patch("/uploads/<upload_id:str>", stream=True)
async def upload_chunk(request: Request, upload_id: str):
    """追加一个分片。

    ``Upload-Offset`` 头（tus 语义）必须等于服务端已收字节数，
    不等则回 409 并带上真实 offset。
    """
    return await append_chunk(request, storage_service.chunked, upload_id)


@file_bp.post("/uploads/<upload_id:str>/complete")
async def upload_complete(request: Request, upload_id: str):
    """完成分片上传：校验字节数后原子落位。"""
    lang = get_lang_from_header(request.headers)
    try:
        path, size, key = storage_service.chunked.complete(upload_id)
    except StoreError as e:
        return _store_error(request, e)
    return with_offset(
        {
            "message": t("success", lang),
            "data": {"key": key, "filename": path.name, "size": size},
        },
        size,
    )


@file_bp.delete("/uploads/<upload_id:str>")
async def upload_abort(request: Request, upload_id: str):
    """放弃分片上传并清理会话（幂等）。"""
    try:
        storage_service.chunked.abort(upload_id)
    except StoreError as e:
        return _store_error(request, e)
    return json({"data": {"aborted": upload_id}})


# ==================== 下载 / 列表 / 删除 ====================


@file_bp.get("/<bucket:str>")
async def list_files(request: Request, bucket: str):
    """列出该 bucket 下的文件名。"""
    bucket = decode_param(bucket)
    return json({"data": {"files": storage_service.store.list_prefix(bucket)}})


@file_bp.get("/<bucket:str>/<filename:str>")
async def download(request: Request, bucket: str, filename: str):
    """下载（流式 + 断点续传）。

    ``?name=`` 可指定呈现给用户的文件名；``?inline=1`` 让浏览器内联展示。
    """
    bucket = decode_param(bucket)
    filename = decode_param(filename)
    lang = get_lang_from_header(request.headers)
    path = storage_service.store.exists(_key(bucket, filename))
    if not path:
        return json({"error": t("not_found", lang)}, status=404)

    return await storage_service.send(
        path,
        download_name=request.args.get("name") or None,
        range_header=request.headers.get("range"),
        inline=request.args.get("inline") in ("1", "true"),
    )


@file_bp.head("/<bucket:str>/<filename:str>")
async def head_file(request: Request, bucket: str, filename: str):
    """探测对象：存在则回长度与 ``Accept-Ranges``，不传文件体。"""
    bucket = decode_param(bucket)
    filename = decode_param(filename)
    path = storage_service.store.exists(_key(bucket, filename))
    if not path:
        return json({}, status=404)
    return storage_service.head(path)


@file_bp.post("/<bucket:str>/<filename:str>/sign")
async def issue_sign(request: Request, bucket: str, filename: str):
    """签发限时下载链接，供无需登录的场景（分享、第三方回源）使用。"""
    bucket = decode_param(bucket)
    filename = decode_param(filename)
    key = _key(bucket, filename)
    if not storage_service.store.exists(key):
        return json({"error": t("not_found", get_lang_from_header(request.headers))}, status=404)

    body = request.json or {}
    ttl = body.get("ttl_seconds")
    sign = storage_service.sign(key, int(ttl) if ttl else None)
    return json({"data": {"key": key, "sign": sign, "url": f"/public/files/{key}?sign={sign}"}})


@file_bp.delete("/<bucket:str>/<filename:str>")
async def delete_file(request: Request, bucket: str, filename: str):
    """删除文件。"""
    bucket = decode_param(bucket)
    filename = decode_param(filename)
    lang = get_lang_from_header(request.headers)
    if not storage_service.store.delete(_key(bucket, filename)):
        return json({"error": t("not_found", lang)}, status=404)
    return json({"message": t("success", lang)})
