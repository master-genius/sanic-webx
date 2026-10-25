"""内核路由：通用对象 / 分片会话 / 签名。只认对象键，不认识任何业务概念。

约定（与全仓 API 规范一致）：路径只放资源名词，动作由 HTTP 方法表达。

  内部（X-Internal-Token）
    POST   /internal/objects                    创建对象（multipart：key + file）
    GET    /internal/objects/<key>              读取（支持 Range；HEAD 只回头）
    DELETE /internal/objects/<key>              删除，幂等
    POST   /internal/uploads                    创建分片上传会话 {key, total?}
    GET    /internal/uploads/<id>               查进度（Upload-Offset 头）
    PATCH  /internal/uploads/<id>               追加分片
    POST   /internal/uploads/<id>/complete      完成（无法映射成 CRUD 的状态迁移，才把动作放子路径）
    DELETE /internal/uploads/<id>               放弃会话，幂等
    POST   /internal/signatures                 签发限时下载签名 {key, ttl_seconds?}
  公开
    GET    /public/objects/<key>?sign=...       签名下载（签名无效一律 404，不泄露存在性）
"""

from __future__ import annotations

from functools import wraps

from sanic.request import Request
from sanic.response import json

from webx.config import settings
from webx.lib.storage import StoreError
from webx.services.storage_http import (
    append_chunk,
    decode_param,
    receive_multipart,
    store_error_response,
    with_offset,
)
from webx_storage.auth import require_internal_token
from webx_storage.policy import StorageContext


def deprecated(successor: str):
    """给兼容别名的响应打上 ``Deprecation`` 头，提示调用方迁移到规范路径。"""

    def deco(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            resp = await f(*args, **kwargs)
            resp.headers["Deprecation"] = "true"
            resp.headers["Link"] = f'<{successor}>; rel="successor-version"'
            return resp

        return wrapper

    return deco


def parse_total(body: dict) -> int:
    """total 可选，仅用于 complete 时的字节完整性校验。

    缺省 / null / 非数字一律视为「未声明」(-1)，与不传等价；不因非法入参抛 500。
    """
    try:
        return int(body.get("total", -1))
    except (TypeError, ValueError):
        return -1


def parse_ttl(body: dict):
    ttl = body.get("ttl_seconds")
    try:
        return int(ttl) if ttl is not None else None
    except (TypeError, ValueError):
        return None


def mount_core(ctx: StorageContext) -> None:
    svc, internal, public = ctx.svc, ctx.internal, ctx.public

    # ==================== 对象 ====================

    async def create_object_impl(request: Request, status: int):
        """创建通用对象（multipart/form-data）：``file`` 必填，``key`` 必填。

        ``key`` 是表单字段而不是 URL：多级键放进 URL 会与其它路由产生歧义，
        且流式请求下拿不到已解析的 body。字段顺序不受控，所以先在存储根下开写入器，
        解析完再原子落到真正的键（同一文件系统内 rename，无拷贝）。
        """
        writer = svc.store.open_writer("")
        try:
            parsed = await receive_multipart(request, writer)
        except StoreError as e:  # 解析函数已 abort 过 writer
            return store_error_response(e)

        key = (parsed.fields.get("key") or "").strip()
        if not key:
            writer.abort()
            return json({"error": "缺少 key 字段"}, status=400)
        try:
            norm = svc.store.normalize(key)
            ctx.check_generic_write(norm)
            writer.finalize_to(norm)
        except StoreError as e:
            writer.abort()
            return store_error_response(e)
        return json({"key": norm, "size": writer.size, **ctx.describe(norm, writer.size)}, status=status)

    @internal.post("/objects", stream=True)
    @require_internal_token
    async def create_object(request: Request):
        return await create_object_impl(request, 201)

    @internal.post("/objects/upload", stream=True)
    @require_internal_token
    @deprecated("/internal/objects")
    async def legacy_create_object(request: Request):
        """兼容别名（已废弃）：路径里不该出现动作。"""
        return await create_object_impl(request, 200)

    @internal.route("/objects/<key:path>", methods=["GET", "HEAD"])
    @require_internal_token
    async def read_object(request: Request, key: str):
        key = decode_param(key)
        path = svc.store.exists(key)
        if not path:
            return json({"error": "对象不存在"}, status=404)
        if request.method == "HEAD":
            return svc.head(path)
        return await svc.send(
            path,
            range_header=request.headers.get("range"),
            inline=request.args.get("inline") in ("1", "true"),
        )

    @internal.delete("/objects/<key:path>")
    @require_internal_token
    async def delete_object(request: Request, key: str):
        """幂等：对象不存在也回 200，``deleted`` 告知是否真的删掉了。"""
        key = decode_param(key)
        try:
            ctx.check_generic_write(svc.store.normalize(key))
        except StoreError as e:
            return store_error_response(e)
        return json({"key": key, "deleted": svc.store.delete(key)})

    # ==================== 分片会话 ====================

    @internal.post("/uploads")
    @require_internal_token
    async def create_upload(request: Request):
        body = request.json or {}
        key = str(body.get("key") or "").strip()
        if not key:
            return json({"error": "缺少 key"}, status=400)
        try:
            ctx.check_generic_write(svc.store.normalize(key))
            session = svc.chunked.create(key, parse_total(body))
        except StoreError as e:
            return store_error_response(e)
        return json({**session, **ctx.describe(session["key"], 0)}, status=201)

    @internal.get("/uploads/<upload_id:str>")
    @require_internal_token
    async def read_upload(request: Request, upload_id: str):
        try:
            session = svc.chunked.get(upload_id)
        except StoreError as e:
            return store_error_response(e)
        if session is None:
            return json({"error": "分片会话不存在或已过期"}, status=404)
        return with_offset(session, session["offset"])

    @internal.patch("/uploads/<upload_id:str>", stream=True)
    @require_internal_token
    async def append_upload(request: Request, upload_id: str):
        return await append_chunk(request, svc.chunked, upload_id)

    @internal.post("/uploads/<upload_id:str>/complete")
    @require_internal_token
    async def complete_upload(request: Request, upload_id: str):
        try:
            _, size, key = svc.chunked.complete(upload_id)
        except StoreError as e:
            return store_error_response(e)
        return json({"key": key, "size": size, **ctx.describe(key, size)})

    @internal.delete("/uploads/<upload_id:str>")
    @require_internal_token
    async def delete_upload(request: Request, upload_id: str):
        try:
            svc.chunked.abort(upload_id)
        except StoreError as e:
            return store_error_response(e)
        return json({"ok": True})

    # ==================== 签名 ====================

    @internal.post("/signatures")
    @require_internal_token
    async def create_signature(request: Request):
        """签发限时下载签名。可在对象上传前预签——校验只比对对象键与有效期。"""
        body = request.json or {}
        try:
            key = svc.store.normalize(str(body.get("key") or ""))
        except StoreError as e:
            return store_error_response(e)
        ttl = parse_ttl(body)
        sign = svc.sign(key, ttl)
        return json(
            {
                "key": key,
                "sign": sign,
                "url": f"/public/objects/{key}?sign={sign}",
                "ttl_seconds": ttl if ttl is not None else settings.STORAGE_SIGN_TTL_SECONDS,
            }
        )

    @public.route("/objects/<key:path>", methods=["GET", "HEAD"])
    async def download_signed(request: Request, key: str):
        key = decode_param(key)
        try:
            norm = svc.store.normalize(key)
        except StoreError:
            return json({"error": "对象不存在"}, status=404)
        if not svc.verify_sign(request.args.get("sign", ""), norm):
            return json({"error": "对象不存在"}, status=404)
        path = svc.store.exists(norm)
        if not path:
            return json({"error": "对象不存在"}, status=404)
        if request.method == "HEAD":
            return svc.head(path)
        return await svc.send(
            path,
            download_name=request.args.get("name") or None,
            range_header=request.headers.get("range"),
            inline=request.args.get("inline") in ("1", "true"),
        )
