"""签名下载：不需要登录，靠限时签名授权。

用途：分享链接、第三方回源、前端直接把 URL 塞进 ``<img>``/``<video>``——
这些场景带不了 Authorization 头，只能把授权信息放进 URL。

安全性由 ``webx.lib.signer`` 保证：签名是密文（看不出对象键与到期时间）、
自带有效期、且**与对象键绑定**（一个签名挪不到别的文件上）。
"""

from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

from webx.i18n import get_lang_from_header, t
from webx.services.storage_service import storage_service

public_file_bp = Blueprint("webx_public_file", url_prefix="/public/files")


@public_file_bp.get("/<bucket:str>/<filename:str>")
async def download_signed(request: Request, bucket: str, filename: str):
    """凭 ``?sign=`` 下载（流式 + 断点续传）。

    签名无效一律回 **404 而非 403**：403 等于告诉对方"这个文件存在，只是你没权限"，
    对公开端点是不必要的信息泄露。
    """
    lang = get_lang_from_header(request.headers)
    key = f"{bucket}/{filename}"

    if not storage_service.verify_sign(request.args.get("sign", ""), key):
        return json({"error": t("not_found", lang)}, status=404)

    path = storage_service.store.exists(key)
    if not path:
        return json({"error": t("not_found", lang)}, status=404)

    return await storage_service.send(
        path,
        download_name=request.args.get("name") or None,
        range_header=request.headers.get("range"),
        inline=request.args.get("inline") in ("1", "true"),
    )


@public_file_bp.head("/<bucket:str>/<filename:str>")
async def head_signed(request: Request, bucket: str, filename: str):
    """签名链接的 HEAD 探测。签名无效同样回 404。"""
    key = f"{bucket}/{filename}"
    if not storage_service.verify_sign(request.args.get("sign", ""), key):
        return json({}, status=404)
    path = storage_service.store.exists(key)
    if not path:
        return json({}, status=404)
    return storage_service.head(path)
