"""笔记接口：本目录已挂认证中间件，此处直接读 request.ctx.user。"""

import time

from sanic import Blueprint
from sanic.response import json

from webx.core.errors import NotFoundError
from webx.i18n import t

from app.models import Note

bp = Blueprint("note", url_prefix="/api/notes")


@bp.get("")
async def list_notes(request):
    rows = await Note.filter(user_id=request.ctx.user["user_id"]).values(
        "id", "title", "create_time"
    )
    return json({"items": rows, "total": len(rows)})


@bp.post("")
async def create_note(request):
    body = request.json or {}
    note = await Note.create(
        user_id=request.ctx.user["user_id"],
        title=str(body.get("title") or "")[:200],
        content=str(body.get("content") or ""),
        create_time=int(time.time() * 1000),
    )
    return json({"id": note.id, "message": t("note_created", request.ctx.lang)})


@bp.get("/<note_id:str>")
async def get_note(request, note_id: str):
    note = await Note.filter(id=note_id, user_id=request.ctx.user["user_id"]).first()
    if not note:
        # 抛出即可：create_app 已注册统一处理器，按 status/code 渲染响应
        raise NotFoundError(code="note_not_found")
    return json({"id": note.id, "title": note.title, "content": note.content})
