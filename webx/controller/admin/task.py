"""管理端：任务监控。

中间件由 ``admin/__mid__.py`` 统一声明（管理员认证 + 权限加载），此处不再重复列举。
需要 ``list-task`` 权限（组织超管 / super 角色自动放行）。

  GET /api/admin/tasks            任务列表（分页，可按状态 / 类型 / 用户过滤）
  GET /api/admin/tasks/summary    各状态任务数
"""

from __future__ import annotations

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

from webx.i18n import get_lang_from_header, t
from webx.middleware import has_perm
from webx.models.task import Task, TaskStatus

admin_task_bp = Blueprint("webx_admin_task", url_prefix="/api/admin/tasks")

MAX_PAGE_SIZE = 200


def _forbidden():
    return json({"error": "权限不足"}, status=403)


@admin_task_bp.get("")
async def list_tasks(request: Request):
    """任务列表，按创建时间倒序。含子任务（运维视角要看全貌），不含软删除的。"""
    if not has_perm(request, "list-task"):
        return _forbidden()
    lang = get_lang_from_header(request.headers)
    try:
        page = max(1, int(request.args.get("page", "1")))
        page_size = min(MAX_PAGE_SIZE, max(1, int(request.args.get("page_size", "20"))))
    except ValueError:
        return json({"error": t("invalid_params", lang)}, status=400)

    filters: dict = {"is_delete": False}
    if status := request.args.get("status"):
        try:
            filters["status"] = TaskStatus(status)
        except ValueError:
            return json({"error": t("task_status_invalid", lang)}, status=400)
    if task_type := request.args.get("task_type"):
        filters["task_type"] = task_type
    if user_id := request.args.get("user_id"):
        filters["user_id"] = user_id

    rows = await Task.filter(**filters).order_by("-created_at").limit(page_size).offset((page - 1) * page_size)
    total = await Task.filter(**filters).count()
    return json({"data": {
        "total": total, "page": page, "page_size": page_size,
        "items": [
            {"id": x.id, "status": x.status.value, "task_type": x.task_type, "name": x.name,
             "user_id": x.user_id, "parent_id": x.parent_id, "worker_id": x.worker_id,
             "retry": x.retry, "created_at": x.created_at.isoformat() if x.created_at else None}
            for x in rows
        ],
    }})


@admin_task_bp.get("/summary")
async def summary(request: Request):
    """各状态任务数（不含软删除的）。"""
    if not has_perm(request, "list-task"):
        return _forbidden()
    counts = {s.value: await Task.filter(status=s, is_delete=False).count() for s in TaskStatus}
    return json({"data": {"stats": counts, "total": sum(counts.values())}})
