"""用户侧任务接口 ``/api/tasks``。

全部要求登录（由本目录 ``__mid__.py`` 保证），身份一律取自令牌的 ``user_id``，**数据按用户隔离**：
非本人的任务一律 403（列表 / 批量查询则静默跳过）。

路径只放资源名词，动作由 HTTP 方法表达；只有无法映射成 CRUD 的状态迁移才把动词放子路径
（``submit`` / ``cancel`` / ``retry`` / ``revise`` / ``redo``）。详见 ``docs/api-conventions.md``。

  POST   /api/tasks                        创建（可选立即提交）
  GET    /api/tasks                        列表（分页；``ids=a,b`` 为轻量状态轮询）
  GET    /api/tasks/<id>                   详情（内联子任务）
  GET    /api/tasks/<id>/children          子任务
  PATCH  /api/tasks/<id>                   编辑 / 推进下一步（可选提交）
  PUT    /api/tasks/<id>/info              辅助信息（任意状态可改）
  DELETE /api/tasks/<id>                   删除
  POST   /api/tasks/<id>/{submit,cancel,retry,revise,redo}
  POST|GET|DELETE /api/tasks/<id>/files[/<name>]   输入文件
"""

from __future__ import annotations

import json as jsonlib
import logging

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import validate

from webx.i18n import get_lang_from_header, t
from webx.lib.storage import StoreError
from webx.lib.task_types import StepValidationError, validate_task_params
from webx.models.task import Task, TaskStatus
from webx.models.task_dtos import (
    TaskCreateRequest,
    TaskInfoUpdateRequest,
    TaskListResponse,
    TaskPatchRequest,
    TaskRedoRequest,
    TaskResponse,
    TaskRetryRequest,
    TaskStatusBrief,
    TaskSubmitRequest,
)
from webx.services.quota import is_task_active
from webx.services.storage_http import decode_param, receive_multipart, store_error_response
from webx.services.storage_service import storage_service
from webx.services.task import parse_files, parse_params, task_service

logger = logging.getLogger(__name__)

task_bp = Blueprint("webx_task", url_prefix="/api/tasks")

#: 批量状态查询单次上限
STATUS_BATCH_LIMIT = 100
MAX_PAGE_SIZE = 100
MAX_PAGE = 1_000_000


# ==================== 公共小工具 ====================


def _user_id(request: Request) -> str:
    user = getattr(request.ctx, "user", None) or {}
    return user.get("user_id") or ""


async def _owned(request: Request, task_id: str):
    """取任务并校验归属。返回 ``(task, None)`` 或 ``(None, 错误响应)``。"""
    lang = get_lang_from_header(request.headers)
    task = await task_service.get_task(task_id)
    if not task or task.is_delete:
        return None, json({"error": t("task_not_found", lang)}, status=404)
    uid = _user_id(request)
    # 令牌没有身份、或任务没有 owner，都不能“放行”：空串不是通配符
    if not uid or task.user_id != uid:
        return None, json({"error": t("unauthorized_task", lang)}, status=403)
    return task, None


def _view(task: Task) -> dict:
    return TaskResponse.model_validate(task).model_dump(mode="json")


def _bad(lang: str, exc: Exception, status: int = 400):
    if isinstance(exc, StepValidationError):
        return json({"error": t(exc.code, lang, **exc.context), "code": exc.code}, status=status)
    return json({"error": str(exc)}, status=status)


def _reject_child(task: Task, lang: str):
    """子任务的提交 / 编辑 / 改写 / 重做都由系统（准入闸门）管理。

    用户侧直接 submit_task 会绕过子任务独立的额度池与重试预算（逐个 revise + submit 就能把全部失败的子任务
    一起塞进池里）；唯一的用户入口是 retry，它走 readmit_child 挂回准入闸。
    """
    if task.parent_id:
        return json({"error": t("child_managed_by_system", lang)}, status=409)
    return None


def _guard_auto_post(task: Task, lang: str):
    """处于"待自动准入"队列的任务由补偿器择机投递，用户不得手动操作——否则绕过节流并遗留 auto_post 脏标记。"""
    if task.auto_post == 1:
        key = "child_auto_post_queued" if task.parent_id else "parent_waiting_children"
        return json({"error": t(key, lang)}, status=409)
    return None


# ==================== 创建 / 查询 ====================


@task_bp.post("")
@validate(json=TaskCreateRequest)
async def create_task(request: Request, body: TaskCreateRequest):
    lang = get_lang_from_header(request.headers)
    user = request.ctx.user
    task = await task_service.create_task(
        body.task_type,
        files=body.files,
        params_text=body.params_text,
        user_id=user.get("user_id"),
        org_id=user.get("org_id") or "",
        name=body.name,
    )
    if body.submit:
        try:
            submitted = await task_service.submit_task(task)
        except BaseException:
            # 前置闸（额度 / 参数 / 点数）拒绝时，刚创建的任务不能留成占每日额度、又无从找回 id 的孤儿
            await task_service.delete_task(task)
            raise
        if not submitted:
            return json({"error": t("service_unavailable", lang), "data": _view(task)}, status=503)
        task = await task_service.get_task(task.id)
    return json({"message": t("success", lang), "data": _view(task)}, status=201)


@task_bp.get("")
async def list_tasks(request: Request):
    """任务列表（仅本人、仅顶层；子任务只在父任务详情里出现）。

    Query: ``page`` ``page_size``（≤100） ``status`` ``task_type``（逗号分隔多值）；
    ``ids=a,b,c``（≤100）切换为**轻量状态轮询**，返回精简视图，不分页。
    """
    lang = get_lang_from_header(request.headers)
    uid = _user_id(request)

    # 查询串里的 NUL（%00）：PostgreSQL 文本列拒收，到查询阶段才炸成 500
    if any("\x00" in v for vals in request.args.values() for v in vals):
        return json({"error": t("invalid_params", lang)}, status=400)

    ids_raw = request.args.get("ids")
    if ids_raw is not None:
        ids = [i for i in dict.fromkeys(x.strip() for x in ids_raw.split(",")) if i]
        if len(ids) > STATUS_BATCH_LIMIT:
            return json({"error": t("batch_limit_exceeded", lang, limit=STATUS_BATCH_LIMIT)}, status=400)
        tasks = await task_service.get_tasks_by_ids(ids, user_id=uid) if ids else []
        return json({"data": [TaskStatusBrief.model_validate(x).model_dump(mode="json") for x in tasks]})

    try:
        # page 设上限：OFFSET 超过 int64 会让 PostgreSQL 报错（500）
        page = min(MAX_PAGE, max(1, int(request.args.get("page", "1"))))
        page_size = min(MAX_PAGE_SIZE, max(1, int(request.args.get("page_size", "20"))))
    except ValueError:
        return json({"error": t("invalid_params", lang)}, status=400)

    filters: dict = {"parent_id": "", "is_delete": False, "user_id": uid}
    if status := request.args.get("status"):
        try:
            filters["status"] = TaskStatus(status)
        except ValueError:
            return json({"error": t("task_status_invalid", lang)}, status=400)
    if task_type := request.args.get("task_type"):
        types = [v.strip() for v in task_type.split(",") if v.strip()]
        if len(types) > 1:
            filters["task_type__in"] = types  # 一次 __in 查回；前端发多个请求再合并的话分页与 total 都对不上
        elif types:
            filters["task_type"] = types[0]

    tasks = await Task.filter(**filters).order_by("-created_at").limit(page_size).offset((page - 1) * page_size)
    total = await Task.filter(**filters).count()
    return json(TaskListResponse(
        items=[TaskResponse.model_validate(x).model_dump(mode="json") for x in tasks],
        total=total, page=page, page_size=page_size,
    ).model_dump(mode="json"))


@task_bp.get("/<task_id:str>")
async def get_task(request: Request, task_id: str):
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    data = _view(task)
    if not task.parent_id:
        data["children"] = [_view(c) for c in await task_service.get_children(task.id)]
    return json({"data": data})


@task_bp.get("/<task_id:str>/children")
async def list_children(request: Request, task_id: str):
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    step = request.args.get("parent_step")
    if step and "\x00" in step:
        return json({"error": t("invalid_params", get_lang_from_header(request.headers))}, status=400)
    children = await task_service.get_children(task.id, parent_step=step)
    return json({"data": [_view(c) for c in children]})


# ==================== 编辑 / 提交 ====================


@task_bp.patch("/<task_id:str>")
@validate(json=TaskPatchRequest)
async def patch_task(request: Request, task_id: str, body: TaskPatchRequest):
    """编辑 / 推进任务。

    - 草稿（含 none）：合并 ``params_text``、可改 ``step``；``submit=true``（默认）则同时提交
    - 多阶段、running+success+sub_step=""（等用户追加参数）/ 草稿窗口（含取消后的编辑窗口）：
      必须带 ``step``，提交下一步还必须带 ``params_text``；``submit=false`` 只存草稿
    """
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if guard := _reject_child(task, lang):
        return guard
    if guard := _guard_auto_post(task, lang):
        return guard
    case = task_service.editable_case(task)
    if case is None:
        return json({"error": t("task_status_no_patch", lang, status=task.status,
                                sub_status=task.sub_status, sub_step=task.sub_step)}, status=400)
    try:
        updated = await task_service.patch_task(task, params=body.params_text, step=body.step, submit=body.submit)
    except RuntimeError:
        return json({"error": t("service_unavailable", lang)}, status=503)
    except ValueError as e:  # 含 StepValidationError
        return _bad(lang, e)
    return json({"message": t("task_updated", lang), "data": _view(updated)})


@task_bp.put("/<task_id:str>/info")
@validate(json=TaskInfoUpdateRequest)
async def update_task_info(request: Request, task_id: str, body: TaskInfoUpdateRequest):
    """与计算无关的辅助信息；**不做任何任务状态判断**，任意状态均可更新。"""
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if body.name is None:
        return json({"error": t("invalid_params", lang)}, status=400)
    task.name = body.name
    await task.save(update_fields=["name", "updated_at"])
    return json({"message": t("task_updated", lang), "data": _view(task)})


@task_bp.post("/<task_id:str>/submit")
@validate(json=TaskSubmitRequest)
async def submit_task(request: Request, task_id: str, body: TaskSubmitRequest):
    """提交草稿：``none`` / ``draft`` → ``pending``。已投递 / 运行 / 终态一律拒绝（避免重复投递）。"""
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if guard := _reject_child(task, lang):
        return guard
    if task.status not in (TaskStatus.NONE, TaskStatus.DRAFT, ""):
        return json({"error": t("submit_only_draft", lang, status=task.status)}, status=400)
    if guard := _guard_auto_post(task, lang):
        return guard

    if body.params_text is not None:
        # 整体覆盖（非合并）。入口闸必须在落库之前：校验通过才存，非法参数不进库
        validate_task_params(task.task_type, parse_params(body.params_text))
        task.params_text = body.params_text
        await task.save(update_fields=["params_text", "updated_at"])

    if not await task_service.submit_task(task):
        return json({"error": t("service_unavailable", lang)}, status=503)
    fresh = await task_service.get_task(task.id)
    return json({"message": t("task_submitted", lang),
                 "data": {"task_id": fresh.id, "status": fresh.status.value, "submitted": True}})


# ==================== 取消 / 重试 / 改写 / 重做 / 删除 ====================


@task_bp.post("/<task_id:str>/cancel")
async def cancel_task(request: Request, task_id: str):
    """取消任务。仅 pending / queued / running 且确有计算在进行 / 待投时才取消。"""
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if task.status not in (TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING):
        return json({"error": t("cancel_status_invalid", lang, status=task.status)}, status=400)
    if not await task_service.cancel_task(task):
        # 竞争 / 态已变：步骤刚完成或任务已终结 → 无正在进行的计算，无需取消
        return json({"error": t("cancel_not_applicable", lang)}, status=409)
    return json({"message": t("task_cancelled", lang), "data": {"task_id": task.id, "status": "cancelled"}})


@task_bp.post("/<task_id:str>/retry")
@validate(json=TaskRetryRequest)
async def retry_task(request: Request, task_id: str, body: TaskRetryRequest):
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if task.parent_id:
        # 子任务不得绕过准入闸直接重投：挂回准入闸门，额度内即时直投、超额则留队
        if task.status not in (TaskStatus.FAILED, TaskStatus.CANCELLED):
            return json({"error": t("cancel_status_invalid", lang, status=task.status)}, status=400)
        return json({"data": _view(await task_service.readmit_child(task))})
    try:
        updated = await task_service.retry_task(task, params_text=body.params_text, step=body.step)
    except ValueError as e:
        return _bad(lang, e)
    return json({"message": t("success", lang), "data": _view(updated)})


@task_bp.post("/<task_id:str>/revise")
async def revise_task(request: Request, task_id: str):
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if guard := _reject_child(task, lang):
        return guard
    try:
        updated = await task_service.revise_task(task)
    except ValueError as e:
        return _bad(lang, e)
    return json({"message": t("success", lang), "data": _view(updated)})


@task_bp.post("/<task_id:str>/redo")
@validate(json=TaskRedoRequest)
async def redo_task(request: Request, task_id: str, body: TaskRedoRequest):
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if guard := _reject_child(task, lang):
        return guard
    try:
        updated = await task_service.redo_task(task, body.step, params_text=body.params_text)
    except ValueError as e:
        return _bad(lang, e)
    return json({"message": t("success", lang), "data": _view(updated)})


@task_bp.delete("/<task_id:str>")
async def delete_task(request: Request, task_id: str):
    """删除任务。

    条件 = 任务**非在飞**（不占用 worker 名额，与并发配额同口径 ``is_task_active``）：终态、草稿 / 未提交、
    多阶段停在 running 但空闲（等用户追加参数 / 存草稿）都可删；在飞的拒绝，避免悬空回调。
    子任务不开放直接删除（接口策略；内部的级联删除不受影响）。
    """
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if task.parent_id:
        return json({"error": t("child_managed_by_system", lang)}, status=409)
    if is_task_active(task):
        return json({"error": t("delete_active_forbidden", lang)}, status=400)
    await task_service.delete_task(task)
    return json({"message": t("success", lang)})


# ==================== 输入文件 ====================
#
# 输入文件存在 ``tasks/<id>/files/<name>``，由 worker 经 ``/internal/tasks/<id>/files`` 下载。
# 只在任务**可编辑**（草稿 / none）时允许改动：已投递的任务，输入变了而 worker 已拿走旧的，会静默跑错。


def _files_key(task_id: str, name: str = "") -> str:
    return f"tasks/{task_id}/files/{name}" if name else f"tasks/{task_id}/files"


def _editable_inputs(task: Task) -> bool:
    return task.status in (TaskStatus.NONE, TaskStatus.DRAFT, "") and task.auto_post != 1


async def _update_input_files(task_id: str, mutate, *, to_draft: bool = False) -> bool:
    """在行锁内重读任务、确认仍可编辑，再改 files_json。返回是否成功。

    读-改-写不加锁时，并发上传会互相覆盖（丢条目）；而且上传是流式的、窗口很宽——上传期间用户提交了任务，
    结束时又用上传开始时的旧状态把它写回 DRAFT，任务就从池里消失了。
    """
    from tortoise.transactions import in_transaction

    async with in_transaction():
        row = await Task.filter(id=task_id).select_for_update().first()
        if row is None or not _editable_inputs(row):
            return False
        data = {"files_json": jsonlib.dumps(mutate(parse_files(row.files_json)), ensure_ascii=False)}
        if to_draft and row.status in (TaskStatus.NONE, ""):
            data["status"] = TaskStatus.DRAFT
        await Task.filter(id=task_id).update(**data)
    return True


@task_bp.post("/<task_id:str>/files", stream=True)
async def upload_file(request: Request, task_id: str):
    """上传一个输入文件（multipart，流式落盘）。同名覆盖。"""
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        # 流式请求：未读的请求体要排空，否则连接状态混乱
        return err
    if not _editable_inputs(task):
        return json({"error": t("task_file_forbidden", lang)}, status=409)

    writer = None
    try:
        writer = storage_service.store.open_writer(_files_key(task.id))
        parsed = await receive_multipart(request, writer)
        filename = (parsed.fields.get("filename") or parsed.filename or "").strip()
        final = writer.finalize(filename)
    except StoreError as e:
        if writer is not None:
            writer.abort()          # 文件名非法等：tmp 要清掉，否则每次最多泄漏 UPLOAD_MAX_SIZE
        return store_error_response(e)

    entry = {"name": final.name, "size": writer.size}

    def add(files: list) -> list:
        return [f for f in files if f.get("name") != final.name] + [entry]

    if not await _update_input_files(task.id, add, to_draft=True):
        # 上传期间任务已被提交 / 开始运行：输入不能再变（worker 可能已拿走旧的）
        storage_service.store.delete(_files_key(task.id, final.name))
        return json({"error": t("task_file_forbidden", lang)}, status=409)
    return json({"message": t("success", lang), "data": entry}, status=201)


@task_bp.get("/<task_id:str>/files")
async def list_files(request: Request, task_id: str):
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    return json({"data": parse_files(task.files_json)})


@task_bp.get("/<task_id:str>/files/<name:path>")
async def download_file(request: Request, task_id: str, name: str):
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    path = storage_service.store.exists(_files_key(task.id, decode_param(name)))
    if not path:
        return json({"error": t("task_file_not_found", lang)}, status=404)
    return await storage_service.send(path, range_header=request.headers.get("range"))


@task_bp.delete("/<task_id:str>/files/<name:path>")
async def delete_file(request: Request, task_id: str, name: str):
    lang = get_lang_from_header(request.headers)
    task, err = await _owned(request, decode_param(task_id))
    if err:
        return err
    if not _editable_inputs(task):
        return json({"error": t("task_file_forbidden", lang)}, status=409)
    name = decode_param(name)
    files = parse_files(task.files_json)
    if not any(f.get("name") == name for f in files):
        return json({"error": t("task_file_not_found", lang)}, status=404)
    if not await _update_input_files(task.id, lambda fs: [f for f in fs if f.get("name") != name]):
        return json({"error": t("task_file_forbidden", lang)}, status=409)
    storage_service.store.delete(_files_key(task.id, name))
    return json({"message": t("success", lang)})

