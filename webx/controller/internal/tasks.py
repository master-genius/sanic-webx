"""worker 回调接口 ``/internal/tasks/<id>/...``。

协议全文见 ``docs/worker-protocol.md`` §4。整组由 ``__mid__.py`` 挂 ``internal_auth_middleware``
（``X-Internal-Token``）；写端点再叠加 ``fence_lease``——令牌 ``(worker_id, lease_seq)`` 不是当前租户一律 403。

  GET    /internal/tasks/<id>                  任务信息（启动恢复协商状态用）
  POST   /internal/tasks/<id>/ack              认领后的即时确认（短租约 → 常规租约）
  POST   /internal/tasks/<id>/heartbeats       心跳（续租；被撤销回 200 + status=revoked）
  PUT    /internal/tasks/<id>/status           状态上报
  POST   /internal/tasks/<id>/complete         完成（只触发迁移）
  POST   /internal/tasks/<id>/fail             失败
  DELETE /internal/tasks/<id>/lease            主动归还租约
  PATCH  /internal/tasks/<id>/steps            步骤进度
  PATCH  /internal/tasks/<id>/fields           result_path / log_path（?mode=overwrite|merge）
  POST   /internal/tasks/<id>/deliveries       产物交付（刻意不做 fencing）
  GET    /internal/tasks/<id>/files[/<name>]   输入文件
"""

from __future__ import annotations

import json as _json
import logging

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import validate

from webx.i18n import get_lang_from_header, t
from webx.lib.task_poke import poke_task_server
from webx.models.task import TaskStatus
from webx.models.task_dtos import (
    StepStatusUpdateRequest,
    TaskFailRequest,
    TaskFieldUpdateRequest,
    TaskStatusUpdateRequest,
)
from webx.services.lease import (
    REASON_BAD_SEQ,
    REASON_NOT_LESSEE,
    extract_fencing,
    fence_lease,
    release_lease_fenced,
    renew_lease,
)
from webx.services.storage_http import decode_param
from webx.services.storage_service import storage_service
from webx.services.task import parse_files, task_service

logger = logging.getLogger(__name__)

internal_task_bp = Blueprint("webx_internal_task", url_prefix="/internal/tasks")


async def _task_or_404(request: Request, task_id: str):
    lang = get_lang_from_header(request.headers)
    task = await task_service.get_task(task_id)
    if not task or task.is_delete:  # 已软删的不返回：否则上传进程会白传已删任务的产物
        return None, json({"error": t("task_not_found", lang), "reason": "gone"}, status=404)
    return task, None


@internal_task_bp.get("/<task_id:str>")
async def get_task_info(request: Request, task_id: str):
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    return json({
        "id": task.id, "status": task.status.value, "task_type": task.task_type,
        "lease_seq": task.lease_seq, "worker_id": task.worker_id,
        "sub_status": task.sub_status.value, "sub_step": task.sub_step,
    })


@internal_task_bp.post("/<task_id:str>/ack")
async def ack_task(request: Request, task_id: str):
    """认领后的即时确认：把短租约续成常规租约。兜住"领取响应丢在路上"。

    带了 worker_id 却没带（或带坏了）lease_seq：**必须响亮拒绝**，不能沉默地当作"无令牌"放行——
    那会把一个配置 / 序列化 bug 伪装成正常。
    """
    worker_id, seq = extract_fencing(request)
    if worker_id and seq is None:
        return json({"ok": False, "reason": REASON_BAD_SEQ}, status=403)
    if not worker_id:
        return json({"ok": False, "reason": REASON_NOT_LESSEE}, status=403)
    renewed = await renew_lease(task_id, worker_id, seq)
    if renewed is None:
        # 已不是租户：多半是领取响应在途时租约就到期回池了，任务可能已归别人。worker 应放弃本次执行
        logger.info("[fence] ack 被拒 task_id=%s caller=%s/%s", task_id, worker_id, seq)
        return json({"ok": False, "reason": REASON_NOT_LESSEE}, status=403)
    return json({"ok": True, "lease_until": renewed.get("lease_until")})


@internal_task_bp.post("/<task_id:str>/heartbeats")
async def create_heartbeat(request: Request, task_id: str):
    """心跳（续租）。

    **与其它写端点不同：被撤销时回 200 而非 403。** worker 的取消判据是 ``status != "running"``，
    只要回一个非 running 的 status，被撤销租约的 worker 会在下一跳自行终止子进程，无需额外分支。
    ``lease_until`` 是承重项：grace 随已投入时长变化，worker 无法自行推算，必须由服务端每跳告知——
    否则自熔时刻会漂到服务端回收之后，出现"服务端已租给 B、A 仍认为自己持有"的窗口。
    """
    task = await task_service.get_task(task_id)
    if not task or task.is_delete:
        return json({"ok": False, "reason": "gone"}, status=404)

    worker_id, seq = extract_fencing(request)
    revoked = json({"ok": False, "status": "revoked", "reason": REASON_NOT_LESSEE})
    if not worker_id or seq is None or (task.worker_id or "") != worker_id or task.lease_seq != seq:
        logger.info("[fence] 心跳吊销 task_id=%s holder=%s/%s caller=%s/%s",
                    task_id, task.worker_id, task.lease_seq, worker_id, seq)
        return revoked

    renewed = await renew_lease(task_id, worker_id, seq)
    if renewed is None:
        return json({"ok": True, "status": task.status.value})  # 读到写之间状态被改走（如刚被取消）：按当前状态答复
    return json({"ok": True, "status": task.status.value, "lease_until": renewed.get("lease_until")})


@internal_task_bp.put("/<task_id:str>/status")
@fence_lease
@validate(json=TaskStatusUpdateRequest)
async def put_status(request: Request, task_id: str, body: TaskStatusUpdateRequest):
    lang = get_lang_from_header(request.headers)
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    try:
        new_status = TaskStatus(body.status)
    except ValueError:
        return json({"error": t("task_status_invalid", lang)}, status=400)
    try:
        await task_service.update_status(task, new_status, error_message=body.error_message)
    except ValueError as e:
        return json({"error": str(e)}, status=400)
    return json({"message": t("success", lang)})


@internal_task_bp.post("/<task_id:str>/complete")
@fence_lease
async def complete_task(request: Request, task_id: str):
    lang = get_lang_from_header(request.headers)
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    await task_service.complete_task(task)
    return json({"message": t("success", lang)})


@internal_task_bp.post("/<task_id:str>/fail")
@fence_lease
@validate(json=TaskFailRequest)
async def fail_task(request: Request, task_id: str, body: TaskFailRequest):
    lang = get_lang_from_header(request.headers)
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    await task_service.fail_task(task, body.error_message)
    return json({"message": t("success", lang)})


@internal_task_bp.delete("/<task_id:str>/lease")
async def release_lease(request: Request, task_id: str):
    """主动归还租约——worker 优雅退出 / 重启恢复时调用，任务立即回 PENDING 池。

    **必须带 fencing**：一台已经丢了租约、正在关闭的 worker 若无条件归还，会把别人的租约退掉。
    fencing 正是"别让过期的 fail 信号覆盖重投后正在执行的工作"这个老顾虑的正解——
    它从"必须靠沉默规避"变成"由机制保证"。
    """
    worker_id, seq = extract_fencing(request)
    if not worker_id or seq is None:
        return json({"ok": False, "reason": "missing_fencing_token"}, status=403)
    if not await release_lease_fenced(task_id, worker_id, seq):
        # 已不是租户：多半是租约早已到期、任务被别人接管。什么都不做才是对的
        return json({"ok": False, "reason": REASON_NOT_LESSEE}, status=403)
    logger.info("[lease] %s 主动归还租约 task_id=%s → 回 PENDING 池", worker_id, task_id)
    await poke_task_server()
    return json({"ok": True})


@internal_task_bp.patch("/<task_id:str>/steps")
@fence_lease
@validate(json=StepStatusUpdateRequest)
async def patch_steps(request: Request, task_id: str, body: StepStatusUpdateRequest):
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    try:
        await task_service.update_step_status(
            task, steps=body.steps, sub_step=body.sub_step,
            sub_status=body.sub_status, step_message=body.step_message,
        )
    except ValueError as e:
        return json({"error": str(e)}, status=400)
    fresh = await task_service.get_task(task_id)
    return json({
        "ok": True, "steps": _json.loads(fresh.steps or "[]"),
        "sub_step": fresh.sub_step, "sub_status": fresh.sub_status.value,
    })


@internal_task_bp.patch("/<task_id:str>/fields")
@fence_lease
@validate(json=TaskFieldUpdateRequest)
async def patch_fields(request: Request, task_id: str, body: TaskFieldUpdateRequest):
    """更新 result_path / log_path。``?mode=overwrite``（默认）替换；``merge`` 按 key 深度合并。不改变任务状态。"""
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    mode = request.args.get("mode", "overwrite")
    if mode not in ("overwrite", "merge"):
        return json({"error": "mode 必须为 overwrite 或 merge"}, status=400)
    fields = {k: v for k, v in (("result_path", body.result_path), ("log_path", body.log_path)) if v is not None}
    if not fields:
        return json({"task_id": task_id, "message": "无字段需更新"})
    updated = await task_service.update_fields_with_mode(task, fields, mode=mode)
    return json({"task_id": task_id, "result_path": updated.get("result_path", task.result_path),
                 "log_path": updated.get("log_path", task.log_path)})


@internal_task_bp.post("/<task_id:str>/deliveries")
@validate(json=TaskFieldUpdateRequest)
async def create_delivery(request: Request, task_id: str, body: TaskFieldUpdateRequest):
    """产物交付——给长驻的独立上传进程用。**刻意不做 fencing。**

    语义：把一个**已经传上去的**产物地址，按 ``result_path`` 的嵌套 key 幂等 merge 进去。
    与 ``PATCH .../fields?mode=merge`` 的写入行为逐字节相同，唯一的差别是**授权口径**。

    为什么不做租户比对（不是放松，是口径本来就不同）：fencing 回答"谁有权推进这个任务的状态机"；
    本端点回答的是"把一份产物的地址填进它自己那一格"。在这条路上做租户比对，除了"任务不存在"之外
    每一种拒绝都是误判——多阶段推进后 lease_seq 早已 +1、甚至换了 worker，终态之后产物照样要交付；
    而上传进程是长驻的，一辈子服务无数个任务，手里永远不可能有 per-task 的租约令牌。
    条目写的是带步骤序号的叶子 key，不碰当前轮的任何字段。
    判据只有两条：任务不存在 / 已软删 → 404（上传进程据此**丢弃**条目，不留僵尸）；其余 → merge 写入。
    ``log_path`` 不在本端点职责内：日志被上一轮覆盖正是 ``prune_log_path`` 在防的事，那条路走严格 fencing。
    """
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    if body.result_path is None:
        return json({"task_id": task_id, "message": "无字段需更新"})
    updated = await task_service.update_fields_with_mode(task, {"result_path": body.result_path}, mode="merge")
    return json({"task_id": task_id, "result_path": updated.get("result_path", task.result_path)})


@internal_task_bp.get("/<task_id:str>/files")
async def list_task_files(request: Request, task_id: str):
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    return json({"task_id": task.id, "files": parse_files(task.files_json)})


@internal_task_bp.get("/<task_id:str>/files/<name:path>")
async def get_task_file(request: Request, task_id: str, name: str):
    lang = get_lang_from_header(request.headers)
    task, err = await _task_or_404(request, task_id)
    if err:
        return err
    path = storage_service.store.exists(f"tasks/{task.id}/files/{decode_param(name)}")
    if not path:
        return json({"error": t("task_file_not_found", lang)}, status=404)
    return await storage_service.send(path, range_header=request.headers.get("range"))

