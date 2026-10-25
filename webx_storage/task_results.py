"""任务产物策略：对象键布局 ``tasks/<task_id>/results/<filename>``。

「任务产物」在这里只是一种**键布局**——一个任务有一组结果文件。存储服务不知道任务是什么，
也不关心任务类型；与具体计算任务相关的东西（例如某类任务的下载文件名拼法）通过
覆盖 ``download_name`` 注入，不在本策略里。

规范路径（路径只放资源名词，动作由 HTTP 方法表达）::

    POST   /internal/tasks/<task_id>/results                创建结果文件（multipart）
    POST   /internal/tasks/<task_id>/results/uploads        创建分片上传会话 {filename, total?}
           （会话的追加 / 查进度 / 完成 / 放弃走通用的 /internal/uploads/<id>）
    POST   /internal/tasks/<task_id>/results/deletions      按 URL 批量删除 {urls}
    POST   /internal/tasks/<task_id>/signatures             签发签名 {filename, ttl_seconds?}
    DELETE /internal/tasks/<task_id>                        删除整个任务目录，幂等
    DELETE /internal/tasks/<task_id>/results/<filename>     删除单个结果文件，幂等
    GET    /public/tasks/<task_id>/results                  列出结果文件
    GET    /public/tasks/<task_id>/results/<filename>       下载（默认公开；带 ?sign= 则校验）

兼容别名（**已废弃**，仅为让既有 worker 的上传契约无需改动即可对接；响应带 ``Deprecation`` 头）::

    POST /internal/tasks/<id>/results/upload        → POST   /internal/tasks/<id>/results
    POST /internal/tasks/<id>/results/upload/init   → POST   /internal/tasks/<id>/results/uploads
    POST /internal/tasks/<id>/results/deletes       → POST   /internal/tasks/<id>/results/deletions
    POST /internal/sign                             → POST   /internal/tasks/<id>/signatures
    POST /internal/objects/upload                   → POST   /internal/objects
"""

from __future__ import annotations

import logging
import re
from pathlib import PurePosixPath
from typing import Optional
from urllib.parse import quote, unquote, urlsplit

from sanic.request import Request
from sanic.response import json, text

from webx.config import settings
from webx.lib.storage import StoreError
from webx.services.storage_http import decode_param, json_object, receive_multipart, store_error_response
from webx_storage.auth import require_internal_token
from webx_storage.policy import KeyPolicy, StorageContext, register_policy
from webx_storage.retention import MtimeRetention, RetentionPolicy
from webx_storage.routes import deprecated as _deprecated
from webx_storage.routes import parse_total, parse_ttl

logger = logging.getLogger(__name__)

_TASK_ID = re.compile(r"\A[A-Za-z0-9_-]+\Z")  # \Z 而非 $：后者放行末尾换行

# 非法文件名字符（< > : " / \ | ? * 及控制字符）→ '-'，用于下载名前缀
_ILLEGAL = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

#: 单次批量删除的条目上限：只为挡住"一个请求扫太久"，不是业务约束
MAX_BATCH_DELETE = 500


def sanitize_name(name: Optional[str]) -> str:
    """把调用方传入的任务名净化成可作下载文件名前缀的片段（可能为空串）。"""
    return _ILLEGAL.sub("-", name).strip(" .") if name else ""


class TaskResultsPolicy(KeyPolicy):
    name = "task_results"

    #: 这些后缀的结果按文本内联返回（便于前端原地查看日志）
    inline_suffixes: tuple[str, ...] = (".log",)

    # ==================== 键与 URL ====================

    @staticmethod
    def check_task_id(task_id: str) -> str:
        if not isinstance(task_id, str) or not task_id or not _TASK_ID.match(task_id):
            raise StoreError(f"非法 task_id: {task_id!r}", status=400)
        return task_id

    @staticmethod
    def check_filename(filename: Optional[str]) -> str:
        """文件名必须是**单段**：不含 ``/`` 或 ``\\``（及 NUL），且不是空串 / ``.`` / ``..``。违反即 400。

        这是全服务统一的规则（通用对象的 finalize 也是如此）。早先这里只取末段（``a/b.txt`` 悄悄存成
        ``b.txt``），两处语义不一致，且调用方永远不知道自己的名字被改了——比被拒绝更糟。
        字符集由存储的键规则再挡一道。
        """
        name = filename if isinstance(filename, str) else ""
        if not name or name in (".", "..") or "/" in name or "\\" in name or "\x00" in name:
            raise StoreError(f"非法 filename（不得为空，不得含路径分隔符）: {filename!r}", status=400)
        return name

    def key_for(self, task_id: str, filename: str) -> str:
        return f"tasks/{self.check_task_id(task_id)}/results/{self.check_filename(filename)}"

    @staticmethod
    def download_url(task_id: str, filename: str) -> str:
        # 逐段百分号编码：服务端对路径参数只解码一次（decode_param），所以名字里的 # ? % 必须编码，
        # 否则链接 404，更糟的是 "100%25.txt" 会被解成 "100%.txt" 而取到另一个文件
        return f"/public/tasks/{quote(task_id, safe='')}/results/{quote(filename, safe='')}"

    def owns(self, key: str) -> bool:
        return key == "tasks" or key.startswith("tasks/")

    def describe_object(self, key: str, size: int) -> dict:
        parts = key.split("/", 3)
        if len(parts) == 4 and parts[0] == "tasks" and parts[2] == "results":
            return {
                "task_id": parts[1],
                "filename": parts[3],
                "download_url": self.download_url(parts[1], parts[3]),
            }
        return {}

    @staticmethod
    def parse_url(url) -> Optional[tuple[str, str]]:
        """把落库的下载 URL 反解成 ``(task_id, filename)``；形状不符返回 None。

        布局是存储服务的私有知识，反解也就该由它做——调用方只说"这些条目我不要了"，
        不必在自己那边复刻一份路径布局（两处各存一份，改一处就漂）。

        判据是**末四段** ``tasks/<id>/results/<name>``，前缀（public / internal /
        反代加的路径前缀）一律不参与判断，反代前缀是部署细节。
        不在该布局内的键（如通用对象 ``objects/...``）天然不匹配，于是在解析层就被挡住，
        不需要一条额外的黑名单。
        """
        if not url or not isinstance(url, str):
            return None
        try:
            path = urlsplit(url.strip()).path
        except ValueError:      # 如 "http://[bad"：批量删除要“永不整体失败”，坏条目只标记为不是结果地址
            return None
        segs = [unquote(s) for s in path.split("/") if s]
        if len(segs) < 4 or segs[-4] != "tasks" or segs[-2] != "results":
            return None
        return segs[-3], segs[-1]

    def download_name(self, task_id: str, stored_name: str, requested: Optional[str]) -> str:
        """呈现给用户的下载文件名：``{前缀}_{存储名}``，前缀取净化后的 ``?name=``，
        缺省则用 task_id。任务名可变、真源在调用方，故由调用方传入，这里只做净化与兜底。

        需要别的拼法（例如存储名已自带任务名就不再加前缀）时覆盖本方法。
        """
        prefix = sanitize_name(requested) or task_id
        return f"{prefix}_{PurePosixPath(stored_name).name}"

    def retention(self) -> Optional[RetentionPolicy]:
        return MtimeRetention("tasks", settings.STORAGE_RETENTION_DAYS)

    # ==================== 路由 ====================

    def mount(self, ctx: StorageContext) -> None:
        svc, internal, public = ctx.svc, ctx.internal, ctx.public
        policy = self

        # ---------- 共用实现（规范路径与兼容别名各自包一层，只差状态码）----------

        async def create_result(request: Request, task_id: str, status: int):
            try:
                tid = policy.check_task_id(task_id)
                writer = svc.store.open_writer(f"tasks/{tid}/results")
            except StoreError as e:
                return store_error_response(e)
            try:
                parsed = await receive_multipart(request, writer)
            except StoreError as e:  # 解析函数已 abort 过 writer
                return store_error_response(e)

            # 最终文件名：优先表单字段 filename（调用方显式覆盖），回退文件部分自带的名字
            try:
                final = writer.finalize(policy.check_filename(parsed.fields.get("filename") or parsed.filename))
            except StoreError as e:
                writer.abort()
                return store_error_response(e)
            return json(
                {
                    "task_id": tid,
                    "filename": final.name,
                    "download_url": policy.download_url(tid, final.name),
                    "size": writer.size,
                },
                status=status,
            )

        async def create_session(request: Request, task_id: str, status: int):
            body = json_object(request)
            if not body.get("filename"):
                return json({"error": "缺少 filename"}, status=400)
            try:
                key = policy.key_for(task_id, body["filename"])
                session = svc.chunked.create(key, parse_total(body))
            except StoreError as e:
                return store_error_response(e)
            return json({**session, **policy.describe_object(key, 0)}, status=status)

        async def batch_delete(request: Request, task_id: str):
            """按 URL 列表批量删除。**幂等**，逐条独立，永不整体失败。

            - 路径里的 task_id 是权威锚点：逐条反解出的 task_id 必须与它相等，否则
              ``skipped: foreign_task``——没有这道校验，一个批量口子就能跨任务删文件。
            - ``items`` 与入参 ``urls`` 一一对应（含重复项，标 ``duplicate``）。
            - 用 POST 而不是带 body 的 DELETE：DELETE 的请求体语义未定义，网关会丢。
            """
            body = request.json
            if not isinstance(body, dict):
                return json({"error": "请求体必须是 JSON 对象"}, status=400)
            urls = body.get("urls")
            if not isinstance(urls, list):
                return json({"error": "urls 必须是数组"}, status=400)
            if len(urls) > MAX_BATCH_DELETE:
                return json({"error": f"单次最多 {MAX_BATCH_DELETE} 条，收到 {len(urls)}"}, status=400)
            try:
                policy.check_task_id(task_id)
            except StoreError as e:
                return store_error_response(e)

            items, deleted_count, seen = [], 0, set()
            for url in urls:
                parsed = policy.parse_url(url)
                if parsed is None:
                    items.append({"url": url, "deleted": False, "reason": "not_a_result_url"})
                    continue
                parsed_task, filename = parsed
                if parsed_task != task_id:
                    items.append({"url": url, "deleted": False, "reason": "foreign_task"})
                    continue
                if filename in seen:
                    items.append({"url": url, "filename": filename, "deleted": False, "reason": "duplicate"})
                    continue
                seen.add(filename)
                try:
                    ok = svc.store.delete(policy.key_for(task_id, filename))
                except StoreError as e:
                    items.append({"url": url, "filename": filename, "deleted": False, "reason": f"invalid: {e.message}"})
                    continue
                deleted_count += ok
                items.append({"url": url, "filename": filename, "deleted": ok})

            logger.info("批量删除结果文件: task_id=%s 请求 %d 条, 实删 %d 条", task_id, len(urls), deleted_count)
            return json(
                {
                    "task_id": task_id,
                    "requested": len(urls),
                    "deleted": deleted_count,
                    "skipped": len(urls) - deleted_count,
                    "items": items,
                }
            )

        async def create_signature(request: Request, task_id: str, body: dict):
            filename = body.get("filename")
            if not task_id or not filename:
                return json({"error": "缺少 task_id 或 filename"}, status=400)
            try:
                key = policy.key_for(task_id, filename)
            except StoreError as e:
                return store_error_response(e)
            ttl = parse_ttl(body)
            sign = svc.sign(key, ttl)
            name = key.rsplit("/", 1)[1]
            return json(
                {
                    "object_key": key,
                    "key": key,
                    "sign": sign,
                    "url": f"{policy.download_url(task_id, name)}?sign={sign}",
                    "ttl_seconds": ttl if ttl is not None else settings.STORAGE_SIGN_TTL_SECONDS,
                }
            )

        # ---------- 规范路径 ----------

        @internal.post("/tasks/<task_id:str>/results", stream=True)
        @require_internal_token
        async def create_task_result(request: Request, task_id: str):
            return await create_result(request, task_id, 201)

        @internal.post("/tasks/<task_id:str>/results/uploads")
        @require_internal_token
        async def create_task_result_upload(request: Request, task_id: str):
            return await create_session(request, task_id, 201)

        @internal.post("/tasks/<task_id:str>/results/deletions")
        @require_internal_token
        async def create_task_result_deletion(request: Request, task_id: str):
            return await batch_delete(request, task_id)

        @internal.post("/tasks/<task_id:str>/signatures")
        @require_internal_token
        async def create_task_signature(request: Request, task_id: str):
            return await create_signature(request, task_id, json_object(request))

        @internal.delete("/tasks/<task_id:str>")
        @require_internal_token
        async def delete_task(request: Request, task_id: str):
            try:
                tid = policy.check_task_id(task_id)
            except StoreError as e:
                return store_error_response(e)
            return json({"task_id": tid, "deleted": svc.store.delete_prefix(f"tasks/{tid}")})

        @internal.delete("/tasks/<task_id:str>/results/<filename:path>")
        @require_internal_token
        async def delete_task_result(request: Request, task_id: str, filename: str):
            filename = decode_param(filename)
            try:
                key = policy.key_for(task_id, filename)
            except StoreError as e:
                return store_error_response(e)
            return json({"task_id": task_id, "filename": filename, "deleted": svc.store.delete(key)})

        @public.get("/tasks/<task_id:str>/results")
        async def list_task_results(request: Request, task_id: str):
            try:
                tid = policy.check_task_id(task_id)
            except StoreError:
                return json({"task_id": task_id, "files": []})
            return json({"task_id": tid, "files": svc.store.list_prefix(f"tasks/{tid}/results")})

        @public.route("/tasks/<task_id:str>/results/<filename:path>", methods=["GET", "HEAD"])
        async def read_task_result(request: Request, task_id: str, filename: str):
            """下载结果文件。默认公开；URL 携带 ``?sign=`` 时校验限时签名（无效 → 403）。

            .log 等文本后缀内联返回；其余支持 Range 分段（206 / 416）。
            """
            filename = decode_param(filename)
            try:
                key = policy.key_for(task_id, filename)
            except StoreError:
                return json({"error": f"结果文件不存在: {filename}"}, status=404)
            path = svc.store.exists(key)
            if not path:
                return json({"error": f"结果文件不存在: {filename}"}, status=404)

            sign = request.args.get("sign")
            if sign and not svc.verify_sign(sign, key):
                return json({"error": "签名无效或已过期"}, status=403)

            if request.method == "HEAD":
                return svc.head(path)
            if path.name.lower().endswith(policy.inline_suffixes):
                # 流式 + 支持 Range：日志可能很大，整读进内存会卡住事件循环（单 worker 下所有请求一起卡）
                return await svc.send(
                    path,
                    inline=True,
                    content_type="text/plain",
                    range_header=request.headers.get("range"),
                    extra_headers={"Cache-Control": "no-cache"},
                )
            return await svc.send(
                path,
                download_name=policy.download_name(task_id, path.name, request.args.get("name")),
                range_header=request.headers.get("range"),
            )

        # ---------- 兼容别名（已废弃）----------

        @internal.post("/tasks/<task_id:str>/results/upload", stream=True)
        @require_internal_token
        @_deprecated("/internal/tasks/{task_id}/results")
        async def legacy_create_result(request: Request, task_id: str):
            return await create_result(request, task_id, 200)

        @internal.post("/tasks/<task_id:str>/results/upload/init")
        @require_internal_token
        @_deprecated("/internal/tasks/{task_id}/results/uploads")
        async def legacy_create_session(request: Request, task_id: str):
            return await create_session(request, task_id, 200)

        @internal.post("/tasks/<task_id:str>/results/deletes")
        @require_internal_token
        @_deprecated("/internal/tasks/{task_id}/results/deletions")
        async def legacy_batch_delete(request: Request, task_id: str):
            return await batch_delete(request, task_id)

        @internal.post("/sign")
        @require_internal_token
        @_deprecated("/internal/tasks/{task_id}/signatures")
        async def legacy_sign(request: Request):
            body = json_object(request)
            return await create_signature(request, str(body.get("task_id") or ""), body)


register_policy(TaskResultsPolicy.name, TaskResultsPolicy)
