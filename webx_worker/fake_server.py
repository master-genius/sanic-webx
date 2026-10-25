"""最小假服务端 —— 按 docs/worker-protocol.md 实现的「任务服务 + 领取服务 + 存储服务」。

用途有二，两者共用这一份实现（避免「测试里的假服务端」与「自检里的假服务端」漂移）：

  · `python -m webx_worker selftest`：在本机起它，跑一遍完整生命周期，无需任何外部依赖；
  · 单元/集成测试：对 worker 的 HTTP 行为做端到端断言（含 fencing 强制校验）。

⚠️ 它是**协议的参考实现**，不是生产服务：单进程、内存状态、线程化。但刻意做得「够严」，
让违反协议的 worker 一定会在这里露馅：

  · 写端点**一律强制 fencing**（请求头优先，其次 JSON 体 / 表单字段）；令牌不符或缺令牌、
    终态任务分别回 `403 {"ok": false, "reason": "not_lessee" / "already_final"}`
    （`DELETE /lease` 缺令牌回 `missing_fencing_token`，协议 §4.6）；任务不存在回 `403 gone`。
  · 方法与路径严格：协议里的旧路径（`POST /status`、`POST /heartbeat`、`/received`……）
    一律 404，路径对但方法错回 405。
  · 心跳被撤销时回 `200 {"ok": false, "status": "revoked"}`（不是 403）。
  · 存储端：创建回 201；分片 offset 不符回 409 并带 `Upload-Offset` 响应头。
  · 所有请求按到达顺序记入 `calls`，测试据此断言调用顺序与内容。

只依赖标准库。
"""

from __future__ import annotations

import email
import email.policy
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlsplit

TERMINAL = ("completed", "failed", "cancelled")

# 状态机（docs/worker-protocol.md §6）
_TRANSITIONS = {
    "draft": ("pending", "draft"),
    "pending": ("queued", "running", "failed", "cancelled"),
    "queued": ("running", "failed", "cancelled"),
    "running": ("completed", "failed", "cancelled", "queued"),
}


def _deep_merge(dst: dict, src: dict) -> dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def _as_obj(v: Any) -> dict:
    """result_path / log_path 既可能是对象也可能是 JSON 字符串（协议未写清，两种都收）。"""
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            o = json.loads(v)
            return o if isinstance(o, dict) else {}
        except ValueError:
            return {}
    return {}


class FakeTaskServer:
    """假服务端。`start()` 后用 `.url` 访问；`stop()` 关闭。"""

    def __init__(
        self,
        token: str = "test-token",
        storage_token: str | None = None,
        hold_seconds: float = 0.4,
        lease_seconds: float = 60.0,
        heartbeat_lease_seconds: float = 300.0,
        wrap_task_info_in_data: bool = False,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self.token = token
        self.storage_token = storage_token if storage_token is not None else token
        self.hold_seconds = hold_seconds
        self.lease_seconds = lease_seconds
        self.heartbeat_lease_seconds = heartbeat_lease_seconds
        self.wrap_task_info_in_data = wrap_task_info_in_data
        self._host, self._port = host, port

        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.tasks: dict[str, dict] = {}
        self.calls: list[dict] = []
        self.storage: dict[tuple[str, str], bytes] = {}      # (task_id, filename) → 内容
        self.uploads: dict[str, dict] = {}                    # upload_id → 会话
        self._seq = 0
        self._order = 0
        self._rules: list[dict] = []                          # 故障注入
        self.hooks: list[Callable[[dict], None]] = []         # 请求到达时的回调（测试用）
        self._closing = False
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

        T = r"(?P<tid>[^/]+)"
        self._routes: list[tuple[str, re.Pattern, Callable]] = [
            ("GET", re.compile(r"^/health$"), self._h_health),
            ("POST", re.compile(r"^/task/leases$"), self._h_lease),
            ("POST", re.compile(r"^/task/pokes$"), self._h_poke),
            ("POST", re.compile(rf"^/internal/tasks/{T}/ack$"), self._h_ack),
            ("POST", re.compile(rf"^/internal/tasks/{T}/heartbeats$"), self._h_heartbeat),
            ("PUT", re.compile(rf"^/internal/tasks/{T}/status$"), self._h_status),
            ("POST", re.compile(rf"^/internal/tasks/{T}/complete$"), self._h_complete),
            ("POST", re.compile(rf"^/internal/tasks/{T}/fail$"), self._h_fail),
            ("DELETE", re.compile(rf"^/internal/tasks/{T}/lease$"), self._h_release),
            ("PATCH", re.compile(rf"^/internal/tasks/{T}/steps$"), self._h_steps),
            ("PATCH", re.compile(rf"^/internal/tasks/{T}/fields$"), self._h_fields),
            ("POST", re.compile(rf"^/internal/tasks/{T}/deliveries$"), self._h_deliveries),
            ("GET", re.compile(rf"^/internal/tasks/{T}$"), self._h_task_info),
            ("GET", re.compile(rf"^/internal/tasks/{T}/files$"), self._h_files),
            ("GET", re.compile(rf"^/internal/tasks/{T}/files/(?P<name>.+)$"), self._h_file),
            # 存储服务（docs/storage-service.md）
            ("POST", re.compile(rf"^/internal/tasks/{T}/results$"), self._h_result_create),
            ("POST", re.compile(rf"^/internal/tasks/{T}/results/uploads$"), self._h_upload_create),
            ("GET", re.compile(r"^/internal/uploads/(?P<uid>[^/]+)$"), self._h_upload_get),
            ("PATCH", re.compile(r"^/internal/uploads/(?P<uid>[^/]+)$"), self._h_upload_patch),
            ("POST", re.compile(r"^/internal/uploads/(?P<uid>[^/]+)/complete$"), self._h_upload_complete),
            ("DELETE", re.compile(r"^/internal/uploads/(?P<uid>[^/]+)$"), self._h_upload_delete),
            ("GET", re.compile(rf"^/public/tasks/{T}/results/(?P<name>.+)$"), self._h_public_get),
            ("GET", re.compile(rf"^/public/tasks/{T}/results$"), self._h_public_list),
        ]

    # ─────────────────────────── 生命周期 ───────────────────────────

    def start(self) -> "FakeTaskServer":
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):          # 静音
                pass

            def _dispatch(self):
                outer._handle(self)

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _dispatch

        self._httpd = ThreadingHTTPServer((self._host, self._port), Handler)
        self._httpd.daemon_threads = True
        self._port = self._httpd.server_address[1]
        self._thread = threading.Thread(
            target=lambda: self._httpd.serve_forever(poll_interval=0.05), daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        with self.cond:
            self._closing = True
            self.cond.notify_all()
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}"

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()

    # ─────────────────────────── 测试/自检用的控制面 ───────────────────────────

    def add_task(
        self,
        task_type: str,
        task_id: str | None = None,
        params: dict | None = None,
        files: dict[str, bytes] | None = None,
        timeout_seconds: int = 3600,
        level: int = 0,
        compute_points: int = 0,
        steps: list | None = None,
        sub_step: str = "",
        end_step: str = "",
        name: str = "",
    ) -> str:
        """提交一个 pending 任务并「戳」一下领取服务。"""
        with self.cond:
            self._order += 1
            tid = task_id or f"task{self._order:011d}"
            files = files or {}
            self.tasks[tid] = {
                "id": tid, "name": name or tid, "task_type": task_type,
                "files_json": json.dumps([{"name": n} for n in files], ensure_ascii=False),
                "files_content": dict(files),
                "params_text": json.dumps(params or {}, ensure_ascii=False),
                "steps": list(steps or []), "sub_step": sub_step, "end_step": end_step,
                "sub_status": "", "step_message": "",
                "timeout_seconds": timeout_seconds, "allowed_retries": 0,
                "level": level, "compute_points": compute_points,
                "lease_seq": 0, "worker_id": "", "status": "pending",
                "lease_until": 0, "order": self._order, "is_delete": False,
                "result_path": {}, "log_path": {}, "error_message": "",
                "start_time": 0.0,
            }
            self.cond.notify_all()
            return tid

    def task(self, tid: str) -> dict:
        with self.lock:
            return json.loads(json.dumps({k: v for k, v in self.tasks[tid].items()
                                          if k != "files_content"}))

    def set_status(self, tid: str, status: str) -> None:
        with self.lock:
            self.tasks[tid]["status"] = status

    def cancel_task(self, tid: str) -> None:
        """模拟用户取消：下一跳心跳会得到 `status=cancelled`。"""
        with self.lock:
            self.tasks[tid]["status"] = "cancelled"

    def revoke_lease(self, tid: str) -> None:
        """模拟补偿器回收租约：任务回 pending，lease_seq+1（协议 §6 不变式），租户清空。"""
        with self.cond:
            t = self.tasks[tid]
            t.update(status="pending", worker_id="", lease_until=0)
            t["lease_seq"] += 1
            self.cond.notify_all()

    def soft_delete(self, tid: str) -> None:
        with self.lock:
            self.tasks[tid]["is_delete"] = True

    def inject(self, method: str, path_re: str, status: int, times: int = 1,
               body: dict | None = None, headers: dict | None = None) -> None:
        """故障注入：匹配的前 `times` 个请求直接回 `status`（不进业务逻辑，仍记入 calls）。"""
        with self.lock:
            self._rules.append({
                "method": method.upper(), "re": re.compile(path_re), "status": status,
                "times": times, "body": body if body is not None else {"error": "injected"},
                "headers": headers or {},
            })

    def calls_to(self, method: str, path_re: str) -> list[dict]:
        rx = re.compile(path_re)
        with self.lock:
            return [c for c in self.calls if c["method"] == method.upper() and rx.search(c["path"])]

    def wait_for(self, predicate: Callable[[], bool], timeout: float = 15.0,
                 interval: float = 0.05) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if predicate():
                return True
            time.sleep(interval)
        return predicate()

    # ─────────────────────────── 请求处理 ───────────────────────────

    def _handle(self, h: BaseHTTPRequestHandler) -> None:
        parts = urlsplit(h.path)
        path, query = parts.path, parse_qs(parts.query)
        raw = self._read_body(h)
        headers = {k.lower(): v for k, v in h.headers.items()}
        ctype = headers.get("content-type", "")
        jbody: Any = None
        form: dict | None = None
        files: dict | None = None
        if raw and ctype.startswith("application/json"):
            try:
                jbody = json.loads(raw)
            except ValueError:
                jbody = None
        elif raw and ctype.startswith("multipart/form-data"):
            form, files = self._parse_multipart(ctype, raw)

        rec = {"method": h.command, "path": path, "query": {k: v[0] for k, v in query.items()},
               "headers": headers, "json": jbody, "form": form, "size": len(raw),
               "status": None, "t": time.monotonic()}
        with self.lock:
            self.calls.append(rec)
        for hook in list(self.hooks):
            hook(rec)

        status, body, extra = self._route(h.command, path, rec, raw, jbody, form, files)
        rec["status"] = status
        self._write(h, status, body, extra)

    @staticmethod
    def _read_body(h: BaseHTTPRequestHandler) -> bytes:
        if "chunked" in (h.headers.get("Transfer-Encoding") or "").lower():
            out = b""
            while True:
                line = h.rfile.readline().strip()
                n = int(line.split(b";")[0] or b"0", 16)
                if n == 0:
                    h.rfile.readline()
                    return out
                out += h.rfile.read(n)
                h.rfile.readline()
        n = int(h.headers.get("Content-Length") or 0)
        return h.rfile.read(n) if n else b""

    @staticmethod
    def _write(h: BaseHTTPRequestHandler, status: int, body: Any, extra: dict | None) -> None:
        data = b"" if body is None else (body if isinstance(body, bytes)
                                         else json.dumps(body, ensure_ascii=False).encode())
        try:
            h.send_response(status)
            if not (extra and "Content-Type" in extra):
                h.send_header("Content-Type", "application/json")
            h.send_header("Content-Length", str(len(data)))
            for k, v in (extra or {}).items():
                h.send_header(k, str(v))
            h.end_headers()
            if h.command != "HEAD":
                h.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass          # 客户端先断开（如 worker 取消了挂起中的领取请求）：不是错误

    @staticmethod
    def _parse_multipart(ctype: str, raw: bytes) -> tuple[dict, dict]:
        msg = email.message_from_bytes(
            b"Content-Type: " + ctype.encode() + b"\r\n\r\n" + raw, policy=email.policy.HTTP
        )
        form: dict[str, str] = {}
        files: dict[str, tuple[str, bytes]] = {}
        for part in msg.iter_parts():
            name = part.get_param("name", header="content-disposition")
            fname = part.get_filename()
            payload = part.get_payload(decode=True) or b""
            if fname is not None:
                files[name] = (fname, payload)
            else:
                form[name] = payload.decode("utf-8", "replace")
        return form, files

    def _route(self, method, path, rec, raw, jbody, form, files):
        # 故障注入优先
        with self.lock:
            for r in self._rules:
                if r["times"] > 0 and r["method"] == method and r["re"].search(path):
                    r["times"] -= 1
                    return r["status"], r["body"], r["headers"]

        matched_other_method = False
        for m, rx, fn in self._routes:
            mo = rx.match(path)
            if not mo:
                continue
            if m != method:
                matched_other_method = True
                continue
            # 鉴权：除 /health 外都要令牌。存储端点认存储令牌，其余认主令牌。
            if path != "/health":
                is_storage = (path.startswith("/public/") or "/results" in path
                              or path.startswith("/internal/uploads"))
                want = self.storage_token if is_storage else self.token
                if rec["headers"].get("x-internal-token") != want and not path.startswith("/public/"):
                    return 401, {"error": "unauthorized"}, None
            ctx = {"m": mo.groupdict(), "rec": rec, "raw": raw, "json": jbody,
                   "form": form, "files": files}
            return fn(ctx)
        if matched_other_method:
            return 405, {"error": "method not allowed"}, None
        return 404, {"error": "not found"}, None

    # ─────────────────────────── fencing ───────────────────────────

    def _token_of(self, rec: dict, jbody: Any, form: dict | None) -> tuple[str | None, str | None]:
        """协议 §2：请求头首选，其次 JSON 体 / 表单字段；两者都带时以请求头为准。"""
        h = rec["headers"]
        wid, seq = h.get("x-worker-id"), h.get("x-lease-seq")
        if wid is None and seq is None:
            src = jbody if isinstance(jbody, dict) else (form or {})
            wid = src.get("worker_id")
            seq = src.get("lease_seq")
            seq = None if seq is None else str(seq)
        return (wid or None), (seq if seq not in ("",) else None)

    def _fence(self, ctx, is_release: bool = False):
        """返回 (task, None) 或 (None, 403 响应三元组)。

        无令牌时的 reason：对 `DELETE /lease` 协议明写 `missing_fencing_token`（§4.6）；
        其它写端点协议没写，这里沿用线上旧服务端的行为——行上有租户而调用方不带令牌 →
        `not_lessee`（见 docs/worker.md 的协议缺口）。"""
        tid = ctx["m"]["tid"]
        with self.lock:
            t = self.tasks.get(tid)
            if t is None or t["is_delete"]:
                return None, (403, {"ok": False, "reason": "gone"}, None)
            wid, seq = self._token_of(ctx["rec"], ctx["json"], ctx["form"])
            if wid is None and seq is None:
                reason = "missing_fencing_token" if is_release else "not_lessee"
                return None, (403, {"ok": False, "reason": reason}, None)
            if wid is not None and (seq is None or not str(seq).lstrip("-").isdigit()):
                return None, (403, {"ok": False, "reason": "bad_lease_seq"}, None)
            if wid != t["worker_id"] or int(seq) != t["lease_seq"]:
                return None, (403, {"ok": False, "reason": "not_lessee"}, None)
            if t["status"] in TERMINAL:
                return None, (403, {"ok": False, "reason": "already_final"}, None)
            return t, None

    # ─────────────────────────── 领取服务 ───────────────────────────

    def _h_health(self, ctx):
        return 200, {"ok": True, "service": "task_server", "pokes": 0}, None

    def _h_poke(self, ctx):
        with self.cond:
            self.cond.notify_all()
        return 200, {"ok": True}, None

    def _claim(self, wid: str, slots: list[list[str]], max_level, max_points) -> list[dict]:
        out = []
        for slot in slots:
            cands = [t for t in self.tasks.values()
                     if t["status"] == "pending" and not t["is_delete"]
                     and t["task_type"] in slot
                     and (max_level is None or t["level"] <= max_level)
                     and (max_points is None or t["compute_points"] <= max_points)]
            if not cands:
                continue
            cands.sort(key=lambda t: (-t["level"], -t["compute_points"], t["order"]))
            t = cands[0]
            t["status"] = "queued"
            t["lease_seq"] += 1
            t["worker_id"] = wid
            t["lease_until"] = int((time.time() + self.lease_seconds) * 1000)
            out.append({k: t[k] for k in (
                "id", "name", "task_type", "files_json", "params_text", "steps", "sub_step",
                "end_step", "timeout_seconds", "allowed_retries", "level", "compute_points",
                "lease_seq", "worker_id", "status")})
        return out

    def _h_lease(self, ctx):
        body = ctx["json"] if isinstance(ctx["json"], dict) else {}
        wid = ctx["rec"]["headers"].get("x-worker-id") or body.get("worker_id")
        if not wid:
            return 400, {"error": "worker_id required"}, None
        slots = body.get("slots")
        if (not isinstance(slots, list) or not slots or len(slots) > 16
                or any(not isinstance(s, list) or not s or len(s) > 32
                       or any(not isinstance(x, str) for x in s) for s in slots)):
            return 400, {"error": "invalid slots"}, None
        limits = []
        for k in ("max_level", "max_compute_points"):
            v = body.get(k)
            if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v < 0):
                return 400, {"error": f"invalid {k}"}, None
            limits.append(v)
        deadline = time.monotonic() + self.hold_seconds
        with self.cond:
            while True:
                got = self._claim(wid, slots, limits[0], limits[1])
                if got:
                    return 200, {"tasks": got, "count": len(got)}, None
                left = deadline - time.monotonic()
                if left <= 0 or self._closing:
                    return 200, {"tasks": [], "count": 0}, None
                self.cond.wait(timeout=left)

    # ─────────────────────────── 任务回调 ───────────────────────────

    def _h_ack(self, ctx):
        t, err = self._fence(ctx)
        if err:
            return err
        with self.lock:
            t["lease_until"] = int((time.time() + self.heartbeat_lease_seconds) * 1000)
            return 200, {"ok": True, "lease_until": t["lease_until"]}, None

    def _h_heartbeat(self, ctx):
        tid = ctx["m"]["tid"]
        with self.lock:
            t = self.tasks.get(tid)
            if t is None or t["is_delete"]:
                return 200, {"ok": False, "status": "revoked", "reason": "gone"}, None
            wid, seq = self._token_of(ctx["rec"], ctx["json"], ctx["form"])
            if wid != t["worker_id"] or seq is None or not str(seq).isdigit() \
                    or int(seq) != t["lease_seq"]:
                # 协议 §4.2：被撤销回 200 + status=revoked，而不是 403
                return 200, {"ok": False, "status": "revoked", "reason": "not_lessee"}, None
            if t["status"] not in TERMINAL:
                t["lease_until"] = int((time.time() + self.heartbeat_lease_seconds) * 1000)
            return 200, {"ok": True, "status": t["status"], "lease_until": t["lease_until"]}, None

    def _h_status(self, ctx):
        t, err = self._fence(ctx)
        if err:
            return err
        body = ctx["json"] if isinstance(ctx["json"], dict) else {}
        new = body.get("status")
        with self.lock:
            if new not in _TRANSITIONS and new not in TERMINAL:
                return 400, {"error": "invalid status"}, None
            if new != t["status"] and new not in _TRANSITIONS.get(t["status"], ()):
                return 400, {"error": f"invalid transition {t['status']} -> {new}"}, None
            t["status"] = new
            if new == "running" and not t["start_time"]:
                t["start_time"] = time.time()
            if body.get("error_message"):
                t["error_message"] = body["error_message"]
            return 200, {"message": "ok"}, None

    def _h_complete(self, ctx):
        t, err = self._fence(ctx)
        if err:
            return err
        with self.lock:
            if t["status"] != "running":
                return 400, {"error": f"invalid transition {t['status']} -> completed"}, None
            t["status"] = "completed"
            return 200, {"message": "ok"}, None

    def _h_fail(self, ctx):
        t, err = self._fence(ctx)
        if err:
            return err
        body = ctx["json"] if isinstance(ctx["json"], dict) else {}
        with self.lock:
            t["status"] = "failed"
            t["error_message"] = body.get("error_message", "")
            return 200, {"message": "ok"}, None

    def _h_release(self, ctx):
        # 协议 §4.6：必须带 fencing（仅请求头，DELETE 无请求体）
        t, err = self._fence(ctx, is_release=True)
        if err:
            return err
        with self.cond:
            t.update(status="pending", worker_id="", lease_until=0)
            t["lease_seq"] += 1                      # 任何路径回 pending 都必须 +1
            self.cond.notify_all()
            return 200, {"ok": True}, None

    def _h_steps(self, ctx):
        t, err = self._fence(ctx)
        if err:
            return err
        body = ctx["json"] if isinstance(ctx["json"], dict) else {}
        with self.lock:
            for k in ("steps", "sub_step", "sub_status", "step_message"):
                if k in body:
                    t[k] = body[k]
            return 200, {"ok": True, "steps": t["steps"], "sub_step": t["sub_step"],
                         "sub_status": t["sub_status"]}, None

    def _h_fields(self, ctx):
        mode = ctx["rec"]["query"].get("mode", "overwrite")
        t, err = self._fence(ctx)
        if err:
            return err
        if mode not in ("overwrite", "merge"):
            return 400, {"error": "invalid mode"}, None
        body = ctx["json"] if isinstance(ctx["json"], dict) else {}
        with self.lock:
            for k in ("result_path", "log_path"):
                if k in body:
                    new = _as_obj(body[k])
                    if mode == "merge":
                        _deep_merge(t[k], new)
                    else:
                        t[k] = new
            return 200, {"ok": True}, None

    def _h_deliveries(self, ctx):
        # 协议 §4.9：刻意不做 fencing；任务不存在/已软删 → 404，其余 merge
        tid = ctx["m"]["tid"]
        body = ctx["json"] if isinstance(ctx["json"], dict) else {}
        with self.lock:
            t = self.tasks.get(tid)
            if t is None or t["is_delete"]:
                return 404, {"error": "task not found"}, None
            _deep_merge(t["result_path"], _as_obj(body.get("result_path")))
            return 200, {"ok": True}, None

    def _h_task_info(self, ctx):
        tid = ctx["m"]["tid"]
        with self.lock:
            t = self.tasks.get(tid)
            if t is None or t["is_delete"]:
                return 404, {"error": "task not found"}, None
            info = {k: t[k] for k in ("id", "status", "lease_seq", "worker_id", "task_type",
                                      "steps", "sub_step", "end_step")}
            return 200, ({"data": info} if self.wrap_task_info_in_data else info), None

    def _h_files(self, ctx):
        with self.lock:
            t = self.tasks.get(ctx["m"]["tid"])
            if t is None:
                return 404, {"error": "task not found"}, None
            return 200, {"files": list(t["files_content"])}, None

    def _h_file(self, ctx):
        name = unquote(ctx["m"]["name"])
        with self.lock:
            t = self.tasks.get(ctx["m"]["tid"])
            if t is None or name not in t["files_content"]:
                return 404, {"error": "file not found"}, None
            return 200, t["files_content"][name], {"Content-Type": "application/octet-stream"}

    # ─────────────────────────── 存储服务 ───────────────────────────

    @staticmethod
    def _dl(tid: str, name: str) -> str:
        return f"/public/tasks/{tid}/results/{name}"

    def _h_result_create(self, ctx):
        files = ctx["files"] or {}
        if "file" not in files:
            return 400, {"error": "file required"}, None
        fname, data = files["file"]
        name = (ctx["form"] or {}).get("filename") or fname
        tid = ctx["m"]["tid"]
        with self.lock:
            self.storage[(tid, name)] = data
        return 201, {"task_id": tid, "filename": name, "download_url": self._dl(tid, name),
                     "size": len(data)}, None

    def _h_upload_create(self, ctx):
        body = ctx["json"] if isinstance(ctx["json"], dict) else {}
        if not body.get("filename"):
            return 400, {"error": "filename required"}, None
        with self.lock:
            self._seq += 1
            uid = f"up{self._seq:08d}"
            self.uploads[uid] = {"task_id": ctx["m"]["tid"], "filename": body["filename"],
                                 "total": body.get("total"), "data": bytearray()}
        return 201, {"upload_id": uid, "offset": 0}, {"Upload-Offset": 0}

    def _h_upload_get(self, ctx):
        with self.lock:
            u = self.uploads.get(ctx["m"]["uid"])
            if u is None:
                return 404, {"error": "upload not found"}, None
            n = len(u["data"])
        return 200, {"offset": n}, {"Upload-Offset": n}

    def _h_upload_patch(self, ctx):
        with self.lock:
            u = self.uploads.get(ctx["m"]["uid"])
            if u is None:
                return 404, {"error": "upload not found"}, None
            have = len(u["data"])
            want = ctx["rec"]["headers"].get("upload-offset")
            if want is None or not want.isdigit() or int(want) != have:
                # 409 + 真实 offset：客户端据此重新对齐
                return 409, {"error": "offset mismatch", "offset": have}, {"Upload-Offset": have}
            u["data"].extend(ctx["raw"])
            return 200, {"offset": len(u["data"])}, {"Upload-Offset": len(u["data"])}

    def _h_upload_complete(self, ctx):
        with self.lock:
            u = self.uploads.get(ctx["m"]["uid"])
            if u is None:
                return 404, {"error": "upload not found"}, None
            if u["total"] is not None and len(u["data"]) != u["total"]:
                return 409, {"error": "incomplete", "offset": len(u["data"])}, \
                    {"Upload-Offset": len(u["data"])}
            self.storage[(u["task_id"], u["filename"])] = bytes(u["data"])
            del self.uploads[ctx["m"]["uid"]]
            return 200, {"task_id": u["task_id"], "filename": u["filename"],
                         "download_url": self._dl(u["task_id"], u["filename"]),
                         "size": len(u["data"])}, None

    def _h_upload_delete(self, ctx):
        with self.lock:
            self.uploads.pop(ctx["m"]["uid"], None)
        return 200, {"deleted": True}, None

    def _h_public_get(self, ctx):
        with self.lock:
            data = self.storage.get((ctx["m"]["tid"], unquote(ctx["m"]["name"])))
        if data is None:
            return 404, {"error": "not found"}, None
        return 200, data, {"Content-Type": "application/octet-stream"}

    def _h_public_list(self, ctx):
        with self.lock:
            names = sorted(n for (t, n) in self.storage if t == ctx["m"]["tid"])
        return 200, {"files": names}, None
