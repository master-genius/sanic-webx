"""uploader 的判据与两阶段语义。

守两件事：
  ① **丢弃判据** —— 队列条目本身就是委托：任务还在、状态不是失败/取消，就该传。
     不比对 result_path 字段值（那会撞上"占位值还没推上云"的次序问题）。
  ② **两阶段** —— 上传成功后原地记 URL；回写失败只重做回写，绝不重传几个 GB。

"""

import asyncio
import tempfile
from pathlib import Path


from webx_worker.sdk import upload_queue as q  # noqa: E402
from webx_worker import uploader as U  # noqa: E402


class _Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._payload


class _Client:
    """极简 async client 桩：按 (method, url 片段) 给响应，并记录调用。"""

    def __init__(self, routes=None, raise_on=None):
        self.routes = routes or {}
        self.raise_on = raise_on or set()
        self.calls = []

    def _pick(self, method, url):
        self.calls.append((method, url))
        for frag in self.raise_on:
            if frag in url:
                raise RuntimeError("network down")
        for frag, resp in self.routes.items():
            if frag in url:
                return resp() if callable(resp) else resp
        return _Resp(500)

    async def get(self, url, **kw):
        return self._pick("GET", url)

    async def post(self, url, **kw):
        return self._pick("POST", url)

    async def patch(self, url, **kw):
        return self._pick("PATCH", url)

    async def delete(self, url, **kw):
        return self._pick("DELETE", url)


def _task_resp(status: str = "running"):
    # 协议 §4.10 没写清响应是否包在 data 里 —— uploader 两种都认（见 test_task_info_shape_*）
    return _Resp(200, {"id": "t1", "status": status})


def test_task_info_shape_wrapped_in_data_is_also_understood():
    work, entry = _bed()
    c = _Client({"/internal/tasks/": _Resp(200, {"data": {"status": "cancelled"}})})
    assert asyncio.run(U._still_wanted(c, entry)) is False
    c = _Client({"/internal/tasks/": _Resp(200, {"data": {"status": "running"}})})
    assert asyncio.run(U._still_wanted(c, entry)) is True


def _bed():
    d = Path(tempfile.mkdtemp())
    work = d / "work"
    q.ensure_dirs(work)
    f = d / "art.dat"
    f.write_bytes(b"z" * 512)
    q.enqueue(work, "t1", f, "step2.out", ["step2", "out_url"])
    return work, q.iter_pending(work)[0]


# ==================== 纯函数 ====================


def test_nest_builds_merge_payload():
    assert U._nest(["step2", "out_url"], "/p/a.dat") == {"step2": {"out_url": "/p/a.dat"}}


def test_nest_supports_three_levels():
    """三层嵌套（分组 → 条目 → 字段）；服务端按嵌套 key 幂等 merge，只覆盖叶子。"""
    assert U._nest(["group", "LIG", "file"], "/p/x.dat") == {"group": {"LIG": {"file": "/p/x.dat"}}}


# ==================== 唯一丢弃判据 ====================


def _wanted(routes=None, raise_on=None):
    work, entry = _bed()
    c = _Client(routes, raise_on)
    return asyncio.run(U._still_wanted(c, entry)), entry


def test_alive_task_is_uploaded():
    """★ 队列条目就是委托：任务在、状态正常 → 传。不看 result_path 里有什么。"""
    work, entry = _bed()
    c = _Client({"/internal/tasks/": _task_resp("running")})
    assert asyncio.run(U._still_wanted(c, entry)) is True


def test_success_status_still_uploads():
    """★ success 也要传：多阶段任务终态之后，产物照样得交付给用户。"""
    work, entry = _bed()
    c = _Client({"/internal/tasks/": _task_resp("success")})
    assert asyncio.run(U._still_wanted(c, entry)) is True


def test_drop_when_task_deleted():
    """404 已经明说任务不存在，没什么可执行的——与 executor 的 is_not_found 同一判断。"""
    ok, _ = _wanted({"/internal/tasks/": _Resp(404)})
    assert ok is False


def test_drop_when_task_cancelled():
    ok, _ = _wanted({"/internal/tasks/": _task_resp("cancelled")})
    assert ok is False


def test_drop_when_task_failed():
    """失败 → 这一代计算作废。要么 redo（重跑会重新投递），要么不要了。"""
    ok, _ = _wanted({"/internal/tasks/": _task_resp("failed")})
    assert ok is False


def test_drop_on_400():
    """400 业务拒绝（如畸形 task_id）→ 删记录，重试不会变好。"""
    ok, _ = _wanted({"/internal/tasks/": _Resp(400)})
    assert ok is False


def test_auth_error_retains_entry():
    """★ 401/403 **保留**，不丢弃。

    鉴权失败是部署问题不是数据问题，修好即自愈。若按"4xx 一律丢弃"处理，
    一次 Token 填错就会在几轮扫描内清空整个待传队列、产物全部永久丢失，
    而日志上只有几行"已丢弃"。与 PendingReporter 的 _AUTH_RETAIN_CODES 同取舍。
    """
    for code in (401, 403):
        ok, _ = _wanted({"/internal/tasks/": _Resp(code)})
        assert ok is True, f"HTTP {code} 不该丢弃条目"


def test_empty_result_path_does_not_drop():
    """★★ 云端 result_path 还没有本步任何内容 → 照样传。

    这正是不做字段比对的理由：条目在本步收尾**之前**就写进队列了，而字段要等
    本步全部产物传完、update_task_fields 推上云才有。若比对字段，uploader 会在
    这个窗口里把还没发布的条目当过期的删掉——产物永久丢失。
    """
    work, entry = _bed()
    c = _Client({"/internal/tasks/": _Resp(200, {"data": {
        "status": "running", "result_path": "",
    }})})
    assert asyncio.run(U._still_wanted(c, entry)) is True


def test_cancel_removes_the_queue_json():
    """★ 取消的动作就是「把这条 json 记录删掉」——两个目录都不该再有它。"""
    work, entry = _bed()
    fname = entry.filename
    c = _Client({"/internal/tasks/": _task_resp("cancelled")})
    asyncio.run(U._process(c, entry, str(work)))
    assert not (q.pending_dir(work) / fname).exists()
    assert not (q.inflight_dir(work) / fname).exists()
    assert not any(m in ("POST", "PATCH") for m, u in c.calls
                   if "/results" in u), "取消后仍发起了上传"


def test_network_failure_keeps_entry():
    """★ 问不到 → 当作"仍需要"。上传是幂等覆盖，多传一次只费带宽；误丢是产物永久缺失。"""
    ok, _ = _wanted(raise_on={"/internal/tasks/"})
    assert ok is True


def test_server_5xx_keeps_entry():
    ok, _ = _wanted({"/internal/tasks/": _Resp(503)})
    assert ok is True


# ==================== 两阶段 ====================


def test_second_pass_skips_upload_and_only_writes_back():
    """★ 上传成功、回写失败之后：下一轮**只回写**，不重传。

    没有这条，一个 5GB 的 out 会因为一次回写抖动整个重传。
    """
    work, entry = _bed()
    entry.uploaded_url = "/public/tasks/t1/results/step2.out"
    q.claim(work, entry)
    q.update(work, entry)
    q.requeue_inflight(work)          # 模拟"上一轮结束、重新排队"
    entry = q.iter_pending(work)[0]
    assert entry.uploaded_url, "前置条件：条目应带着已上传的 URL"

    c = _Client({
        "/internal/tasks/t1/deliveries": _Resp(200),
        "/internal/tasks/": _task_resp("running"),
    })
    asyncio.run(U._process(c, entry, str(work)))

    methods = [m for m, u in c.calls if "/results" in u or "/uploads/" in u]
    assert methods == [], f"不该再发起任何上传请求，实际发生了 {methods}"
    assert q.iter_pending(work) == [], "回写成功后条目应被移除"


def test_writeback_failure_releases_and_keeps_url():
    """回写失败 → 退回队列，但 uploaded_url 留着，下轮不重传。"""
    work, entry = _bed()
    entry.uploaded_url = "/public/a.dat"
    q.claim(work, entry)
    q.update(work, entry)
    q.requeue_inflight(work)
    entry = q.iter_pending(work)[0]

    c = _Client({
        "/deliveries": _Resp(500),
        "/internal/tasks/": _task_resp("running"),
    })
    asyncio.run(U._process(c, entry, str(work)))

    items = q.iter_pending(work)
    assert len(items) == 1
    assert items[0].uploaded_url == "/public/a.dat"
    assert items[0].attempts == 1


def _writeback_404_bed(payload):
    """构造「已上传、待回写」的条目，让 deliver 端点回一个带指定响应体的 404。"""
    work, entry = _bed()
    entry.uploaded_url = "/public/a.dat"
    q.claim(work, entry)
    q.update(work, entry)
    q.requeue_inflight(work)
    entry = q.iter_pending(work)[0]

    c = _Client({
        "/deliveries": _Resp(404, payload),
        "/internal/tasks/": _task_resp("running"),
    })
    asyncio.run(U._process(c, entry, str(work)))
    return q.iter_pending(work)


def test_writeback_404_drops_entry():
    """任务在"传完"与"回写"之间被删了：URL 无处可写，丢弃而非无限重试。

    判据是响应体形状——业务 404 带 ``error`` 键（服务端 fail 出来的），见下一条对照。
    """
    assert _writeback_404_bed({"error": "task not found"}) == []


def test_writeback_404_without_error_body_is_retained():
    """★ 对照：**路由**不存在的 404（web 尚未升级，缺 /results/deliver）必须保留。

    Sanic 的通用 404 是 ``description``/``message`` 那套、没有 ``error`` 键。若按
    "404 一律丢弃"处理，worker 先于 web 升级的那个窗口里，所有产物地址会被永久丢掉，
    而它们本来只要等 web 升上来就能送达。误留只是多试几轮，误丢是产物永久缺失。
    """
    items = _writeback_404_bed({"description": "Not Found", "message": "Requested URL not found"})
    assert len(items) == 1, "路由 404 被误当成「任务不存在」丢弃了"
    assert items[0].uploaded_url == "/public/a.dat", "已上传的 URL 必须留着，下轮不重传"


def test_process_drops_when_local_file_gone():
    """产物已被清理 → 丢弃（清理侧与队列的竞态兜底）。"""
    work, entry = _bed()
    Path(entry.local_path).unlink()
    c = _Client()
    asyncio.run(U._process(c, entry, str(work)))
    assert q.iter_pending(work) == []
    assert c.calls == [], "本地文件都没了，不该再问云端"


def test_process_drops_when_cloud_says_no():
    work, entry = _bed()
    c = _Client({"/internal/tasks/": _Resp(404)})
    asyncio.run(U._process(c, entry, str(work)))
    assert q.iter_pending(work) == []


def test_process_releases_on_unexpected_exception():
    """★ 单条出错绝不能掀翻 uploader，也不能吞掉条目。"""
    work, entry = _bed()

    class _Boom(_Client):
        async def get(self, url, **kw):
            raise KeyboardInterrupt if False else ValueError("unexpected")

    asyncio.run(U._process(_Boom(), entry, str(work)))
    items = q.iter_pending(work)
    assert len(items) == 1 and items[0].attempts == 1


# ==================== 配置口径 ====================


def test_storage_token_prefers_dedicated_and_falls_back_to_main():
    """存储服务专属 Token 优先；未配时回退主 Token（与 sdk.result_client / callback 同一条回退链）。

    没有「回退到任务服务」这一档了：协议规定产物只传存储服务。"""
    from webx_worker.config import worker_settings
    old = (worker_settings.STORAGE_URL, worker_settings.STORAGE_INTERNAL_URL,
           worker_settings.STORAGE_INTERNAL_TOKEN, worker_settings.INTERNAL_TOKEN)
    try:
        worker_settings.STORAGE_URL = ""
        worker_settings.STORAGE_INTERNAL_URL = "http://storage.internal"
        worker_settings.STORAGE_INTERNAL_TOKEN = "dedicated"
        worker_settings.INTERNAL_TOKEN = "main"
        assert U._storage_token() == "dedicated"
        worker_settings.STORAGE_INTERNAL_TOKEN = ""
        assert U._storage_token() == "main", "专属 Token 未配时必须回退主 Token"
    finally:
        (worker_settings.STORAGE_URL, worker_settings.STORAGE_INTERNAL_URL,
         worker_settings.STORAGE_INTERNAL_TOKEN, worker_settings.INTERNAL_TOKEN) = old


def test_storage_base_never_falls_back_to_the_task_service():
    from webx_worker.config import worker_settings
    old = (worker_settings.STORAGE_URL, worker_settings.STORAGE_INTERNAL_URL,
           worker_settings.WEB_BASE_URL)
    try:
        worker_settings.STORAGE_URL = ""
        worker_settings.STORAGE_INTERNAL_URL = ""
        worker_settings.WEB_BASE_URL = "http://task-service"
        assert U._storage_base() == "", "不得回退到任务服务（那里没有产物端点）"
        worker_settings.STORAGE_URL = "https://pub.example.com/"
        assert U._storage_base() == "https://pub.example.com"
        worker_settings.STORAGE_INTERNAL_URL = "http://10.0.0.5:9010"
        assert U._storage_base() == "http://10.0.0.5:9010", "内网地址优先"
    finally:
        (worker_settings.STORAGE_URL, worker_settings.STORAGE_INTERNAL_URL,
         worker_settings.WEB_BASE_URL) = old


def test_public_url_normalization():
    """落库 URL 恒用【对外】地址；未配则保持相对路径由前端按 origin 解析。"""
    from webx_worker.config import worker_settings
    old = worker_settings.STORAGE_URL
    try:
        worker_settings.STORAGE_URL = ""
        assert U._normalize_download_url("/public/a.dat") == "/public/a.dat"
        worker_settings.STORAGE_URL = "https://cdn.example.com/"
        assert U._normalize_download_url("/public/a.dat") == "https://cdn.example.com/public/a.dat"
        assert U._normalize_download_url("https://x/y") == "https://x/y"
    finally:
        worker_settings.STORAGE_URL = old

