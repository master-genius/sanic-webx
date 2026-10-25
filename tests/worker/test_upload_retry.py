"""产物上传的重试与超时契约。

上传是 worker 唯一「失败即永久丢失」的路径：url 落空后本地副本会被后续
`_cleanup_from_step`/redo 清掉，用户只能整步重跑（长程 md 可能是数小时）。
状态汇报有 PendingReporter 兜底，上传**没有**——所以前台这几次重试就是全部防线。

本组测试守两件事：

1. **重试窗口盖得住 storage 的一次重启**（5 次 / 累计 30s）。
2. **超时四项分开**，尤其 connect 不得继承大文件的长超时——httpx 传标量会把
   connect 也设成同一个值，防火墙 DROP 掉 SYN 时就抱着 Lane 干等数分钟。

"""

from pathlib import Path


import httpx  # noqa: E402

from webx_worker.sdk import result_client as RC  # noqa: E402


# ==================== 环境与桩 ====================


class _Env:
    """临时设置环境变量（退出时精确还原，包括「原本不存在」）。"""

    def __init__(self, **kv):
        self._kv, self._old = kv, {}

    def __enter__(self):
        import os
        for k, v in self._kv.items():
            self._old[k] = os.environ.get(k)
            os.environ[k] = v
        return self

    def __exit__(self, *_):
        import os
        for k, v in self._old.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v


class _Recorder:
    """替换 httpx.post / time.sleep，记录每次调用。

    ``responses`` 逐次弹出：元素是 int（状态码）或异常实例（直接 raise）。
    用尽后重复最后一个——让「全程失败」的用例不必写满 N 个。
    """

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[dict] = []
        self.sleeps: list[float] = []

    def post(self, url, **kw):
        self.calls.append({"url": url, **kw})
        item = self._responses[min(len(self.calls) - 1, len(self._responses) - 1)]
        if isinstance(item, BaseException):
            raise item
        return httpx.Response(item, json={"download_url": "/public/tasks/t/results/f.bin"},
                              request=httpx.Request("POST", url))

    def sleep(self, sec):
        self.sleeps.append(sec)


def _run_upload(tmpfile: Path, responses) -> tuple[_Recorder, object]:
    """在桩环境下跑一次 upload_result，返回 (recorder, 结果或异常)。"""
    rec = _Recorder(responses)
    old_post, old_sleep = RC.httpx.post, RC.time.sleep
    RC.httpx.post, RC.time.sleep = rec.post, rec.sleep
    try:
        with _Env(WEBX_WORKER_WEB_BASE_URL="http://web.test",
                  WEBX_WORKER_INTERNAL_TOKEN="tok",
                  WEBX_WORKER_STORAGE_URL="",
                  WEBX_WORKER_STORAGE_INTERNAL_URL="http://storage.test"):
            try:
                return rec, RC.upload_result("t1", str(tmpfile), filename="f.bin")
            except BaseException as e:  # noqa: BLE001 —— 用例要断言异常本身
                return rec, e
    finally:
        RC.httpx.post, RC.time.sleep = old_post, old_sleep


def _tmpfile() -> Path:
    import tempfile
    p = Path(tempfile.mkdtemp()) / "f.bin"
    p.write_bytes(b"payload")
    return p


# ==================== 一、重试窗口 ====================


def test_max_attempts_is_five():
    """5 次而非 3 次：3 次的退避窗口只有 3s，盖不住 storage 的一次重启。"""
    assert RC._UPLOAD_MAX_ATTEMPTS == 5


def test_backoff_window_covers_a_storage_restart():
    """★ 累计退避 = 30s。

    要盖住的是 storage 的一次 systemd 重启（进程退出 + Sanic 起来 +
    启动期扫会话目录），量级在秒到十几秒。
    再长就是白等——而这段等待占着 Lane，计算资源全闲着。
    """
    waits = [RC._UPLOAD_BACKOFF_BASE_SEC * (2 ** i)
             for i in range(RC._UPLOAD_MAX_ATTEMPTS - 1)]
    assert waits == [2, 4, 8, 16], f"退避序列应为 2/4/8/16，实为 {waits}"
    assert sum(waits) == 30


def test_transport_error_retries_to_the_limit():
    """连接失败（ConnectError，瞬时返回）→ 试满 5 次，退避序列一致，最终抛异常。"""
    f = _tmpfile()
    rec, out = _run_upload(f, [httpx.ConnectError("refused")])
    assert len(rec.calls) == 5, f"应试满 5 次，实为 {len(rec.calls)}"
    assert rec.sleeps == [2, 4, 8, 16], f"退避实为 {rec.sleeps}"
    assert rec.calls[0]["url"] == "http://storage.test/internal/tasks/t1/results", \
        "上传必须走存储服务的规范路径（POST /internal/tasks/<id>/results），不是任务服务"
    assert isinstance(out, httpx.HTTPError), f"耗尽后应抛 HTTPError，实为 {out!r}"


def test_connect_timeout_is_retriable():
    """★ ConnectTimeout（防火墙 DROP 掉 SYN）必须走重试，不能逃出循环。

    它与 ConnectError（收到 RST）是两种故障：本项目跨公网部署遇到的正是前者。
    _RETRIABLE_EXC 用 TransportError 基类兜住，此处守的是「别有人改回手写元组」。
    """
    assert issubclass(httpx.ConnectTimeout, RC._RETRIABLE_EXC)
    assert issubclass(httpx.WriteTimeout, RC._RETRIABLE_EXC)
    assert issubclass(httpx.ReadTimeout, RC._RETRIABLE_EXC)
    f = _tmpfile()
    rec, _ = _run_upload(f, [httpx.ConnectTimeout("dropped")])
    assert len(rec.calls) == 5


def test_5xx_retries():
    """5xx（storage 重启中 / 反代 502）→ 重试。"""
    f = _tmpfile()
    rec, out = _run_upload(f, [502])
    assert len(rec.calls) == 5
    assert isinstance(out, httpx.HTTPError)


def test_4xx_bails_out_immediately():
    """4xx（鉴权失败 / 任务不存在 / 超限）→ 只试 1 次。

    重试不会让它变好，多试 5 次只是把 Lane 多占 30 秒。
    """
    for code in (400, 401, 404, 413):
        f = _tmpfile()
        rec, out = _run_upload(f, [code])
        assert len(rec.calls) == 1, f"HTTP {code} 应只试 1 次，实为 {len(rec.calls)}"
        assert rec.sleeps == [], f"HTTP {code} 不应退避，实为 {rec.sleeps}"
        assert isinstance(out, httpx.HTTPError)


def test_success_on_retry_stops_early():
    """首次失败、第二次成功 → 不再继续试，且只退避一次。"""
    f = _tmpfile()
    rec, out = _run_upload(f, [httpx.ConnectError("x"), 201])     # 存储服务创建成功回 201
    assert len(rec.calls) == 2
    assert rec.sleeps == [2]
    assert isinstance(out, dict) and out.get("download_url")


def test_file_handle_reopened_each_attempt():
    """★ 句柄必须每次重开——httpx 会把流读到底，复用已消费的句柄会传 0 字节。

    断言方式：每次调用拿到的 file 对象都是不同实例，且都还能读出内容。
    """
    f = _tmpfile()
    rec, _ = _run_upload(f, [httpx.ConnectError("x")])
    handles = [c["files"]["file"][1] for c in rec.calls]
    assert len({id(h) for h in handles}) == len(handles), "各次尝试复用了同一个文件句柄"


# ==================== 二、超时四项分开 ====================


def test_timeout_is_a_split_object_not_a_scalar():
    """★ 必须是 httpx.Timeout 实例。

    传标量（timeout=300.0）会被 httpx 定义为「所有操作都 300s」——connect 也是。
    这正是「防火墙 DROP 掉 SYN → 抱着 Lane 干等」的成因。
    """
    assert isinstance(RC._UPLOAD_TIMEOUT, httpx.Timeout), (
        "上传超时必须用 httpx.Timeout 四项分开设置，标量会把 connect 一起拉长"
    )


def test_connect_timeout_is_short():
    """握手不需要长超时：连不上就快速认输去重试，别占着 Lane。"""
    t = RC._UPLOAD_TIMEOUT
    assert t.connect is not None and t.connect <= 15, f"connect 应 ≤15s，实为 {t.connect}"
    assert t.pool is not None and t.pool <= 15, f"pool 应 ≤15s，实为 {t.pool}"


def test_read_timeout_is_generous_for_storage_fsync():
    """read 管的是「请求体发完后等 storage 应答」这一段，必须留足。

    storage 的落盘收尾（fsync + 原子改名） 要 fsync + os.replace，几 GB 的产物在慢盘上
    花掉几十秒是正常的。砍短会把「storage 正在老实落盘」误判成超时，
    白丢一次本已成功的上传——文件其实已经落好了。
    """
    t = RC._UPLOAD_TIMEOUT
    assert t.read is not None and t.read >= 120, f"read 应 ≥120s，实为 {t.read}"
    assert t.read > t.connect, "read 必须比 connect 长（两者管的是完全不同的事）"


def test_write_timeout_does_not_cap_large_uploads():
    """★ write 是**空闲超时**，不是总时长——httpx 没有「请求总超时」这个概念。

    write=60 的含义是「单次 socket 写最多卡 60 秒」：几 GB 的产物只要传输一直有
    进展，跑多久都不会超时。60s 都推不出去一个块的链路是死的，不是慢。

    这条守的是理解本身：若有人为了「让大文件传得完」把它改成几千秒，
    就是在为一个不存在的问题付出「卡死时多占 Lane 十几分钟」的代价。
    """
    t = RC._UPLOAD_TIMEOUT
    assert t.write is not None and 30 <= t.write <= 120, (
        f"write 应在 30–120s（空闲上限，与文件大小无关），实为 {t.write}"
    )


def test_timeout_object_is_passed_through():
    """桩里确认真的传下去了——常量对了但没接上，等于没改。"""
    f = _tmpfile()
    rec, _ = _run_upload(f, [200])
    assert rec.calls[0]["timeout"] is RC._UPLOAD_TIMEOUT


# ==================== 三、最坏情况占用 Lane 的时长 ====================


def test_worst_case_lane_occupancy_is_bounded():
    """算一遍最坏占用：上传发生在任务子进程里，这段时间 Lane 一直被占着。

      · 防火墙 DROP  = 5×connect + 30s 退避
      · 链路卡死     = 5×write   + 30s 退避

    守的是「加重试次数」与「占用计算资源」这对矛盾不能失衡——
    次数还能加，但单次超时不能跟着一起长。
    """
    t = RC._UPLOAD_TIMEOUT
    backoff = sum(RC._UPLOAD_BACKOFF_BASE_SEC * (2 ** i)
                  for i in range(RC._UPLOAD_MAX_ATTEMPTS - 1))
    dropped = RC._UPLOAD_MAX_ATTEMPTS * t.connect + backoff
    stalled = RC._UPLOAD_MAX_ATTEMPTS * t.write + backoff
    assert dropped <= 120, f"防火墙 DROP 最坏占用 {dropped}s，超过 2 分钟"
    assert stalled <= 420, f"链路卡死最坏占用 {stalled}s，超过 7 分钟"

