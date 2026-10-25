"""link_health 与鉴权失败快速失败路径的测试。

钉死本次改动的四条契约：
  1. 401/403 前台**只试 1 次**（不再空转 5 次退避），且仍然入队（永不放弃语义不变）
  2. 确定性故障（鉴权 / TLS）经 ObservedTransport 记账，首次立刻 ERROR
  3. 持续故障按间隔限流，不刷屏；恢复时打 INFO 并归零
  4. 启动自检探的是与真实回调**同一条路径**（同 base_url / 同 token / 同鉴权装饰器）

异步用例用 asyncio.run 驱动（与 pytest-asyncio 的 auto 模式不冲突）。
"""

import asyncio
import logging
import ssl
from pathlib import Path

import httpx
import pytest


from webx_worker.callback import (  # noqa: E402
    _CALLBACK_MAX_ATTEMPTS,
    WebCallbackClient,
    _enqueue_reason,
)
from webx_worker.link_health import (  # noqa: E402
    KIND_AUTH,
    KIND_UNREACHABLE,
    KIND_TLS,
    PROBE_TASK_ID,
    LinkHealth,
    ObservedTransport,
    check_web_link,
    classify_exception,
    classify_status,
    link_health,
)


# ─── 测试替身 ────────────────────────────────────────────────────────────────


class _Script:
    """按脚本回放状态码 / 抛异常，并记录被调用次数。"""

    def __init__(self, items):
        self.items = list(items)
        self.calls = 0
        self.seen: list[httpx.Request] = []

    async def __call__(self, request):
        self.calls += 1
        self.seen.append(request)
        item = self.items.pop(0) if self.items else 200
        if isinstance(item, BaseException):
            raise item
        return httpx.Response(item, request=request)

    def as_method(self):
        """返回可直接 monkeypatch 到类上的**普通函数**。

        不能直接把 `_Script` 实例设为类属性：可调用对象不是描述符，`self` 不会被绑定，
        于是 `super().handle_async_request(request)` 会把 request 传给形参 self。
        """
        script = self

        async def handle_async_request(_self, request):
            return await script(request)

        return handle_async_request


class _StubTransport(httpx.AsyncBaseTransport):
    """直接作为 client 的 transport 使用（绕过 ObservedTransport，隔离重试语义）。"""

    def __init__(self, script: _Script):
        self._script = script

    async def handle_async_request(self, request):
        return await self._script(request)


@pytest.fixture(autouse=True)
def _reset_link_health():
    """每个用例前后归零全局单例，避免用例间串味。"""
    link_health.note_success()
    yield
    link_health.note_success()


# ─── 故障分类 ────────────────────────────────────────────────────────────────


def test_classify_status_only_covers_401():
    """只有 401 是内部鉴权故障——依据 docs/worker-protocol.md §2，详见 test_business_403_is_not_an_auth_failure。"""
    assert classify_status(401) == KIND_AUTH
    # 403 是 fencing / 路径守卫的业务拒绝、400/404 是业务拒绝、5xx 是瞬时故障，都不归本模块管
    for code in (200, 400, 403, 404, 500, 502, 503):
        assert classify_status(code) is None


def test_classify_exception_finds_ssl_error_through_wrapper():
    """httpx 把 ssl.SSLError 包进 ConnectError，判据必须沿异常链走而非看类型。"""
    wrapped = httpx.ConnectError("connect failed")
    wrapped.__cause__ = ssl.SSLCertVerificationError("unable to get local issuer certificate")
    assert classify_exception(wrapped) == KIND_TLS


def test_classify_exception_ignores_transient_faults():
    """超时/连接被拒是瞬时故障，重试有意义，不该被当成确定性故障告警。"""
    assert classify_exception(httpx.ReadTimeout("timeout")) is None
    assert classify_exception(httpx.ConnectError("Connection refused")) is None


# ─── 告警节流与恢复 ──────────────────────────────────────────────────────────


def test_first_failure_alarms_immediately(caplog):
    lh = LinkHealth()
    with caplog.at_level(logging.ERROR, logger="webx_worker.link_health"):
        lh.note_failure(KIND_AUTH, "HTTP 401 POST /internal/tasks/x/ack")
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1
    assert lh.failing and lh.failure_count == 1


def test_repeated_failures_are_throttled_not_silenced(caplog):
    """连续失败不刷屏（限流），但计数照常累加——恢复日志要报得出准确总数。"""
    lh = LinkHealth(alarm_interval_sec=3600)
    with caplog.at_level(logging.ERROR, logger="webx_worker.link_health"):
        for _ in range(50):
            lh.note_failure(KIND_AUTH, "HTTP 401")
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1
    assert lh.failure_count == 50


def test_switching_failure_kind_restarts_the_round(caplog):
    """auth 的计数不该被 tls 继承，否则告警里的「累计次数」张冠李戴。"""
    lh = LinkHealth(alarm_interval_sec=3600)
    with caplog.at_level(logging.ERROR, logger="webx_worker.link_health"):
        lh.note_failure(KIND_AUTH, "HTTP 401")
        lh.note_failure(KIND_AUTH, "HTTP 401")
        lh.note_failure(KIND_TLS, "certificate verify failed")
    assert lh.failure_count == 1
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 2


def test_recovery_logs_and_resets(caplog):
    lh = LinkHealth(alarm_interval_sec=3600)
    lh.note_failure(KIND_AUTH, "HTTP 401")
    with caplog.at_level(logging.INFO, logger="webx_worker.link_health"):
        lh.note_success()
    assert not lh.failing and lh.failure_count == 0
    assert any("已恢复" in r.getMessage() for r in caplog.records)


def test_success_without_prior_failure_is_silent(caplog):
    """健康期每个请求都会调 note_success，绝不能因此刷日志。"""
    lh = LinkHealth()
    with caplog.at_level(logging.INFO, logger="webx_worker.link_health"):
        for _ in range(100):
            lh.note_success()
    assert not caplog.records


# ─── ObservedTransport：单一咽喉（测真类，打桩其基类）────────────────────────


def test_observed_transport_records_auth_failure(caplog):
    async def _run():
        inner = _StubTransport(_Script([401]))
        async with httpx.AsyncClient(transport=ObservedTransport(inner)) as client:
            return await client.post("http://x/internal/tasks/a/ack")

    with caplog.at_level(logging.ERROR, logger="webx_worker.link_health"):
        resp = asyncio.run(_run())

    assert resp.status_code == 401, "观测层只读不改，状态码须原样透传"
    assert link_health.failing
    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_observed_transport_does_not_swallow_exceptions():
    """观测层绝不能吞异常——否则调用点的重试/降级逻辑全部失效。"""
    exc = httpx.ConnectError("connect failed")
    exc.__cause__ = ssl.SSLCertVerificationError("unable to get local issuer certificate")

    async def _run():
        inner = _StubTransport(_Script([exc]))
        async with httpx.AsyncClient(transport=ObservedTransport(inner)) as client:
            await client.post("http://x/internal/tasks/a/ack")

    with pytest.raises(httpx.ConnectError):
        asyncio.run(_run())
    assert link_health.failing


def test_observed_transport_treats_business_reject_as_healthy():
    """400/404 是服务端给出的业务应答 → 链路本身是通的，不该报故障。"""
    async def _run():
        inner = _StubTransport(_Script([404]))
        async with httpx.AsyncClient(transport=ObservedTransport(inner)) as client:
            return await client.post("http://x/internal/tasks/a/ack")

    assert asyncio.run(_run()).status_code == 404
    assert not link_health.failing


# ─── 401 只试 1 次 ───────────────────────────────────────────────────────────


def _client_with(script: _Script) -> WebCallbackClient:
    cb = WebCallbackClient()
    cb._client = httpx.AsyncClient(transport=_StubTransport(script))
    return cb


def test_auth_failure_tries_once_not_five_times():
    """★ 本次改动的核心契约：401 前台只发 1 个请求，不再退避重试 5 次。"""
    script = _Script([401] * 10)
    cb = _client_with(script)

    async def _run():
        try:
            return await cb._send_with_retry("PUT", "http://x/internal/tasks/a/status", {})
        finally:
            await cb.close()

    resp, last_exc = asyncio.run(_run())
    assert script.calls == 1, "401 不该触发退避重试"
    assert resp is not None and resp.status_code == 401
    assert last_exc is None


def test_transient_failure_still_retries_five_times(monkeypatch):
    """回归护栏：5xx 的重试语义不能被本次改动波及。"""
    import webx_worker.callback as cbmod

    monkeypatch.setattr(cbmod, "_CALLBACK_BACKOFF_BASE_SEC", 0)
    script = _Script([503] * 10)
    cb = _client_with(script)

    async def _run():
        try:
            return await cb._send_with_retry("PUT", "http://x/internal/tasks/a/status", {})
        finally:
            await cb.close()

    resp, last_exc = asyncio.run(_run())
    assert script.calls == _CALLBACK_MAX_ATTEMPTS
    assert resp is None and last_exc is not None


def test_auth_failure_still_enqueues_never_gives_up(monkeypatch):
    """「永不放弃」语义不变：401 仍然入 PendingReporter 队列，等运维修好后自愈。"""
    cb = _client_with(_Script([401] * 5))
    enqueued = []

    async def fake_enqueue(action, task_id, payload):
        enqueued.append((action, task_id))

    monkeypatch.setattr(cb, "_enqueue_pending", fake_enqueue)

    async def _run():
        try:
            return await cb.update_status("t1", "running")
        finally:
            await cb.close()

    assert asyncio.run(_run()) is False
    assert enqueued == [("update_status", "t1")], "鉴权失败仍须入队，不可静默丢弃"


def test_enqueue_reason_does_not_lie_about_retry_count():
    """日志文案必须与实际行为一致：只试了 1 次就不能写「5 次重试失败」。"""
    resp = httpx.Response(401, request=httpx.Request("POST", "http://x"))
    auth_reason = _enqueue_reason(resp, None)
    assert "仅试 1 次" in auth_reason
    assert "5 次重试失败" not in auth_reason

    assert "5 次重试失败" in _enqueue_reason(None, RuntimeError("boom"))


# ─── 启动自检 ────────────────────────────────────────────────────────────────


def test_startup_probe_uses_the_real_callback_path():
    """自检必须走与真实回调完全相同的路径，否则「探测通过」没有代表性。"""
    script = _Script([404])         # 协议 §4.10：不存在的任务回 404 = 可达且鉴权通过
    cb = WebCallbackClient()
    cb._client = httpx.AsyncClient(
        transport=_StubTransport(script),
        headers={"X-Internal-Token": cb.internal_token},
    )

    async def _run():
        try:
            return await check_web_link(cb, attempts=1, delay_sec=0)
        finally:
            await cb.close()

    ok, kind, detail = asyncio.run(_run())
    assert ok and kind is None and detail == ""
    req = script.seen[0]
    assert req.method == "GET", "自检必须是只读探测，不能用写端点（写端点会被 fencing 拒绝）"
    assert str(req.url) == f"{cb.base_url}/internal/tasks/{PROBE_TASK_ID}"
    assert req.headers.get("X-Internal-Token") == cb.internal_token, \
        "自检必须带 token，否则探不出鉴权问题"


def test_startup_probe_treats_200_and_404_as_healthy_but_403_as_not():
    """可达且鉴权通过 = 200（该 ID 恰好存在）或 404；403/5xx 说明链路上有东西不对。"""
    for code, expect_ok in ((200, True), (404, True), (403, False), (500, False)):
        cb = _client_with(_Script([code] * 2))
        ok, kind, _ = asyncio.run(_probe_once(cb))
        assert ok is expect_ok, (code, ok, kind)
        if not expect_ok:
            assert kind == KIND_UNREACHABLE      # 403 不是鉴权故障（见 classify_status）


async def _probe_once(cb):
    try:
        return await check_web_link(cb, attempts=2, delay_sec=0)
    finally:
        await cb.close()


def test_startup_probe_reports_auth_failure():
    cb = _client_with(_Script([401, 401]))

    async def _run():
        try:
            return await check_web_link(cb, attempts=2, delay_sec=0)
        finally:
            await cb.close()

    ok, kind, detail = asyncio.run(_run())
    assert ok is False and kind == KIND_AUTH and "401" in detail


def test_startup_probe_retries_transient_then_succeeds():
    """启动期服务端可能还没起来，自检要容忍短暂失败。"""
    script = _Script([httpx.ConnectError("refused"), 404])
    cb = _client_with(script)

    async def _run():
        try:
            return await check_web_link(cb, attempts=3, delay_sec=0)
        finally:
            await cb.close()

    ok, kind, _ = asyncio.run(_run())
    assert ok is True and kind is None and script.calls == 2


# ─── 防漂移：观测覆盖不许被新代码绕过 ────────────────────────────────────────


def test_no_worker_module_builds_a_raw_httpx_client():
    """★ 全包扫描：worker 包里不许直接构造 httpx.AsyncClient，一律走 observed_async_client。

    这条测试守的是**本次事故的根因类别**——不是某一行代码写错，而是"新增一条 HTTP
    调用路径时忘了让它发声"。历史上 callback 有观测而 http_file_client 没有，于是同一
    条链路的故障在一半路径上是哑的。靠人记住不现实，交给测试。

    用 AST 而非正则/文本窗口：构造调用附近往往带着「见 link_health…」这类注释，文本
    匹配会把注释当成实现从而永远通过——一条永远不会失败的测试比没有测试更危险
    （初版就踩了这个坑，见 git 历史）。

    新增客户端时若确有理由用裸客户端（例如打的根本不是内部 API），把文件名加进
    `_EXEMPT` 并写明理由——让豁免成为需要解释的显式动作，而不是默默漏掉。
    """
    import ast

    pkg = Path(__file__).resolve().parents[2] / "webx_worker"
    _EXEMPT = {
        "link_health.py": "观测工厂本身：observed_async_client 必须在这里构造裸客户端",
    }

    offenders = []
    for py in sorted(pkg.rglob("*.py")):
        if py.name in _EXEMPT:
            continue
        tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "AsyncClient"
            ):
                offenders.append(f"{py.name}:{node.lineno}")

    assert not offenders, (
        "以下位置直接构造了 httpx.AsyncClient，其链路故障将无声无息：\n  "
        + "\n  ".join(offenders)
        + "\n请改用 link_health.observed_async_client()，"
        "或在本测试的 _EXEMPT 里写明豁免理由。"
    )


def test_observed_client_preserves_env_proxies():
    """★ 观测不得改变出网路径。

    httpx 的规则是 `allow_env_proxies = trust_env and transport is None`，所以直接传
    `transport=` 会**整体禁用** HTTP_PROXY/HTTPS_PROXY。在设了全局代理的机器上，那是
    一次静默的出网路径变更——与本模块的目的正好相反。故工厂必须先让 httpx 自行装配、
    再逐个包装。
    """
    import os
    from unittest import mock

    from webx_worker.link_health import observed_async_client

    env = {"HTTP_PROXY": "http://proxy.example:3128",
           "HTTPS_PROXY": "http://proxy.example:3128"}
    with mock.patch.dict(os.environ, env, clear=False):
        baseline = httpx.AsyncClient(timeout=30.0)
        observed = observed_async_client(timeout=30.0)

    assert {k.pattern for k in observed._mounts} == {k.pattern for k in baseline._mounts}, \
        "代理映射与默认构造不一致 —— 出网路径被观测层改变了"
    assert observed._mounts, "环境变量代理应当被保留（本用例已注入 HTTP_PROXY）"
    # 代理路径本身也必须被观测，否则走代理时故障又变哑
    assert all(isinstance(t, ObservedTransport) for t in observed._mounts.values() if t)
    assert isinstance(observed._transport, ObservedTransport)
    assert observed.timeout.connect == baseline.timeout.connect


def test_observed_client_factory_fails_loudly_if_httpx_renames_internals():
    """工厂依赖 httpx 的 _transport / _mounts；升级后改名要在测试里立刻炸，不能静默失效。"""
    from webx_worker.link_health import observed_async_client

    c = observed_async_client(timeout=5.0)
    assert hasattr(c, "_transport") and hasattr(c, "_mounts"), \
        "httpx 内部结构已变更，请同步修改 observed_async_client"


def test_observed_transport_forwards_lifecycle_hooks():
    """★ 回归：包装器必须转发 __aenter__ / __aexit__ / aclose。

    httpx.AsyncClient.__aenter__ 会调 transport.__aenter__()，而基类实现只是
    `return self`。初版包装器没转发，导致 `async with observed_async_client(...)`
    退出时内层连接池永不关闭——每次调用泄漏一组连接。包装器的第一原则是"除观测外行为完全一致"。
    """
    seen = []

    class _Inner(httpx.AsyncBaseTransport):
        async def __aenter__(self):
            seen.append("enter"); return self

        async def __aexit__(self, *a):
            seen.append("exit")

        async def aclose(self):
            seen.append("aclose")

        async def handle_async_request(self, request):
            return httpx.Response(200, request=request)

    async def _run():
        async with ObservedTransport(_Inner()) as t:
            await t.handle_async_request(httpx.Request("GET", "http://x"))
        await ObservedTransport(_Inner()).aclose()

    asyncio.run(_run())
    assert seen == ["enter", "exit", "aclose"], f"生命周期钩子未完整转发: {seen}"


def test_observed_client_closes_inner_pool_on_context_exit():
    """端到端：走 httpx.AsyncClient 的 async with，内层池必须被真正关闭。"""
    closed = []

    class _Inner(httpx.AsyncBaseTransport):
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            closed.append(True)

        async def handle_async_request(self, request):
            return httpx.Response(200, request=request)

    async def _run():
        async with httpx.AsyncClient(transport=ObservedTransport(_Inner())) as c:
            await c.get("http://x/ping")

    asyncio.run(_run())
    assert closed, "async with 退出时内层 transport 未被关闭 —— 连接泄漏"


def test_observation_failure_never_breaks_the_request(monkeypatch):
    """★ 观测是旁路：它自己出错也绝不能让请求失败。

    没有这条底线，观测层的一个小 bug 就会放大成全链路故障——那比不加观测糟糕得多。
    """
    def _boom(*a, **k):
        raise RuntimeError("观测层自爆")

    monkeypatch.setattr(link_health, "note_success", _boom)
    monkeypatch.setattr(link_health, "note_failure", _boom)

    class _Inner(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            return httpx.Response(200, request=request)

    async def _run():
        async with httpx.AsyncClient(transport=ObservedTransport(_Inner())) as c:
            return await c.get("http://x/ping")

    assert asyncio.run(_run()).status_code == 200, "观测异常不该影响请求"


def test_observation_overhead_is_negligible():
    """性能护栏：健康路径下每请求的观测开销必须可忽略（纯内存、无锁、无 IO）。"""
    import time as _t

    lh = LinkHealth()
    n = 200_000
    t0 = _t.perf_counter()
    for _ in range(n):
        classify_status(200)
        lh.note_success()
    per_call_us = (_t.perf_counter() - t0) / n * 1e6
    assert per_call_us < 5, f"单次观测 {per_call_us:.2f}μs，超出预期（应 <5μs）"


def test_link_health_state_does_not_grow():
    """内存护栏：无论失败多少次，状态恒为固定几个标量，不存在累积容器。"""
    lh = LinkHealth(alarm_interval_sec=3600)
    before = set(vars(lh))
    for i in range(10_000):
        lh.note_failure(KIND_AUTH, f"HTTP 401 #{i}", task_id=f"t{i}")
    assert set(vars(lh)) == before, "出现了新属性"
    assert all(
        isinstance(v, (int, float, str, type(None))) for v in vars(lh).values()
    ), f"状态里混入了容器类型: {vars(lh)}"


def test_startup_probe_classifies_unreachable_not_auth():
    """★ 回归：服务端没起来（连接被拒）绝不能被归成鉴权问题。

    初版把类别从 detail 文本反推——凡是文本里没有 "SSL"/"certificate" 就当成 auth，
    于是「Connection refused」会打出「请核对 INTERNAL_TOKEN」。大声指错方向与安静
    失败同样耽误事，故类别必须由分类函数判定后带出。
    """
    cb = _client_with(_Script([httpx.ConnectError("[Errno 111] Connection refused")] * 2))

    async def _run():
        try:
            return await check_web_link(cb, attempts=2, delay_sec=0)
        finally:
            await cb.close()

    ok, kind, detail = asyncio.run(_run())
    assert ok is False
    assert kind == KIND_UNREACHABLE, f"连接被拒被误判为 {kind}"
    assert "Connection refused" in detail


def test_startup_probe_classifies_5xx_as_unreachable():
    """服务端可达但应答不对（502/500）—— 是上游/反代问题，不是 token 问题。"""
    cb = _client_with(_Script([502, 502]))

    async def _run():
        try:
            return await check_web_link(cb, attempts=2, delay_sec=0)
        finally:
            await cb.close()

    ok, kind, _ = asyncio.run(_run())
    assert ok is False and kind == KIND_UNREACHABLE


def test_banner_prints_hints_matching_the_kind(caplog):
    """横幅的排查清单必须与类别一致（这才是自检的价值所在）。"""
    from webx_worker.link_health import log_link_check_banner

    with caplog.at_level(logging.ERROR, logger="webx_worker.link_health"):
        log_link_check_banner(False, KIND_UNREACHABLE, "ConnectError: refused", "http://x")
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "WEBX_WORKER_WEB_BASE_URL" in text and "任务服务是否已启动" in text
    assert "INTERNAL_TOKEN" not in text, "不可达却提示核对 token = 指错方向"


# ─── certs 自动同步：改用观测工厂后功能必须不变 ──────────────────────────────


def test_business_403_is_not_an_auth_failure():
    """★ 依据协议 §2：内部鉴权失败恒为 401；403 是 fencing / 路径守卫的业务拒绝
    （not_lessee / gone / already_final，客户端不应重试，也不应触发全局鉴权告警）。

    若把 403 也算成鉴权故障，一次正常的租约丢失就会告警「令牌不一致 / 反代未透传」——指错方向。
    """
    assert classify_status(401) == KIND_AUTH
    assert classify_status(403) is None, "403 被误判为鉴权故障"


def test_403_download_keeps_link_healthy(caplog):
    """端到端：服务端回 403 业务拒绝（非法路径 / fencing）不得触发全局链路告警。"""
    async def _run():
        inner = _StubTransport(_Script([403]))
        async with httpx.AsyncClient(transport=ObservedTransport(inner)) as c:
            return await c.get("http://x/internal/tasks/t1/files/../../etc/passwd")

    with caplog.at_level(logging.ERROR, logger="webx_worker.link_health"):
        resp = asyncio.run(_run())

    assert resp.status_code == 403
    assert not link_health.failing, "业务 403 把链路标成了故障"
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]


# ─── 自检必须有界且不占启动路径 ──────────────────────────────────────────────


def test_selfcheck_uses_short_timeout_not_the_30s_business_timeout():
    """★ 自检超时必须逐请求覆盖，远小于 callback 客户端的 30s 业务超时。"""
    script = _Script([404])
    cb = _client_with(script)

    async def _run():
        try:
            return await check_web_link(cb, attempts=1, delay_sec=0, timeout=5.0)
        finally:
            await cb.close()

    asyncio.run(_run())
    t = script.seen[0].extensions.get("timeout") or {}
    assert t.get("connect") == 5.0 and t.get("read") == 5.0, \
        f"自检未覆盖超时，实际={t}（复用 30s 会让最坏启动延迟到 96s）"


def test_selfcheck_does_not_retry_deterministic_failures():
    """★ 401 / TLS 再试也是同一个答案，必须立即返回，不占启动时间。"""
    auth = _Script([401] * 5)
    cb = _client_with(auth)
    asyncio.run(check_web_link(cb, attempts=3, delay_sec=0))
    assert auth.calls == 1, f"401 重试了 {auth.calls} 次"
    asyncio.run(cb.close())

    tls_exc = httpx.ConnectError("connect failed")
    tls_exc.__cause__ = ssl.SSLCertVerificationError("unable to get local issuer certificate")
    tls = _Script([tls_exc] * 5)
    cb2 = _client_with(tls)
    ok, kind, _ = asyncio.run(check_web_link(cb2, attempts=3, delay_sec=0))
    assert tls.calls == 1, f"TLS 故障重试了 {tls.calls} 次"
    assert kind == KIND_TLS
    asyncio.run(cb2.close())


def test_selfcheck_still_retries_unreachable():
    """回归：只有「不可达」才该重试——对应 web 还没起来的启动顺序竞态。"""
    script = _Script([httpx.ConnectError("refused"), httpx.ConnectError("refused"), 404])
    cb = _client_with(script)
    ok, kind, _ = asyncio.run(check_web_link(cb, attempts=3, delay_sec=0))
    assert ok is True and script.calls == 3
    asyncio.run(cb.close())


def test_selfcheck_is_launched_in_background_not_awaited():
    """★ 自检必须在后台任务里跑，绝不能出现在启动路径的 await 链上。

    早期版本同步 await，服务端黑洞时最坏 96s 内 worker 领不到任何任务——
    观测反过来伤了功能。用 AST 确认 app.py 里是 create_task 而非直接 await。
    """
    import ast

    src = (Path(__file__).resolve().parents[2] / "webx_worker" / "app.py").read_text()
    tree = ast.parse(src)

    awaited, tasked = [], []
    for node in ast.walk(tree):
        name = None
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            name = getattr(node.value.func, "id", None) or getattr(node.value.func, "attr", None)
            if name in ("selfcheck_and_report", "check_web_link"):
                awaited.append(name)
        if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "create_task":
            for a in node.args:
                inner = getattr(getattr(a, "func", None), "id", None)
                if inner in ("selfcheck_and_report", "check_web_link"):
                    tasked.append(inner)

    assert not awaited, f"自检被直接 await，会阻塞启动: {awaited}"
    assert tasked, "未在后台任务中启动自检"
