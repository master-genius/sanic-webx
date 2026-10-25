"""分流判据：**下游依赖的产物恒同步，只有纯交付才按大小走 uploader。**

这是整个异步上传里唯一会造成正确性事故的地方。纯按大小分流会把下游步骤要吃的
产物推给 uploader；它们一旦没传完，跨机接力就拿不到上一步的产物——而且偶发
（取决于产物大小），最难排查的那种。

本组测试：deliver_required 无论多大都同步；deliver_only 才看大小；
deliver 由**可配置规则**（环境变量里的 fnmatch 模式）决定，框架不内置任何扩展名判据。
（原先对业务脚本源码的“调用点选对了函数”扫描属于业务测试，不迁移。）
"""

import atexit
import shutil
import tempfile
from pathlib import Path


from webx_worker.sdk import upload_queue as q  # noqa: E402
from webx_worker.sdk import upload_route as R  # noqa: E402

_MB = 1024 * 1024


class _Env:
    def __init__(self, **kv):
        self._kv, self._old = kv, {}

    def __enter__(self):
        import os
        for k, v in self._kv.items():
            self._old[k] = os.environ.get(k)
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        return self

    def __exit__(self, *_):
        import os
        for k, v in self._old.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v


class _Sync:
    """替换同步上传，记录调用。``ok=False`` 模拟上传失败。"""

    def __init__(self, ok=True):
        self.ok, self.calls = ok, []

    def __call__(self, task_id, path, filename=None):
        self.calls.append(filename)
        if not self.ok:
            raise RuntimeError("boom")
        return {"download_url": f"/public/tasks/{task_id}/results/{filename}"}


def _bed(size_mb: float):
    """建一个临时 work_dir + 指定大小的产物文件。

    两处刻意为之：
      · **稀疏文件**（truncate 而非 write_bytes）：分流只看 ``stat().st_size``，
        没人读内容 —— 真写 65MB 只是白花磁盘与时间。
      · **atexit 清理**：这些床位曾经一个不删地留在 /tmp，几天攒下数 GB。
    """
    d = Path(tempfile.mkdtemp())
    atexit.register(shutil.rmtree, d, True)
    work = d / "work"
    q.ensure_dirs(work)
    f = d / "art.bin"
    with open(f, "wb") as fh:
        fh.truncate(int(size_mb * _MB))
    return work, f


def _run(fn, work, f, *, enabled=True, threshold="15", sync_ok=True):
    stub = _Sync(ok=sync_ok)
    old = R.upload_result
    R.upload_result = stub
    try:
        with _Env(WEBX_WORKER_UPLOADER_ENABLED="true" if enabled else "false",
                  WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB=threshold,
                  WEBX_WORKER_STORAGE_INTERNAL_URL="http://storage.invalid",
                  WEBX_WORKER_WEB_BASE_URL="http://web.invalid",
                  WEBX_WORKER_WORK_DIR=str(work)):
            return fn("t1", f, "art.bin", ["step2", "out_url"]), stub
    finally:
        R.upload_result = old


# ==================== 一、行为层 ====================


def test_no_storage_configured_forces_sync():
    """未配存储服务地址 → 一律同步上传，**绝不投队列**。

    协议规定产物只上传到存储服务，没有回退路径。没有存储地址时投进队列的产物永远传不上去，
    只会按队列「永不放弃」的语义无限重试 —— 所以正确行为是根本不投。
    """
    for storage in (None, ""):                          # 未配 / 配成空
        work, f = _bed(50)                              # 远超阈值，正常情况下必走 uploader
        stub = _Sync()
        old_u, R.upload_result = R.upload_result, stub
        R._warned_no_storage = False                    # 告警只发一次，逐用例复位
        try:
            with _Env(WEBX_WORKER_UPLOADER_ENABLED="true", WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB="15",
                      WEBX_WORKER_STORAGE_INTERNAL_URL=storage, WEBX_WORKER_STORAGE_URL=None,
                      WEBX_WORKER_WEB_BASE_URL="http://web.invalid",
                      WEBX_WORKER_WORK_DIR=str(work)):
                url = R.deliver_only("t1", f, "art.bin", ["a", "u"])
        finally:
            R.upload_result = old_u
        assert q.iter_pending(work) == [], f"storage={storage!r} 时不该投队列"
        assert stub.calls == ["art.bin"], f"storage={storage!r} 时应同步上传"
        assert url.startswith("/public/")


def test_public_storage_url_alone_also_enables_uploader():
    """内网地址缺省时回退对外地址（与 sdk.result_client 的回退链一致）。"""
    work, f = _bed(50)
    stub = _Sync()
    old_u, R.upload_result = R.upload_result, stub
    try:
        with _Env(WEBX_WORKER_UPLOADER_ENABLED="true", WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB="15",
                  WEBX_WORKER_STORAGE_INTERNAL_URL=None,
                  WEBX_WORKER_STORAGE_URL="https://files.example.com",
                  WEBX_WORKER_WORK_DIR=str(work)):
            R.deliver_only("t1", f, "art.bin", ["a", "u"])
    finally:
        R.upload_result = old_u
    assert len(q.iter_pending(work)) == 1


def test_storage_configured_enables_uploader():
    """配了存储服务 → 大的纯交付产物才走 uploader（与上一条互为对照）。"""
    work, f = _bed(50)
    stub = _Sync()
    old_u, R.upload_result = R.upload_result, stub
    try:
        with _Env(WEBX_WORKER_UPLOADER_ENABLED="true", WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB="15",
                  WEBX_WORKER_STORAGE_INTERNAL_URL="http://storage.invalid",
                  WEBX_WORKER_WEB_BASE_URL="http://web.invalid",
                  WEBX_WORKER_WORK_DIR=str(work)):
            url = R.deliver_only("t1", f, "art.bin", ["a", "u"])
    finally:
        R.upload_result = old_u
    assert len(q.iter_pending(work)) == 1, "配了 storage 就该走队列"
    assert stub.calls == [] and url == "", "进队列时不同步传、字段留空"


def test_required_stays_sync_even_when_huge():
    """★ 下游依赖的产物，再大也同步——不看阈值。"""
    work, f = _bed(64)                       # 远超 15MB 阈值
    url, stub = _run(R.deliver_required, work, f)
    assert stub.calls == ["art.bin"], "required 产物没走同步上传"
    assert url.startswith("/public/"), url
    assert q.iter_pending(work) == [], "required 产物被投进了队列"


def test_delivery_large_goes_to_uploader():
    """纯交付且超阈值 → 进队列，字段**留空**（不写任何占位值）。"""
    work, f = _bed(64)
    url, stub = _run(R.deliver_only, work, f)
    assert stub.calls == [], "大的纯交付产物不该同步上传"
    assert url == "", f"进队列时字段应留空，实为 {url!r}"
    items = q.iter_pending(work)
    assert len(items) == 1
    assert items[0].key_path == ["step2", "out_url"], "回写位置没随条目投进队列"


def test_delivery_small_stays_sync():
    """小的纯交付产物同步传掉——省一趟队列扫描与权威检查往返。"""
    work, f = _bed(1)
    url, stub = _run(R.deliver_only, work, f)
    assert stub.calls == ["art.bin"]
    assert url.startswith("/public/")
    assert q.iter_pending(work) == []


def test_threshold_is_configurable():
    """阈值可配：内网千兆与公网 10Mbps 的合理值差两个数量级。"""
    work, f = _bed(5)
    url, stub = _run(R.deliver_only, work, f, threshold="1")   # 5MB > 1MB
    assert stub.calls == [] and url == "" and len(q.iter_pending(work)) == 1


def test_disabled_switch_forces_everything_sync():
    """★ 总开关关掉 = 完全回到旧行为，出问题先关它、不必回滚代码。"""
    work, f = _bed(64)
    url, stub = _run(R.deliver_only, work, f, enabled=False)
    assert stub.calls == ["art.bin"]
    assert q.iter_pending(work) == []


def test_missing_work_dir_forces_sync():
    """★ 没注入 WORK_DIR（旧 executor / 直接调试）→ 退回同步，绝不投进没人消费的队列。"""
    work, f = _bed(64)
    stub = _Sync()
    old, R.upload_result = R.upload_result, stub
    try:
        with _Env(WEBX_WORKER_UPLOADER_ENABLED="true", WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB="15",
                  WEBX_WORKER_STORAGE_INTERNAL_URL="http://storage.invalid",
                  WEBX_WORKER_WEB_BASE_URL="http://web.invalid",
                  WEBX_WORKER_WORK_DIR=None):
            url = R.deliver_only("t1", f, "art.bin", ["a", "u"])
    finally:
        R.upload_result = old
    assert stub.calls == ["art.bin"] and url.startswith("/public/")


def test_sync_failure_degrades_to_queue():
    """★ 同步失败（太大 / 重试超时）降级进队列——比改造前"写空串、产物永久丢失"严格改善。"""
    work, f = _bed(1)
    url, stub = _run(R.deliver_only, work, f, sync_ok=False)
    assert stub.calls == ["art.bin"], "应先尝试过同步"
    assert url == ""
    assert len(q.iter_pending(work)) == 1, "同步失败后应转交队列"


def test_required_failure_also_degrades_to_queue():
    """required 同步失败同样降级：落空串对下游本来就是「暂时没有」，进队列只是多一条自愈的路。"""
    work, f = _bed(64)
    url, _ = _run(R.deliver_required, work, f, sync_ok=False)
    assert url == "" and len(q.iter_pending(work)) == 1


def test_sync_failure_without_uploader_returns_empty():
    """uploader 关闭时，同步失败仍是空串且不进队列——与改造前逐字节一致。"""
    work, f = _bed(1)
    url, _ = _run(R.deliver_only, work, f, enabled=False, sync_ok=False)
    assert url == "" and q.iter_pending(work) == []


def test_enqueue_failure_falls_back_to_sync():
    """投队列失败 → 退回同步，别让产物无声消失。"""
    work, f = _bed(64)
    stub = _Sync()
    old_u, old_e = R.upload_result, R.q.enqueue
    R.upload_result = stub
    R.q.enqueue = lambda *a, **k: None
    try:
        with _Env(WEBX_WORKER_UPLOADER_ENABLED="true", WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB="15",
                  WEBX_WORKER_STORAGE_INTERNAL_URL="http://storage.invalid",
                  WEBX_WORKER_WEB_BASE_URL="http://web.invalid",
                  WEBX_WORKER_WORK_DIR=str(work)):
            url = R.deliver_only("t1", f, "art.bin", ["a", "u"])
    finally:
        R.upload_result, R.q.enqueue = old_u, old_e
    assert stub.calls == ["art.bin"] and url.startswith("/public/")


def test_configurable_rule_decides_required_vs_only():
    """★ 可配置规则：命中 WEBX_WORKER_UPLOAD_REQUIRED_GLOBS 的远端文件名恒同步，其余按大小。

    框架不内置任何扩展名判据——哪些文件是「下游依赖」只有业务知道。
    """
    work, big = _bed(64)
    stub = _Sync()
    old = R.upload_result
    R.upload_result = stub
    try:
        with _Env(WEBX_WORKER_UPLOADER_ENABLED="true", WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB="15",
                  WEBX_WORKER_STORAGE_INTERNAL_URL="http://storage.invalid",
                  WEBX_WORKER_UPLOAD_REQUIRED_GLOBS="*.keep, input_*",
                  WEBX_WORKER_WORK_DIR=str(work)):
            assert R.is_required("a.keep") and R.is_required("input_1.dat")
            assert not R.is_required("a.dat")
            url_req = R.deliver("t1", big, "x.keep", ["k", "u"])       # 命中 → 同步
            n_after_req = len(q.iter_pending(work))
            url_only = R.deliver("t1", big, "x.dat", ["k2", "u"])      # 未命中 → 大文件走队列
    finally:
        R.upload_result = old
    assert url_req.startswith("/public/") and n_after_req == 0
    assert url_only == "" and len(q.iter_pending(work)) == 1
    assert stub.calls == ["x.keep"]


def test_no_rule_configured_means_everything_is_only():
    with _Env(WEBX_WORKER_UPLOAD_REQUIRED_GLOBS=""):
        assert R.required_globs() == [] and not R.is_required("anything.bin")


def test_missing_file_returns_empty():
    work, _ = _bed(1)
    url, stub = _run(R.deliver_only, work, work / "nope.bin")
    assert url == "" and stub.calls == []


