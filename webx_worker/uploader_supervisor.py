"""uploader 进程的守护 —— 由 worker 父进程拉起、看住、退出时收走。

【为什么由父进程 spawn，而不是独立 systemd unit】
  · 生命周期必须与 worker 一致：独立 unit 会造出"worker 停了 uploader 还在传"的状态
  · 配置只有一份：token / URL / WORK_DIR 全部由父进程按**真环境变量**注入
    （⚠️ 配置文件层 .env / 覆盖层只存在于父进程内存，**传不到子进程**，
      故必须显式传 env，不能指望子进程自己读到同一份配置）
  · --work-dir / --ext-env 这类命令行覆盖只存在于父进程内存里，子进程自读配置会拿到旧值

【重启不需要恢复任何状态】uploader 的全部状态都在磁盘队列上。这是选磁盘队列而非内存
队列的核心红利：崩了就重启，起来接着扫目录，没有 snapshot、没有交接、没有丢失窗口。
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from . import envnames as E
from ._paths import with_package_on_pythonpath
from .config import storage_configured, worker_settings

logger = logging.getLogger(__name__)

# 崩溃重启退避：1→2→4→…→30s 封顶。
# 封顶是为了让"配置错了导致起来就死"这种情况不至于每秒烧一个进程，
# 又不至于久到真故障修好后迟迟不恢复。
_BACKOFF_START = 1.0
_BACKOFF_MAX = 30.0

# 退出宽限期：让在途分片写完并把 offset 落盘。不等它传完——传不完下次启动接着传。
_TERM_GRACE_SEC = 10.0


def _child_env() -> dict:
    """构造子进程环境：继承当前环境，再用父进程**已定版**的配置显式覆盖。

    显式覆盖的这几项，正是 worker_settings 对应字段的 env 名——子进程的
    WorkerSettings 会优先读真环境变量，于是与父进程完全一致。
    """
    env = dict(os.environ)
    env.update({
        E.ENV_WORK_DIR: worker_settings.WORK_DIR,
        E.ENV_WEB_BASE_URL: worker_settings.WEB_BASE_URL,
        E.ENV_INTERNAL_TOKEN: worker_settings.INTERNAL_TOKEN,
        E.ENV_STORAGE_INTERNAL_TOKEN: worker_settings.STORAGE_INTERNAL_TOKEN,
        E.ENV_STORAGE_URL: worker_settings.STORAGE_URL,
        E.ENV_STORAGE_INTERNAL_URL: worker_settings.STORAGE_INTERNAL_URL,
        "WEBX_WORKER_UPLOADER_CONCURRENCY": str(worker_settings.UPLOADER_CONCURRENCY),
        "WEBX_WORKER_UPLOADER_SCAN_INTERVAL": str(worker_settings.UPLOADER_SCAN_INTERVAL),
        "WEBX_WORKER_LOG_DIR": worker_settings.LOG_DIR or "",
    })
    # 子进程要能 import 本包（包可能没装进解释器，只是靠 sys.path 找到的）
    return with_package_on_pythonpath(env)


class UploaderSupervisor:
    """拉起并守护 uploader 子进程；崩溃自动重启（退避），停止时优雅收走。"""

    def __init__(self) -> None:
        self._proc: asyncio.subprocess.Process | None = None
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    # ─── 生命周期 ────────────────────────────────────────────────

    def start(self) -> None:
        if not worker_settings.UPLOADER_ENABLED:
            logger.info("[UploaderSup] 已禁用（WEBX_WORKER_UPLOADER_ENABLED=false），不拉起")
            return
        # 没有存储服务地址 → 拉起来也无处可传（见 config.storage_configured）。
        # 与投递侧的闸对称：那边不再投新条目，这边不再拉起进程。
        # 队列里**已有的**旧条目不会自己消失，故把积压说出来，否则它们会静默留在盘上。
        if not storage_configured():
            backlog = pending_backlog_bytes()
            logger.error(
                "[UploaderSup] 未配置存储服务（%s / %s），不拉起 uploader —— "
                "产物只能上传到存储服务。大产物已自动改走同步上传；队列现有积压 %.1f MB，"
                "需配好存储服务后重启 worker 方能补传。",
                E.ENV_STORAGE_INTERNAL_URL, E.ENV_STORAGE_URL, backlog / 1048576,
            )
            return
        self._task = asyncio.create_task(self._supervise(), name="uploader-supervisor")

    async def _supervise(self) -> None:
        backoff = _BACKOFF_START
        while not self._stopping.is_set():
            try:
                self._proc = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", "webx_worker", "uploader",
                    env=_child_env(),
                    stdout=asyncio.subprocess.DEVNULL,   # 日志由子进程自己落盘
                    stderr=asyncio.subprocess.DEVNULL,
                )
            except Exception as e:  # noqa: BLE001
                logger.error(f"[UploaderSup] 拉起失败: {e}", exc_info=True)
                await self._sleep_or_stop(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)
                continue

            logger.info(f"[UploaderSup] uploader 已启动 pid={self._proc.pid}")
            rc = await self._proc.wait()
            self._proc = None

            if self._stopping.is_set():
                logger.info(f"[UploaderSup] uploader 随停止流程退出 rc={rc}")
                return

            # 活过一段时间再崩 → 重置退避（区分"起来就死"与"跑了很久才崩"）
            logger.warning(f"[UploaderSup] uploader 异常退出 rc={rc}，{backoff:.0f}s 后重启")
            await self._sleep_or_stop(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX)

    async def _sleep_or_stop(self, sec: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=sec)
        except asyncio.TimeoutError:
            pass

    async def stop(self) -> None:
        """SIGTERM + 宽限期 + SIGKILL 兜底。幂等。"""
        self._stopping.set()
        proc = self._proc
        if proc is not None and proc.returncode is None:
            try:
                proc.send_signal(signal.SIGTERM)
                await asyncio.wait_for(proc.wait(), timeout=_TERM_GRACE_SEC)
            except asyncio.TimeoutError:
                logger.warning("[UploaderSup] uploader 未在宽限期内退出，强杀")
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            except ProcessLookupError:
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("[UploaderSup] 已停止")


# ==================== 背压 ====================


def pending_backlog_bytes() -> int:
    """当前队列积压字节数。读磁盘目录，无需与 uploader 通信。"""
    try:
        from .sdk import upload_queue as q
        return q.pending_bytes(worker_settings.WORK_DIR)
    except Exception as e:  # noqa: BLE001 —— 观测失败绝不能影响接任务
        logger.debug(f"[UploaderSup] 读取积压失败: {e}")
        return 0


def is_backpressured() -> bool:
    """积压是否已超过水位——父进程接新任务前问一次。

    算得比传得快时队列会无限堆积，而队列里的文件不能删（清理侧要跳过），磁盘会被撑爆。
    这是唯一会真正拖垮机器的失败模式，故必须有闸。
    """
    limit_gb = worker_settings.UPLOADER_PENDING_LIMIT_GB
    if limit_gb <= 0:
        return False
    backlog = pending_backlog_bytes()
    over = backlog > limit_gb * 1024 ** 3
    if over:
        logger.warning(
            f"[UploaderSup] 上传积压 {backlog / 1024**3:.1f}GB 已超水位 {limit_gb}GB，暂缓接新任务"
        )
    return over
