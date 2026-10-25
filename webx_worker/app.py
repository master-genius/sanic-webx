"""
Worker 主程序 — 多 Lane 并发版（`python -m webx_worker [run]`）

架构说明：
  - 单进程运行，Lane 间并发、Lane 内串行
  - worker 主动向领取服务领取任务（有空槽才领，天然背压）
  - Lane 分组由 channel 配置文件的 LANES 决定；空 LANES = 所有 channel 归入一条 Lane
  - 能力声明 = CHANNELS 的键（task_type）经 Lane 映射，随每次领取请求带给服务端

启动顺序（同一 asyncio.run 入口）：
  1. 构建 LaneManager（空 LANES → 单 Lane）
  2. PendingReporter（失败上报的持久重放队列）+ IPC server（接子进程的失败事件）
  3. 启动恢复：灌回上次 snapshot，再扫工作目录的 ok.json 与服务端协商（Recovery）
  4. 拉起 uploader 子进程，启动领取分发循环 + 后台清理
  5. 收到 SIGTERM/SIGINT：先同步捕获令牌 → 同步取消在跑任务 → 用捕获的令牌归还租约，
     然后停 uploader、落 PendingReporter 快照
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from .broker import run_worker
from .callback import callback_client
from .channel_loader import (
    ensure_config_exists,
    load_channel_config,
    resolve_config_path,
)
from .config import (
    apply_env_overlay,
    default_overlay_for,
    derive_worker_id,
    load_base_env,
    worker_home,
    worker_settings,
)
from .executor import GenericTaskExecutor
from .heartbeat import HeartbeatManager
from .http_file_client import http_file_client
from .ipc_server import IPCServer
from .lane_manager import LaneManager
from .link_health import selfcheck_and_report
from .log_config import setup_logging
from .log_writer import ConcurrentLogWriter, build_redact_roots
from .pending_reporter import PendingReporter, set_global_reporter
from .plugin import TaskKindPlugin
from .recovery import WorkerRecovery
from .uploader_supervisor import UploaderSupervisor

logger = logging.getLogger(__name__)


def build_executor(
    channels: dict,
    conda_root: str,
    ipc_sock_path: str | None = None,
    plugin: TaskKindPlugin | None = None,
    script_root: str | Path | None = None,
) -> GenericTaskExecutor:
    """构建共享的 GenericTaskExecutor 实例。"""
    plugin = plugin or TaskKindPlugin()
    log_writer = ConcurrentLogWriter(
        head_lines=worker_settings.LOG_HEAD_LINES,
        tail_lines=worker_settings.LOG_TAIL_LINES,
        # 前端可见日志的路径脱敏根：此处 WORK_DIR / conda_root / channels 三值均已定版
        # （--work-dir 覆盖与 load_channel_config 都在 main() 里先于 async_main 完成），
        # 构造期一次性传入，保证增量日志从第一次推送起就已脱敏。
        redact_roots=build_redact_roots(
            work_dir=worker_settings.WORK_DIR,
            conda_root=conda_root,
            channels=channels,
            script_root=script_root,
        ),
        # 业务脱敏函数注册表：存插件列表的引用（插件之后追加的函数同样生效）
        redactors=plugin.redactors,
    )

    hb = HeartbeatManager(
        interval=worker_settings.HEARTBEAT_INTERVAL,
    )

    executor = GenericTaskExecutor(
        callback=callback_client,
        log_writer=log_writer,
        heartbeat=hb,
        http_client=http_file_client,
        work_base_dir=worker_settings.WORK_DIR,
        channels_config=channels,
        conda_root=conda_root,
        ipc_sock_path=ipc_sock_path,
        plugin=plugin,
        script_root=script_root,
    )

    return executor


def build_handlers(
    executor: GenericTaskExecutor, channels: dict,
) -> dict[str, callable]:
    """构建 channel -> handler 路由表，所有 channel 共享同一 executor。"""
    handlers: dict[str, callable] = {}

    for channel_name in channels:
        async def wrapper(
            task_id: str, payload: dict, ex: GenericTaskExecutor = executor
        ):
            await ex.execute(task_id, payload)

        handlers[channel_name] = wrapper
        print(f"[+] 注册 channel: {channel_name}")

    return handlers


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m webx_worker run", description="webx_worker — 多 Lane 并发",
    )
    parser.add_argument(
        "--home",
        type=str,
        default=None,
        help=(
            "worker 主目录（存放 .env 与 channel_config/）。"
            "缺省取环境变量 WEBX_WORKER_HOME，再缺省为当前目录。"
        ),
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help=(
            "channel 配置文件（默认: <home>/channel_config/default.py）。"
            "裸文件名 → 到 <home>/channel_config/ 目录下查找（如 --config gpu0）；"
            "带路径 → 直接读取该路径。"
        ),
    )
    parser.add_argument(
        "--ext-env",
        type=str,
        default=None,
        metavar="FILE",
        help=(
            "实例环境覆盖层文件，叠加在 <home>/.env 之上（继承 + 覆盖 + 新增）。"
            "不传时按约定自动探测 <channel 配置同目录>/<配置名>_env"
            "（如 --config gpu0 → channel_config/gpu0_env），存在即加载、不存在静默跳过；"
            "显式指定则文件必须存在，否则启动失败。"
        ),
    )
    parser.add_argument(
        "--work-dir",
        type=str,
        default=None,
        help=(
            "任务工作目录根路径（覆盖 WEBX_WORKER_WORK_DIR）。"
            "支持 ~ 展开；不传则沿用 .env / 环境变量配置。"
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="启用调试模式：DEBUG 级别日志",
    )
    parser.add_argument(
        "--log",
        type=str,
        default=None,
        metavar="DIR",
        help=(
            "后台日志目录（覆盖 WEBX_WORKER_LOG_DIR）。"
            "按 worker_task_{YYYY-MM-DD-HH}_{pid}.log 生成，单文件 ≤10MB 或 ≤20000 行滚动。"
            "文件名以 PID 结尾，多实例可安全共用同一目录。"
            "不传则沿用 .env；均未配置时仅输出到控制台。"
        ),
    )
    # 用 parse_known_args 容忍未知参数：识别到不支持的参数跳过即可，不强制报错退出
    # （便于部署侧包装脚本统一透传，新增/移除 flag 不互相牵制）
    args, unknown = parser.parse_known_args(argv)
    # 延迟到 setup_logging 之后再告警，确保这条也落进 --log 文件（此刻日志尚未配置）
    args.unknown_args = unknown
    return args


async def async_main(
    channels: dict,
    lanes: list[list[str]],
    conda_root: str,
    plugin: TaskKindPlugin | None = None,
    script_root: str | Path | None = None,
):
    """统一异步入口：Recovery + IPC server + PendingReporter + dispatcher + Lane workers。"""

    # 兜底：捕获事件循环内未处理异常，记入日志（后台服务排障刚需）
    def _loop_exc_handler(loop, context):
        msg = context.get("message", "事件循环未处理异常")
        exc = context.get("exception")
        if exc is not None:
            logger.error(f"[Main] {msg}", exc_info=exc)
        else:
            logger.error(f"[Main] {msg}: {context}")

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_loop_exc_handler)

    # ── 停止信号：**没有这段，下面那个 finally 在 systemd 下一行都跑不到** ──────
    #
    # systemd 的 stop/restart 按默认 KillMode=control-group 发 **SIGTERM**。
    # 而 CPython 只为 SIGINT 装了 Python 级 handler（→ KeyboardInterrupt），
    # **SIGTERM 保持内核默认动作**：进程立即终止，`finally` 与 `atexit` 全部不执行
    # （实测 exit=143，收尾零执行）。于是在手租约不会主动归还、失败上报队列不会落盘。
    #
    # 语义上**不等在跑的任务**：它们随 cgroup 一起被 SIGTERM 掉，任务回池重跑
    # （租约模型保证这样做是安全的）。这里只争取几秒钟把「自己的账本」写完。
    # 信号只置 Event，真正的收尾在协程里做。
    stopping = asyncio.Event()

    def _on_signal(signame: str) -> None:
        if stopping.is_set():
            # **第二次信号 = 强退**。装了 handler 之后 SIGINT 不再抛 KeyboardInterrupt，
            # 少了这条逃生口，收尾一旦卡住就再也 Ctrl-C 不出来了。
            logger.warning("[Main] 再次收到 %s，放弃收尾直接退出", signame)
            os._exit(1)
        logger.info("[Main] 收到 %s，开始收尾", signame)
        stopping.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig.name)
        except NotImplementedError:      # 非 Unix：退化为无优雅停止
            pass

    # 1. 构建 LaneManager（空 LANES → 所有已声明 channel 归入一条 Lane）
    lane_manager = LaneManager(lanes, channel_types=channels.keys())

    # 2. 构建 PendingReporter（汇报失败事件的后台队列消费者）
    #    需在 executor 之前构建，因为 callback 失败时会通过全局函数访问它
    work_base = Path(worker_settings.WORK_DIR)
    work_base.mkdir(parents=True, exist_ok=True)
    pending_reporter = PendingReporter(
        callback=callback_client,
        work_base_dir=worker_settings.WORK_DIR,
    )
    set_global_reporter(pending_reporter)

    # 3. 启动 IPC server（接子进程通过 send_report 发来的失败事件）
    ipc_sock_path = work_base / "ipc.sock"
    ipc_server = IPCServer(sock_path=ipc_sock_path, reporter=pending_reporter)
    await ipc_server.start()

    # 4. 启动恢复：先把上次 snapshot + 各任务降级文件灌回队列
    restored = await pending_reporter.restore_from_snapshot()
    if restored:
        logger.info(f"[Main] PendingReporter 启动恢复 {restored} 条历史事件")

    # 内部回调链路自检：把 TLS + 路由 + token 探通一次。
    # 证书/令牌/反代这类开机就存在的故障，不自检则要等"任务投递之后"才暴露
    # （表现为一批任务莫名卡在 queued）。
    #
    # ⏱ 刻意放到【后台任务】里跑，绝不 await：自检是观测，不该占用启动路径。
    # 自检失败也不阻止启动，理由见 link_health.check_web_link 的 docstring。
    selfcheck_task = asyncio.create_task(
        selfcheck_and_report(callback_client), name="link-selfcheck"
    )

    # 5. 构建 executor（注入 ipc_sock_path 给子进程）
    executor = build_executor(
        channels, conda_root, ipc_sock_path=str(ipc_sock_path),
        plugin=plugin, script_root=script_root,
    )

    # 6. 执行 Recovery（启动恢复扫描 ok.json）
    recovery = WorkerRecovery(
        callback=callback_client,
        work_base_dir=worker_settings.WORK_DIR,
        plugin=plugin,
    )
    await recovery.recover()

    # 6.5 拉起 uploader 子进程（大产物异步上传，让 Lane 提前释放）。
    #     刻意放在 recover() 之后：恢复扫描可能重排产物，先把队列现场理清再开传。
    #     它自己扫磁盘队列，父进程不与它通信——拉起、看住、退出时收走，仅此三件事。
    uploader_sup = UploaderSupervisor()
    uploader_sup.start()

    # 7. 构建 handlers 并启动领取分发 + PendingReporter consumer
    handlers = build_handlers(executor, channels)

    reporter_task = asyncio.create_task(pending_reporter.run_loop(), name="pending_reporter")

    try:
        # run_worker 起成 task 才能被取消——它自己 `except CancelledError` 之后会
        # 走到 dispatcher.release_all()，正是我们要的收尾。
        worker_task = asyncio.create_task(
            run_worker(handlers, lane_manager, executor), name="run-worker"
        )
        stop_task = asyncio.create_task(stopping.wait(), name="stop-signal")
        await asyncio.wait({worker_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        stop_task.cancel()
        if worker_task.done():
            # ⚠️ **必须 result()**：run_worker 起成 task 之后，它抛的异常会被 task
            # 兜住，不 retrieve 的话进程会「干净地」退出 0 —— 把一次崩溃伪装成正常停机。
            worker_task.result()
        else:
            logger.info("[Main] 收到停止信号，开始收尾（**不等待在跑的任务**）")
            # ── 先停领取，再三步，顺序本身就是判据，一步都不能换 ────────────────────────
            #
            # 要防的倒退：systemd 按 cgroup 一并 SIGTERM，**在跑任务的子进程同时被杀**。
            # executor 看到的只是「子进程非零退出」，于是走普通失败分支去调 `fail_task`
            # —— 而那一刻租约还在自己手上，fencing 会**放行**。结果是一次运维重启
            # 把一个本可自动回池重跑的任务判成 failed，要用户手动 retry。
            #
            # ① 同步捕获令牌：② 的取消会触发 _run_one 的 finally → fence_unregister，
            #    注册表随之清空；③ 若还指望请求钩子现取令牌就会空手，归还反被拒。
            # ② **同步**取消在跑任务：cancel() 在本轮任何 await 之前生效，那条失败链
            #    下次被调度时拿到的是 CancelledError，走 execute 的**中断分支**
            #    （不写终态 ok.json、绝不上报 fail）。
            #    只做 ③ 是不够的——归还要 await，await 期间失败链照样在推进，
            #    两个请求并发，先后仍是概率问题。有了 ② 才是结构保证。
            # ③ 用捕获到的令牌归还，任务立刻回池等人来领。
            from .lease import (
                begin_drain,
                cancel_running_tasks,
                held_fencing_headers,
                release_held_leases,
            )

            # ⓪ 先**同步**停掉领取：否则 ③ 的 await 期间，空闲 Lane 会把刚归还的任务又领回来，
            #    任务停在 queued 等 60s 短租约才回池（见 begin_drain）。
            begin_drain()
            tokens = held_fencing_headers()
            cancelled = cancel_running_tasks()
            if cancelled:
                logger.info(
                    "[Main] 已取消 %d 个在跑任务（走中断分支，不上报失败）", cancelled
                )
            await release_held_leases(tokens)
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
    finally:
        # uploader 先停：给它宽限期把在途分片落盘，传不完的下次启动接着传
        await uploader_sup.stop()
        # 自检是后台观测任务：退出时取消，绝不让它拖住关闭流程
        if not selfcheck_task.done():
            selfcheck_task.cancel()
            try:
                await selfcheck_task
            except (asyncio.CancelledError, Exception):
                pass

        # 停止所有心跳
        await executor.heartbeat.shutdown_all()

        # 优雅停止 PendingReporter + IPC server，并 snapshot 当前队列
        pending_reporter.request_stop()
        reporter_task.cancel()
        try:
            await reporter_task
        except (asyncio.CancelledError, Exception):
            pass

        await ipc_server.close()

        # 把内存里没处理完的事件 dump 到文件（排除 fail_task）
        try:
            pending_reporter.snapshot_to_file()
        except Exception as e:
            logger.error(f"[Main] PendingReporter snapshot 失败: {e}", exc_info=True)


def main(argv: list[str] | None = None):
    args = parse_args(argv)

    # ── 主目录：.env 与 channel_config/ 的锚点 ──────────────────────────
    if args.home:
        os.environ["WEBX_WORKER_HOME"] = str(Path(args.home).expanduser().resolve())
    home = worker_home()

    # ── 基础层 .env，再叠实例环境覆盖层 ────────────────────────────────
    # load_base_env / apply_env_overlay 会用新构造的 WorkerSettings 整体刷新单例，
    # 故必须先于下方所有命令行覆盖（--debug/--log/--work-dir），
    # 否则那些覆盖会被冲掉。详见 config.apply_env_overlay 的约束说明。
    base_loaded = load_base_env(home / ".env")

    # 路径来源：--ext-env 显式指定 > 按 --config 约定推导（<配置名>_env）。
    # 约定探测缺省是常态（静默跳过），显式指定缺失则 fail-fast —— 后者说明
    # 部署方明确期望这份配置生效，静默降级只会让实例带着错配置跑起来。
    config_path = resolve_config_path(args.config, home)
    if args.ext_env:
        overlay_loaded = apply_env_overlay(args.ext_env)
        if overlay_loaded is None:
            print(
                f"[Worker] 致命错误：--ext-env 指定的文件不存在: {args.ext_env}",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        overlay_loaded = apply_env_overlay(default_overlay_for(config_path))

    # 派生 worker 实例标识：必须在覆盖层之后（那里会整体重建 WorkerSettings），
    # 依据是 --config 的裸名 —— 同机多实例靠各自的配置名天然区分，
    # 每台机器只需设一个 WEBX_WORKER_PREFIX。详见 config.derive_worker_id。
    derive_worker_id(config_path.stem)

    if args.debug:
        worker_settings.DEBUG = True

    # 配置日志：命令行 --log > 覆盖层/.env/环境变量 WEBX_WORKER_LOG_DIR > 默认（仅控制台）。
    # 尽早调用，确保后续启动期的 logger 输出都被捕获。
    log_dir = args.log or worker_settings.LOG_DIR or None
    effective_log_dir = setup_logging(
        log_dir=log_dir,
        debug=worker_settings.DEBUG,
        max_bytes=worker_settings.LOG_MAX_BYTES,
        max_lines=worker_settings.LOG_MAX_LINES,
    )

    # 日志已就绪：此刻起 print/logger 均落盘。补告之前暂存的「未知参数」告警（不漏）。
    if getattr(args, "unknown_args", None):
        logger.warning("[Worker] 忽略不支持的参数: %s", " ".join(args.unknown_args))

    # --work-dir 覆盖 .env：展开 ~ 并归一化为绝对路径，
    # 后续 executor / recovery / cleanup 子进程都读 worker_settings.WORK_DIR
    if args.work_dir:
        worker_settings.WORK_DIR = str(Path(args.work_dir).expanduser().resolve())

    # config_path 已在函数开头解析（覆盖层探测依赖它），此处直接用
    if not ensure_config_exists(config_path):
        sys.exit(0)

    cfg = load_channel_config(config_path)
    channels = cfg["CHANNELS"]
    lanes = cfg.get("LANES") or []
    conda_root = cfg["CONDA_ROOT"]
    plugin = cfg["PLUGIN"]
    script_root = cfg["SCRIPT_ROOT"]

    print(f"\n{'=' * 60}")
    print("  webx_worker — 多 Lane 并发版")
    print(f"{'=' * 60}")
    print(f"[+] 主目录:    {home}")
    print(f"[+] 基础配置:  {base_loaded or '(无 .env)'}")
    print(f"[+] 配置文件:  {config_path}")
    print(f"[+] 环境覆盖层: {overlay_loaded or '(无)'}")
    print(f"[+] Channel:  {list(channels.keys())}")
    print(f"[+] Lane 分组: {lanes or '单 Lane 模式（所有 channel 归入一条 Lane）'}")
    print(f"[+] 工作目录: {worker_settings.WORK_DIR}")
    # 两条通道分别列出：领取（HTTP，主动去要任务）、回调（HTTP，报状态）。
    # 任一条坏掉都会让任务卡住，但症状完全不同，故不合并成一行"服务端地址"。
    print(f"[+] Worker ID:  {worker_settings.WORKER_ID}")
    print(f"[+] 领取通道:  {worker_settings.TASK_SERVER_URL or worker_settings.WEB_BASE_URL}（主动领任务）")
    print(f"[+] 回调通道:  {worker_settings.WEB_BASE_URL}（报状态，启动时自检）")
    print("[+] 领取模型:  Lane=槽位，跑完才领下一个（无预取、无本地队列）")
    print("[+] running 上报: 3 次重试，全失败则放弃执行")
    print("[+] 租约自熔:  续租持续失败到期即自行终止子进程（限制分区下的算力浪费）")
    print("[+] 子进程状态检测: 非 running 立即终止（防双重执行）")
    print(f"[+] DEBUG 模式: {'开启' if worker_settings.DEBUG else '关闭'}")
    print(f"[+] 日志输出:  {effective_log_dir or '仅控制台'}")
    print(f"{'=' * 60}\n")

    asyncio.run(async_main(channels, lanes, conda_root, plugin, script_root))
