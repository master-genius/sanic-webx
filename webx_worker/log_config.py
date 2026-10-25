"""Worker 日志配置模块。

提供后台服务模式下的文件日志能力：

  - 文件名按小时命名 + 进程 PID 结尾：
        worker_task_{YYYY-MM-DD-HH}_{pid}.log（如 worker_task_2026-06-12-08_31245.log）
    PID 放在**最后**是刻意的：日期在前，`ls` / 字典序天然按时间排序，一眼看到时间段；
    PID 只作为并发去重后缀。多个 worker 实例共用同一日志目录时互不覆盖。
  - 单文件双上限：≤ 10MB 或 ≤ 20000 行，任一先达到即切新文件
  - 同一小时内超限滚动： worker_task_{hour}_{pid}.1.log / .2.log ...
  - 同一进程内重开接续已有文件（按实际大小 / 行数初始化计数器，不丢计数）；
    ⚠️ 进程重启会换 PID → 落到新文件，不再接续旧文件。这是「共用目录不冲突」的代价，
    也让每个进程的日志天然独立、便于按 PID 归因。

setup_logging() 统一配置 root logger：
  - 控制台 handler 始终挂（保留前台可见性 / systemd 下进 journald）
  - log_dir 非空时把 sys.stdout/sys.stderr tee 到滚动文件：logging、print()、traceback、
    第三方直写 stdout 的内容全部一并落盘、与控制台完全镜像、零遗漏（不单独挂 file handler，
    文件写入统一经 tee，避免 logging 记录重复）
  - level：debug=True → DEBUG，否则 INFO（--debug 的 logger.debug 同步进文件）
  - 挂 sys.excepthook 兜底记录未捕获异常
"""

import logging
import os
import sys
import time
from pathlib import Path

# 默认双上限（可由 setup_logging 调用方覆盖）
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_MAX_LINES = 20000

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class HourlyRotatingFileHandler(logging.Handler):
    """按小时命名 + 大小/条数双上限滚动的文件日志 handler。

    切换规则（emit 内，单进程 asyncio + logging 自带锁，无并发竞争）：
      - 小时变化            → 关旧文件，开新小时文件（接续该小时已有最大 seq）
      - 同小时但写本条会超限 → seq+1 切新文件
    """

    def __init__(
        self,
        log_dir: str,
        prefix: str = "worker_task",
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_lines: int = DEFAULT_MAX_LINES,
        encoding: str = "utf-8",
        pid: int | None = None,
    ) -> None:
        super().__init__()
        self.log_dir = Path(log_dir).expanduser().resolve()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix
        # PID 后缀：多 worker 实例共用同一日志目录时的去重键。放在文件名末尾，
        # 使日期仍处于开头、字典序 == 时间序。pid=None 时取当前进程（仅测试会显式传值）。
        self.pid = os.getpid() if pid is None else pid
        self.max_bytes = max_bytes
        self.max_lines = max_lines
        self.encoding = encoding

        # 当前文件状态
        self._hour_key: str | None = None
        self._seq: int = 0
        self._stream = None
        self._cur_bytes: int = 0
        self._cur_lines: int = 0

    # ------------------------------------------------------------------
    # 文件路径 / 计数
    # ------------------------------------------------------------------

    def _path_for(self, hour_key: str, seq: int) -> Path:
        """seq=0 → worker_task_{hour}_{pid}.log；seq>0 → worker_task_{hour}_{pid}.{seq}.log。

        排序意图：`{hour}` 紧跟 prefix，`_{pid}` 垫在小时与 `.{seq}` 之间，
        故按文件名排序 == 按时间排序，PID 仅在同一小时内起区分作用。
        """
        stem = f"{self.prefix}_{hour_key}_{self.pid}"
        name = stem + (".log" if seq == 0 else f".{seq}.log")
        return self.log_dir / name

    def _latest_seq(self, hour_key: str) -> int:
        """返回该小时**本进程**已存在文件的最大 seq（无 seq=0 文件则返回 0）。

        因路径含 PID，扫描范围天然只覆盖本进程自己的文件 —— 共用目录时
        兄弟实例的文件既不会被误认、也不会引发 seq 竞争。
        """
        if not self._path_for(hour_key, 0).exists():
            return 0
        seq, n = 0, 1
        while self._path_for(hour_key, n).exists():
            seq, n = n, n + 1
        return seq

    @staticmethod
    def _count_lines(path: Path, encoding: str) -> int:
        """统计已有文件行数（仅在打开文件时调用一次，廉价）。"""
        try:
            with open(path, "r", encoding=encoding, errors="replace") as f:
                return sum(1 for _ in f)
        except OSError:
            return 0

    def _open(self, hour_key: str, seq: int) -> None:
        """以 append 模式打开 (hour_key, seq) 文件，并按实际内容初始化计数器。"""
        path = self._path_for(hour_key, seq)
        if path.exists():
            self._cur_bytes = path.stat().st_size
            self._cur_lines = self._count_lines(path, self.encoding)
        else:
            self._cur_bytes = 0
            self._cur_lines = 0
        self._stream = open(path, "a", encoding=self.encoding)
        self._hour_key = hour_key
        self._seq = seq

    # ------------------------------------------------------------------
    # 滚动逻辑
    # ------------------------------------------------------------------

    def _ensure_stream(self, record_created: float) -> None:
        """按 record 时间确保当前流指向正确的小时文件。"""
        hour_key = time.strftime("%Y-%m-%d-%H", time.localtime(record_created))
        if self._stream is not None and hour_key == self._hour_key:
            return
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        # 新小时：接续该小时已有最大 seq（重启场景），全新小时则 seq=0
        self._open(hour_key, self._latest_seq(hour_key))

    def _rollover_if_needed(self, msg_bytes: int) -> None:
        """同小时内若写本条会超限则切到 seq+1 新文件。"""
        over_lines = self._cur_lines + 1 > self.max_lines
        over_bytes = self._cur_bytes + msg_bytes > self.max_bytes
        # 仅当当前文件已有内容时滚动，避免单条超大消息导致空文件空转
        if (over_lines or over_bytes) and (self._cur_bytes > 0 or self._cur_lines > 0):
            self._stream.close()
            self._open(self._hour_key, self._seq + 1)

    # ------------------------------------------------------------------
    # logging.Handler 接口
    # ------------------------------------------------------------------

    def _write(self, text: str, created: float) -> None:
        """按 created 时间定位小时文件，必要时滚动后写入并 flush（核心写入逻辑）。"""
        msg_bytes = len(text.encode(self.encoding, errors="replace"))
        self._ensure_stream(created)
        self._rollover_if_needed(msg_bytes)
        self._stream.write(text)
        self._stream.flush()
        self._cur_bytes += msg_bytes
        self._cur_lines += 1

    def write_text(self, text: str) -> None:
        """供 stdout/stderr tee 直接写入原始文本（复用小时滚动逻辑，用当前时间定位）。

        与 emit 共用同一份滚动/计数逻辑，使 print()/traceback 与 logging 落进同一组
        滚动文件、口径一致。文件侧异常不外抛——由 _TeeStream 兜底，绝不影响控制台输出。
        """
        if not text:
            return
        self._write(text, time.time())

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._write(self.format(record) + "\n", record.created)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        try:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
        finally:
            super().close()


class _TeeStream:
    """把写入同时转发给原始流（控制台/journald）和滚动文件 writer。

    仅在 --log/WEBX_WORKER_LOG_DIR 生效时安装到 sys.stdout / sys.stderr，使所有 print()、
    traceback、第三方库直写 stdout 的内容都与 logging 一并落盘、且与控制台完全镜像、零遗漏。
    文件侧任何异常都被吞掉，绝不影响原始流输出（控制台/journald 永远优先可见）。
    """

    def __init__(self, original, file_handler: "HourlyRotatingFileHandler") -> None:
        self._original = original
        self._fh = file_handler

    def write(self, s: str) -> int:
        n = self._original.write(s)
        try:
            self._fh.write_text(s)
        except Exception:
            pass  # 文件侧故障不得波及控制台/journald
        return n

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        self._original.flush()

    def isatty(self) -> bool:
        return self._original.isatty()

    def fileno(self) -> int:
        # 子进程通过显式 PIPE 收发日志，不继承本进程 sys.stdout，故 fileno 透传安全
        return self._original.fileno()

    def __getattr__(self, name):
        # 其余属性（encoding/buffer 等）一律委托原始流
        return getattr(self._original, name)


def setup_logging(
    log_dir: str | None = None,
    debug: bool = False,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_lines: int = DEFAULT_MAX_LINES,
) -> str | None:
    """配置 root logger：控制台始终挂，log_dir 非空时追加文件 handler。

    Args:
        log_dir:   日志目录；None/空串 → 仅控制台
        debug:     True → DEBUG，否则 INFO
        max_bytes: 单文件字节上限
        max_lines: 单文件行数上限

    Returns:
        实际生效的日志目录（None 表示仅控制台）。
    """
    level = logging.DEBUG if debug else logging.INFO
    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    root = logging.getLogger()
    root.setLevel(level)
    # 幂等：清掉已存在 handler，避免重复调用导致重复输出
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    # 文件落盘（可选）：log_dir 非空时把 stdout/stderr tee 到滚动文件。
    # 必须在创建 console handler 之前安装 tee —— StreamHandler(sys.stdout) 会在构造时捕获
    # 当前 sys.stdout 引用；先包裹，logging 的控制台输出才会经 tee 一并落盘（与 print 同口径）。
    # 不再单独 addHandler(file_handler)：文件写入统一由 tee 完成，避免 logging 记录重复落盘。
    effective_dir: str | None = None
    if log_dir:
        file_handler = HourlyRotatingFileHandler(
            log_dir, max_bytes=max_bytes, max_lines=max_lines
        )
        effective_dir = str(file_handler.log_dir)
        # 幂等：避免 setup_logging 重入时把 sys.stdout 重复包裹成 tee 套 tee
        if not isinstance(sys.stdout, _TeeStream):
            sys.stdout = _TeeStream(sys.stdout, file_handler)
        if not isinstance(sys.stderr, _TeeStream):
            sys.stderr = _TeeStream(sys.stderr, file_handler)

    # 控制台 handler 始终挂（指向可能已被 tee 包装的 sys.stdout）
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(level)
    root.addHandler(console)

    # 兜底：记录未捕获的同步异常（KeyboardInterrupt 仍走默认行为）
    def _excepthook(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        logging.getLogger("webx_worker").critical(
            "未捕获异常", exc_info=(exc_type, exc_value, exc_tb)
        )

    sys.excepthook = _excepthook

    return effective_dir
