"""Web 服务日志落盘配置（独立实现，与 worker/log_config.py 无代码依赖）。

方案：tee 单文件（复刻 worker 思路，针对 Sanic 多进程做 PID 隔离）。

  - 包裹 sys.stdout / sys.stderr，把两者都镜像进【同一个】按小时滚动的文件：
    logging、print()、traceback、第三方库直写 stdout 全部零遗漏落盘，且与控制台
    （systemd 下 journald）完全镜像。access 与 error 混在一个文件——刻意为之：
      ① 每行有 `[LEVEL] name:` 标签，grep 即可分类（关键字 'sanic.access' / 'ERROR'）；
      ② 单文件时间线连续，能直接看到「请求 → 紧跟的报错」的因果顺序，无需跨文件对齐。
  - 多进程不共享文件：文件名含进程 PID → web_{YYYY-MM-DD-HH}_{pid}.log，
    每个 Sanic worker 各写各的、零竞争（不引入跨进程队列/父进程汇聚那套重基础设施）。
  - 单文件双上限：≤ 10MB 或 ≤ 20000 行，任一先达即切 .1/.2 ... 新文件。
  - 进程重启接续同一小时已有文件（按实际大小/行数初始化计数器，不丢计数）。
  - log_dir 为空 → 完全不介入，保持 Sanic 默认控制台/journald 行为。

时序要求（关键）：setup_logging() 必须在 `app = Sanic()` 构造之前调用。
Sanic 默认 configure_logging=True，构造时即 dictConfig，其 sanic.access/error 的
StreamHandler 用 ext://sys.stdout 在【配置那一刻】捕获 sys.stdout 引用——只有先把
sys.stdout 换成 tee，Sanic 的 handler 才会拿到 tee 后的流、其输出才会一并落盘。
"""

import logging
import os
import sys
import time
from pathlib import Path

from webx.paths import resolve_path

# 默认双上限（可由 setup_logging 调用方覆盖）
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_MAX_LINES = 20000

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class _AccessAwareFormatter(logging.Formatter):
    """兼容 sanic.access 记录的 formatter。

    根因（Sanic 内部机制，非缺陷）：Sanic 访问日志在
        sanic/http/http1.py: access_logger.info("", extra={host,request,status,byte,duration})
    发出——message 恒为空，全部信息在 extra，由 Sanic 专用的 AutoAccessFormatter
    （sanic/logging/formatter.py）渲染，该 formatter 只读 extra、不读 message。
    我们用 tee + root 通用 handler 接管日志时，这些记录靠 propagate 冒泡到 root、
    经本 handler 落盘；若用纯 %(message)s 会打出空行、丢掉 method/path/status。
    故本 formatter 检测该来源、把访问字段拼进输出，字段对齐 Sanic 官方
    （host request status byte duration）；其余 record 走标准格式不受影响。

    副作用安全：logging 中 LogRecord 是跨 handler 共享对象——Sanic 自己的
    access_console handler 与本 handler 会先后格式化同一条记录。故此处临时改写
    record.msg/args 后在 finally 中立即恢复，绝不留下污染。
    """

    def format(self, record: logging.LogRecord) -> str:
        if (
            record.name == "sanic.access"
            and not record.getMessage()
            and hasattr(record, "request")
        ):
            saved_msg, saved_args = record.msg, record.args
            # duration 由 Sanic 组装时自带前导空格（如 " 1.2ms"），直接拼接即可
            record.msg = "%s %s %s %s%s" % (
                getattr(record, "host", "-"),
                getattr(record, "request", "-"),
                getattr(record, "status", "-"),
                getattr(record, "byte", "-"),
                getattr(record, "duration", ""),
            )
            record.args = None
            try:
                return super().format(record)
            finally:
                record.msg, record.args = saved_msg, saved_args
        return super().format(record)

def _resolve_log_dir(log_dir: str) -> Path:
    """把日志目录归一化为绝对路径：相对路径一律相对【项目根】，而非进程 CWD。

    服务可能由 systemd（WorkingDirectory 任意）、启动脚本（cd 到项目根）、
    裸 python run.py（CWD=调用处）等多种方式启动，CWD 不稳定；spawn 子进程还会
    继承父 CWD。锚定到项目根后，相对路径含义在所有启动方式下保持一致、可预期。
    绝对路径与 ~ 展开保持原样。

    注：项目根由 webx.paths 统一裁定（不再靠 __file__ 上溯——webx 是独立安装
    的包，上溯只会溯到 site-packages）。
    """
    return resolve_path(log_dir)


class HourlyRotatingFileWriter:
    """按小时命名 + 大小/条数双上限滚动的文件写入器（tee 的落盘目标）。

    不是 logging.Handler——tee 模式下文件写入统一经 write_text()，logging 的内容
    是「先经 StreamHandler 写到 tee 后的 stdout、再由 tee 转发到这里」，故本类只需
    一个纯文本写入入口即可承接 logging / print / traceback 全部输出。

    切换规则（单进程 asyncio，写入天然串行）：
      - 小时变化            → 关旧文件，开新小时文件（接续该小时已有最大 seq）
      - 同小时但写本条会超限 → seq+1 切新文件
    多进程安全由「文件名含 PID」保证：不同进程写不同文件，本类自身不做跨进程协调。
    """

    def __init__(
        self,
        log_dir: str,
        prefix: str,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_lines: int = DEFAULT_MAX_LINES,
        encoding: str = "utf-8",
    ) -> None:
        self.log_dir = Path(log_dir).expanduser().resolve()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix
        self.max_bytes = max_bytes
        self.max_lines = max_lines
        self.encoding = encoding

        self._hour_key: str | None = None
        self._seq: int = 0
        self._stream = None
        self._cur_bytes: int = 0
        self._cur_lines: int = 0

    def _path_for(self, hour_key: str, seq: int) -> Path:
        """seq=0 → {prefix}_{hour}.log；seq>0 → {prefix}_{hour}.{seq}.log。

        prefix 已在 setup_logging 拼入 PID（如 web_12345），故天然按进程隔离。
        """
        name = (
            f"{self.prefix}_{hour_key}.log"
            if seq == 0
            else f"{self.prefix}_{hour_key}.{seq}.log"
        )
        return self.log_dir / name

    def _latest_seq(self, hour_key: str) -> int:
        """返回该小时已存在文件的最大 seq（无 seq=0 文件则返回 0）。"""
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

    def _ensure_stream(self, created: float) -> None:
        """按时间确保当前流指向正确的小时文件。"""
        hour_key = time.strftime("%Y-%m-%d-%H", time.localtime(created))
        if self._stream is not None and hour_key == self._hour_key:
            return
        if self._stream is not None:
            self._stream.flush()
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
            self._stream.flush()
            self._stream.close()
            self._open(self._hour_key, self._seq + 1)

    def write_text(self, text: str) -> None:
        """写入原始文本（复用小时滚动逻辑，用当前时间定位）。异常不外抛——由 tee 兜底。

        flush 使用 file.flush()（推给 OS 页缓存，非 fsync，成本低）：保证进程崩溃前
        已写内容对 tail/journald 立即可见，高频 access 下也可接受。
        """
        if not text:
            return
        msg_bytes = len(text.encode(self.encoding, errors="replace"))
        self._ensure_stream(time.time())
        self._rollover_if_needed(msg_bytes)
        self._stream.write(text)
        self._stream.flush()
        # 计数按「行」近似：一次 write 可能含多行，用换行数（至少 1）累加，防大块写入不触发滚动
        self._cur_bytes += msg_bytes
        self._cur_lines += max(1, text.count("\n"))

    def close(self) -> None:
        if self._stream is not None:
            try:
                self._stream.flush()
                self._stream.close()
            finally:
                self._stream = None


class _TeeStream:
    """把写入同时转发给原始流（控制台/journald）和滚动文件 writer。

    安装到 sys.stdout / sys.stderr 后，所有 print()、traceback、logging 的 StreamHandler
    输出、第三方直写 stdout 的内容都与控制台完全镜像、一并落盘、零遗漏。
    文件侧任何异常都被吞掉，绝不影响原始流输出（控制台/journald 永远优先可见）。
    """

    def __init__(self, original, writer: "HourlyRotatingFileWriter") -> None:
        self._original = original
        self._writer = writer

    def write(self, s: str) -> int:
        n = self._original.write(s)
        try:
            self._writer.write_text(s)
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
    """tee 单文件日志落盘。log_dir 为空 → 不介入。必须在 app=Sanic() 之前调用。

    Args:
        log_dir:   日志目录；None/空串 → 仅控制台，返回 None
        debug:     True → 收 DEBUG，否则 INFO
        max_bytes: 单文件字节上限
        max_lines: 单文件行数上限

    Returns:
        实际生效的日志目录（None 表示未落盘）。
    """
    if not log_dir:
        return None

    level = logging.DEBUG if debug else logging.INFO

    # 目录创建失败（权限 / 路径非法 / 磁盘满等）绝不能拖垮服务启动——本函数在 main.py
    # 模块 import 期（app=Sanic() 之前）调用，抛异常会让整个 Web 起不来。故此处兜底：
    # 仅告警并返回 None，跳过全部日志落盘、保持控制台/journald 行为，服务照常启动。
    try:
        # 相对路径锚定项目根（而非 CWD），保证多种启动方式下落点一致
        resolved_dir = _resolve_log_dir(log_dir)
        writer = HourlyRotatingFileWriter(
            str(resolved_dir),
            prefix=f"web_{os.getpid()}",   # PID 保证多 worker 各写各的文件
            max_bytes=max_bytes,
            max_lines=max_lines,
        )
    except Exception as e:
        logging.getLogger("webx").warning(
            "日志目录初始化失败，已跳过文件落盘（仅控制台/journald）: %s — %s", log_dir, e
        )
        return None
    effective_dir = str(writer.log_dir)

    # 先包 tee，再让 logging 的控制台 handler 指向 tee 后的 stdout（顺序不能反）。
    # 幂等：fork 子进程重新 import / setup_logging 重入时，避免把 tee 套 tee。
    if not isinstance(sys.stdout, _TeeStream):
        sys.stdout = _TeeStream(sys.stdout, writer)
    if not isinstance(sys.stderr, _TeeStream):
        sys.stderr = _TeeStream(sys.stderr, writer)

    # 业务 logging（logging.getLogger(__name__)）冒泡到 root——给 root 挂一个指向
    # tee 后 stdout 的 StreamHandler，业务 INFO 才会经 tee 落盘（Sanic 只配 sanic.*
    # logger，不含 root）。Sanic 自己的 sanic.access/error handler 已指向 tee 后的
    # stdout/stderr，无需再动，也【不给任何 logger 挂 file handler】——文件写入
    # 唯一经 tee，杜绝重复落盘。
    root = logging.getLogger()
    root.setLevel(level)
    if not any(getattr(h, "_webx_tee_console", False) for h in root.handlers):
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(_AccessAwareFormatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        console.setLevel(level)
        console._webx_tee_console = True  # 幂等标记
        root.addHandler(console)

    # 兜底：未捕获同步异常记入文件（KeyboardInterrupt 保持默认行为）
    def _excepthook(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        logging.getLogger("webx").critical(
            "未捕获异常", exc_info=(exc_type, exc_value, exc_tb)
        )

    sys.excepthook = _excepthook

    return effective_dir
