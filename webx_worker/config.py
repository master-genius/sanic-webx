"""
Worker 配置模块

配置来源分三层，**高优先级在前**：

  ⓪ 真实进程环境变量（os.environ）          —— 运维的显式覆盖（systemd Environment= / export）
  ② 实例覆盖层  <channel 配置同目录>/<配置名>_env  —— 可选，由 apply_env_overlay() 加载
  ① 基础层      <worker 主目录>/.env           —— 所有实例共享，由 load_base_env() 加载
  默认值

第 ② 层是多 worker 实例的配置机制：`--config gpu0` 启动时自动探测同目录下的
`gpu0_env`，存在即叠加。这样每个实例只需写自己「不一样」的那几行（如
WEBX_WORKER_WORK_DIR / WEBX_WORKER_LOG_DIR），公共项继续从 .env 继承，
无需为每个实例克隆一整份 .env。

⚠️ 本模块**不依赖任何第三方配置库**（worker 要能单独打包，只带标准库 + httpx），
`.env` 的解析在 `parse_env_text()` 里自己做，保持精简：
  · `KEY=VALUE`，可带 `export ` 前缀；`#` 开头为注释；值两端的成对引号会被剥掉
  · 未加引号的值里，空白 + `#` 之后视为行内注释
  · 同名键重复出现取【最后一行】
  · 不做变量展开、不做 `~` 展开（路径类值请写绝对路径）

⚠️ 文件层（.env / 覆盖层）只影响 worker【主进程】的配置项。任务子进程读不到它们——
子进程需要的几项由 executor 显式注入环境（见 envnames），其余子进程级参数写在
channel 配置的 `run_env` 里。
"""

import logging
import os
from dataclasses import dataclass, field, fields
from pathlib import Path

from . import envnames as E

logger = logging.getLogger(__name__)


# ────────────────────────── .env 解析 + 分层取值 ──────────────────────────

def parse_env_text(text: str) -> dict[str, str]:
    """把 .env 文本解析成 {键: 值}。规则见模块 docstring。"""
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        if not key:
            continue
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        else:
            # 未加引号：空白 + '#' 起为行内注释
            for i, ch in enumerate(val):
                if ch == "#" and i > 0 and val[i - 1].isspace():
                    val = val[:i].rstrip()
                    break
        out[key] = val
    return out


# 文件层：base = .env，overlay = 实例覆盖层。取值时 overlay 压过 base。
_FILE_LAYERS: dict[str, dict[str, str]] = {"base": {}, "overlay": {}}


def _lookup(name: str) -> str | None:
    """分层取值：真实环境变量 > 覆盖层 > .env。取不到返回 None。"""
    v = os.environ.get(name)
    if v is not None:
        return v
    for layer in ("overlay", "base"):
        if name in _FILE_LAYERS[layer]:
            return _FILE_LAYERS[layer][name]
    return None


def _str(name: str, default: str = "") -> str:
    v = _lookup(name)
    return default if v is None else v


def _int(name: str, default: int) -> int:
    v = (_lookup(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        logger.warning("%s 不是整数：%r，使用默认值 %s", name, v, default)
        return default


def _float(name: str, default: float) -> float:
    v = (_lookup(name) or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        logger.warning("%s 不是数字：%r，使用默认值 %s", name, v, default)
        return default


def _bool(name: str, default: bool) -> bool:
    v = (_lookup(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


# ────────────────────────── 配置项 ──────────────────────────

@dataclass
class WorkerSettings:
    """Worker 配置（所有环境变量以 WEBX_WORKER_ 为前缀）。"""

    # ==================== Worker 标识 ====================
    WORKER_ID: str = field(default_factory=lambda: _str(E.ENV_WORKER_ID))
    """Worker 实例标识（fencing 令牌的前半，也写入任务的归属字段）。

    ⚠️ 缺省为空字符串 = **尚未派生**。真实值由 `derive_worker_id()` 在启动时填入，
    因为它依赖 `--config`（实例区分依据），而本 dataclass 在 import 期就构造完成，
    那时命令行还没解析。显式设置 `WEBX_WORKER_ID` 则原样采用（逃生舱），不再派生。
    """

    WORKER_PREFIX: str = field(default_factory=lambda: _str("WEBX_WORKER_PREFIX"))
    """机器标识前缀。空 = 回落到主机名。

    部署意图：**每台机器只需设这一个变量**，同机多实例靠各自的 `--config` 天然区分，
    无需为每个实例编一个 ID（既麻烦又易错）。
    """

    # ==================== 任务分发：lease ====================
    TASK_SERVER_URL: str = field(default_factory=lambda: _str("WEBX_WORKER_TASK_SERVER_URL"))
    """领取服务（task_server）的可达地址。空 = 回落到 WEB_BASE_URL
    （同一反代下挂路径前缀的常见部署）。"""

    LEASE_MAX_LEVEL: str = field(default_factory=lambda: _str("WEBX_WORKER_LEASE_MAX_LEVEL"))
    """只领取档位 ≤ 此值的任务。空 = 不限。

    上限而非集合：语义上正好（「这台机器最高服务到哪一档」），性能上也最优。
    设 `0` 即只服务最低档。空字符串而非 int 默认值，是为了区分
    「没配置」与「配置成 0」—— 后者是个有意义且常用的取值。
    """

    LEASE_MAX_COMPUTE_POINTS: str = field(
        default_factory=lambda: _str("WEBX_WORKER_LEASE_MAX_COMPUTE_POINTS")
    )
    """只领取算力点 ≤ 此值的任务。空 = 不限（语义同 LEASE_MAX_LEVEL）。"""

    LEASE_HOLD_SECONDS: float = field(
        default_factory=lambda: _float("WEBX_WORKER_LEASE_HOLD_SECONDS", 25.0)
    )
    """预期的服务端挂起时长（秒）——只用来推导本地 HTTP 超时（取它 +15s）。

    真正决定挂多久的是服务端的 HOLD_SECONDS；此处填小了会在服务端正常挂起时被
    客户端自己掐断（不损失正确性，只是白重连），填大了则服务端早已返回而本地超时
    形同虚设。保持两边一致即可。
    """

    # ==================== 鉴权配置 ====================
    INTERNAL_TOKEN: str = field(default_factory=lambda: _str(E.ENV_INTERNAL_TOKEN))
    """内部回调鉴权 Token（X-Internal-Token）。空 = 未配置，服务端会拒绝一切请求。"""

    # ==================== 执行配置 ====================
    WORK_DIR: str = field(
        default_factory=lambda: _str(E.ENV_WORK_DIR, str(Path.home() / "webx-worker-tasks"))
    )
    """任务工作目录根路径（默认：~/webx-worker-tasks）"""

    HEARTBEAT_INTERVAL: int = field(
        default_factory=lambda: _int("WEBX_WORKER_HEARTBEAT_INTERVAL", 30)
    )
    """⚠️ 已废弃：心跳节拍由 heartbeat._HEARTBEAT_INTERVALS 序列驱动，此值不生效（仅占位）。"""

    LOG_HEAD_LINES: int = field(default_factory=lambda: _int("WEBX_WORKER_LOG_HEAD_LINES", 100))
    """日志保留头部行数"""

    LOG_TAIL_LINES: int = field(default_factory=lambda: _int("WEBX_WORKER_LOG_TAIL_LINES", 2000))
    """日志保留尾部行数（上传截断用；本地文件仍存全量）。
    与 head 共同决定阈值：total ≤ head+tail 时全量上传、无截断；超出才头尾截取、中间省略。"""

    # ==================== 任务服务回调配置 ====================
    WEB_BASE_URL: str = field(
        default_factory=lambda: _str(E.ENV_WEB_BASE_URL, "http://localhost:9009")
    )
    """任务服务（承载 /internal/tasks/<id>/... 的 Web 服务）基础 URL。"""

    STORAGE_URL: str = field(default_factory=lambda: _str(E.ENV_STORAGE_URL))
    """存储服务【对外】基础 URL（面向浏览器/前端下载的地址）。

    恒用于「落库的 download_url」。注意：本协议里产物上传只走存储服务，**没有**回退到
    任务服务的路径——未配置 STORAGE_URL / STORAGE_INTERNAL_URL 时，产物与步骤日志
    都无处可传（见 `storage_configured()`）。"""

    STORAGE_INTERNAL_URL: str = field(default_factory=lambda: _str(E.ENV_STORAGE_INTERNAL_URL))
    """存储服务【内网】基础 URL（worker 实际上传/下载走此地址）；空 = 回退 STORAGE_URL。
    ⚠️ 仅影响 worker→storage 的请求地址；落库给前端的 download_url 恒用【对外】STORAGE_URL。"""

    STORAGE_INTERNAL_TOKEN: str = field(
        default_factory=lambda: _str(E.ENV_STORAGE_INTERNAL_TOKEN)
    )
    """存储服务专属鉴权 Token。空串 = 回退 INTERNAL_TOKEN。

    父进程（uploader 跑在父进程侧）与任务子进程两端保持同一条回退链
    （STORAGE_INTERNAL_TOKEN → INTERNAL_TOKEN），配了独立密钥就全都用它，
    不配就全都回退，不会出现"一半能用"。"""

    # ==================== 异步上传（uploader）====================
    UPLOADER_ENABLED: bool = field(default_factory=lambda: _bool(E.ENV_UPLOADER_ENABLED, True))
    """是否启用独立 uploader 进程。

    关掉 = 所有产物一律在任务子进程里同步上传，是部署期的总开关：
    出任何问题先关它，不必回滚代码。"""

    UPLOAD_ASYNC_THRESHOLD_MB: int = field(
        default_factory=lambda: _int(E.ENV_UPLOAD_ASYNC_THRESHOLD_MB, 15)
    )
    """产物走 uploader 的体积门槛（MB）。⚠️ 只对 `deliver_only` 的「纯交付产物」生效；
    下游依赖的产物（`deliver_required`）恒同步上传，不看大小。

    同一个数字在内网（千兆传 15MB = 0.12s）与公网（10Mbps 上行 = 12s）下差两个数量级，
    故必须可配。"""

    UPLOAD_REQUIRED_GLOBS: str = field(
        default_factory=lambda: _str(E.ENV_UPLOAD_REQUIRED_GLOBS)
    )
    """分流规则：逗号分隔的 fnmatch 模式，按【远端文件名】匹配；命中的产物恒走同步上传
    （等价于调用 `deliver_required`）。空 = 不设规则，由调用点显式选择。"""

    UPLOADER_CONCURRENCY: int = field(
        default_factory=lambda: _int("WEBX_WORKER_UPLOADER_CONCURRENCY", 3)
    )
    """uploader 内并发上传数。

    不设上限是错的：带宽守恒——N 路并发传大文件总时长不变，只是每路都变慢，而 N 个
    本地文件**同时**被队列锁住不能清理，磁盘峰值占用变成 N 倍。"""

    UPLOADER_SCAN_INTERVAL: float = field(
        default_factory=lambda: _float("WEBX_WORKER_UPLOADER_SCAN_INTERVAL", 3.0)
    )
    """uploader 扫描 pending/ 的间隔（秒）。不用 inotify：目录里常态 0–10 个文件，
    扫描开销可忽略；上传本身是分钟级的事，几秒延迟无意义。"""

    UPLOADER_PENDING_LIMIT_GB: float = field(
        default_factory=lambda: _float("WEBX_WORKER_UPLOADER_PENDING_LIMIT_GB", 100.0)
    )
    """队列积压上限（GB），超过则暂缓接新任务（背压）。0 = 不限（不建议）。"""

    # ==================== 运行配置 ====================
    DEBUG: bool = field(default=False)
    """调试模式开关。由 --debug 命令行参数覆盖。"""

    # ==================== 日志配置 ====================
    LOG_DIR: str = field(default_factory=lambda: _str("WEBX_WORKER_LOG_DIR"))
    """后台日志目录。空串 = 仅控制台输出。可被命令行 --log 覆盖（命令行 > 覆盖层 > .env > 默认）。
    文件名 worker_task_{YYYY-MM-DD-HH}_{pid}.log，单文件 ≤10MB 或 ≤20000 行滚动。"""

    LOG_MAX_BYTES: int = field(
        default_factory=lambda: _int("WEBX_WORKER_LOG_MAX_BYTES", 10 * 1024 * 1024)
    )
    """单个日志文件字节上限（默认 10MB），超过则切新文件。"""

    LOG_MAX_LINES: int = field(default_factory=lambda: _int("WEBX_WORKER_LOG_MAX_LINES", 20000))
    """单个日志文件行数上限（默认 20000 行），超过则切新文件。"""


# 全局配置单例（各模块 `from .config import worker_settings` 持有的是对象引用，
# 所以刷新只能原地 setattr，绝不可重新绑定模块全局名）
worker_settings = WorkerSettings()


def refresh_settings() -> None:
    """按当前三层来源重建一份配置，并**原地**拷回单例。

    保留 DEBUG（它来自命令行而非环境，重建会把它冲回默认值）。
    """
    debug = worker_settings.DEBUG
    fresh = WorkerSettings()
    for f in fields(fresh):
        setattr(worker_settings, f.name, getattr(fresh, f.name))
    worker_settings.DEBUG = debug


# ────────────────────────── 基础层 / 实例覆盖层 ──────────────────────────

# 覆盖层文件名后缀：`--config gpu0` → channel_config/gpu0_env
ENV_OVERLAY_SUFFIX = "_env"


def load_base_env(path: Path | str) -> Path | None:
    """加载基础层 `.env` 并刷新 worker_settings。文件不存在 → 静默跳过（返回 None）。"""
    p = Path(path).expanduser()
    if not p.is_file():
        return None
    _FILE_LAYERS["base"] = parse_env_text(p.read_text(encoding="utf-8"))
    refresh_settings()
    return p.resolve()


def default_overlay_for(config_path: Path) -> Path:
    """按约定推导 channel 配置对应的覆盖层路径：<同目录>/<配置名去扩展名>_env。

    例：channel_config/gpu0.py → channel_config/gpu0_env
    仅返回路径，不判断存在性（由 apply_env_overlay 负责）。
    """
    return config_path.parent / f"{config_path.stem}{ENV_OVERLAY_SUFFIX}"


def apply_env_overlay(path: Path | str) -> Path | None:
    """加载实例覆盖层并**原地刷新** worker_settings。

    语义：叠加在 .env 之上 —— 覆盖层未写的键继续沿用 .env 的值（继承），
    写了的键取覆盖层的值（覆盖），.env 没有的键也可新增。文件不存在 → 静默跳过
    （按约定探测时缺省是常态，不是错误）。真实进程环境变量仍然压过覆盖层。

    ⚠️ **只覆盖 WorkerSettings 里定义的键**：没有对应字段的键读了也无处可去。
    任务子进程同样读不到覆盖层 —— 子进程级参数（线程、GPU、并发绑定）的正确位置是
    channel 配置里各 channel 的 `run_env`。
    ⚠️ 路径类值不做 ~ 展开（与 .env 一致），请写绝对路径。

    ⚠️ 两条硬约束：

      1) **必须在任何命令行覆盖之前调用**。本函数会整体刷新单例，晚于
         --debug / --work-dir 调用会把它们冲掉。
      2) **只能原地 setattr，绝不可重新绑定模块全局名**。各模块都是
         `from .config import worker_settings` 持有对象引用而非名字。

    Returns:
        实际加载的绝对路径；文件不存在时返回 None。
    """
    p = Path(path).expanduser()
    if not p.is_file():
        return None
    _FILE_LAYERS["overlay"] = parse_env_text(p.read_text(encoding="utf-8"))
    refresh_settings()
    return p.resolve()


def reset_file_layers() -> None:
    """清空文件层并刷新（测试隔离用）。"""
    _FILE_LAYERS["base"] = {}
    _FILE_LAYERS["overlay"] = {}
    refresh_settings()


def worker_home() -> Path:
    """worker 主目录：存放 `.env` 与 `channel_config/` 的地方。

    取 `WEBX_WORKER_HOME`，缺省为当前工作目录。**刻意不锚定到包源码目录**：
    包可能被装进 site-packages，把配置写进那里既不可维护也不可见。
    """
    raw = os.environ.get("WEBX_WORKER_HOME", "").strip()
    return Path(raw).expanduser().resolve() if raw else Path.cwd().resolve()


# ────────────────────────── worker_id 派生 ──────────────────────────

# worker_id 允许的字符：其余一律折成 '-'（要落 DB 的 varchar，且要人眼可读）
_ID_SAFE = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_ID_MAX_LEN = 64


def _sanitize_id_part(raw: str) -> str:
    """把任意字符串折成可安全用作 worker_id 片段的形式。"""
    cleaned = "".join(c if c in _ID_SAFE else "-" for c in raw.strip())
    # 折叠连续 '-' 并去掉首尾，避免出现 "gpu--01-" 这类难看的值
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")


def derive_worker_id(config_name: str) -> str:
    """派生并**原地写入** worker_settings.WORKER_ID，返回最终值。

    规则：`{前缀}-{配置名}`
      · 前缀   = WEBX_WORKER_PREFIX，为空则回落主机名的第一段（去掉域名部分）
      · 配置名 = --config 的裸名（缺省 "default"），即 channel_config/<名>.py

    **为什么用配置名做实例区分**：部署形态是「一 worker 一 unit」，每个 unit 本来
    就要传自己的 --config；而 channel 配置决定该实例跑哪些 channel 与 run_env
    （设备绑定）—— 天然唯一，且已经在命令行里，不必再编一个 ID。

    **为什么必须跨重启稳定**：worker 重启后 recovery 会用它归还上一轮的租约；
    ID 若变了，归还被服务端拒绝，任务退回等租约自然到期。**是优雅降级不是故障**，
    所以不值得为此上持久化 UUID —— 但也因此别用 PID / 随机数当来源。

    ⚠️ 必须在 apply_env_overlay() 之后调用：那个函数会整体重建 WorkerSettings，
    早于它调用会被冲掉（与 --debug 同一约束）。

    Args:
        config_name: 配置文件裸名（不含 .py）

    Returns:
        最终生效的 worker_id
    """
    explicit = (worker_settings.WORKER_ID or "").strip()
    if explicit:
        # 逃生舱：显式指定则原样采用，不做任何派生
        worker_settings.WORKER_ID = explicit[:_ID_MAX_LEN]
        return worker_settings.WORKER_ID

    prefix = (worker_settings.WORKER_PREFIX or "").strip()
    if not prefix:
        import socket
        # 只取主机名第一段：FQDN 里的域名部分对区分实例毫无帮助，只会撑长 ID
        prefix = socket.gethostname().split(".")[0]

    prefix = _sanitize_id_part(prefix) or "worker"
    suffix = _sanitize_id_part(config_name) or "default"

    worker_settings.WORKER_ID = f"{prefix}-{suffix}"[:_ID_MAX_LEN]
    return worker_settings.WORKER_ID


# ────────────────────────── 派生访问器 ──────────────────────────

def storage_configured() -> bool:
    """是否配置了存储服务地址（内网或对外任一）。

    ⚠️ 这是产物上传能否工作的**硬前提**，不是优化项：协议规定产物只上传到存储服务
    （docs/worker-protocol.md §5），任务服务上没有对应端点，**没有回退路径**。
    未配置时：uploader 不拉起，子进程 SDK 的 `deliver_*` 无处可传。

    子进程侧（sdk.upload_route）读环境变量做同一判断 —— 两处口径必须一致。
    """
    return bool(
        worker_settings.STORAGE_INTERNAL_URL.strip() or worker_settings.STORAGE_URL.strip()
    )


def lease_filters() -> tuple[int | None, int | None]:
    """把 worker 的筛选配置解析成领取请求要带的字段（`max_level`, `max_compute_points`）。

    两者都返回 None 表示不限 —— 此时请求体里不带这两个键。
    """
    out: list[int | None] = []
    for name, raw in (("WEBX_WORKER_LEASE_MAX_LEVEL", worker_settings.LEASE_MAX_LEVEL),
                      ("WEBX_WORKER_LEASE_MAX_COMPUTE_POINTS",
                       worker_settings.LEASE_MAX_COMPUTE_POINTS)):
        raw = (raw or "").strip()
        if not raw:
            out.append(None)
            continue
        try:
            out.append(int(raw))
        except ValueError:
            logger.warning("%s 非整数：%r，按不限处理", name, raw)
            out.append(None)
    return out[0], out[1]
