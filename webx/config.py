"""配置基类。

与原项目最大的区别：**框架不定义业务配置**。webx 只提供一个 `BaseSettings`
骨架（应用/数据库/令牌/CORS/日志/邮件这些任何 web 服务都要的字段），业务项目
继承它、追加自己的字段，再通过 `configure_settings()` 注册为全局单例。

  业务项目 app/config.py::

      from dataclasses import dataclass, field
      from webx.config import BaseSettings, env, configure_settings

      @dataclass
      class Settings(BaseSettings):
          APP_NAME: str = "myapp"
          MY_FEATURE_FLAG: bool = field(default_factory=lambda: env.bool("MY_FEATURE_FLAG", False))

      settings = configure_settings(Settings())

  这里**只写业务自有字段**。模型模块不用声明——按约定扫 `<pkg>/models` 自动挂接。

框架内部一律 `from webx.config import settings` 拿到的是**代理对象**，
访问时才解析到业务注册的实例——保证框架代码与业务配置类零编译期耦合。

环境变量说明（environs ≥ 15 的坑）：`read_env()` 不再回写 `os.environ`，
只写进 Env 实例内部。故全框架必须共用本模块导出的 `env` 单例，不要自己
`Env()` 一个新的——那个读不到 .env 的内容。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable

from environs import Env

from webx.paths import project_root, resolve_path

# ── 全局 Env 单例：import 期即读取项目根下的 .env ────────────────────
# 时序：project_root() 依赖 WEBX_ROOT 环境变量或 configure_root()，故业务项目
# 必须在 import 本模块【之前】调用 configure_root()（见 webx.paths）。
env = Env()
env.read_env(path=str(project_root() / ".env"), recurse=False)


def _sms_templates_from_env() -> dict[str, str]:
    """「业务场景 → 模板码」映射，JSON 形如 ``{"verify_code": "SMS_123456789"}``。

    解析失败（缺失 / 非法 JSON / 非对象）一律回退空 dict：配置写坏不该让整个应用
    起不来——缺模板会在真正发送时以明确的 SmsError(template_not_configured) 报出，
    比启动期崩溃更容易定位。
    """
    raw = env.str("SMS_TEMPLATES", "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except Exception:
        return {}
    return {str(k): str(v) for k, v in value.items()} if isinstance(value, dict) else {}


@dataclass
class BaseSettings:
    """通用配置骨架。业务项目继承后追加自有字段。"""

    # ==================== 应用 ====================
    APP_NAME: str = field(default_factory=lambda: env.str("APP_NAME", "webx"))
    # 面向用户的展示名（邮件标题/正文等）。留空则回落 APP_NAME
    APP_DISPLAY_NAME: str = field(default_factory=lambda: env.str("APP_DISPLAY_NAME", ""))
    APP_ENV: str = field(default_factory=lambda: env.str("APP_ENV", "dev"))
    APP_DEBUG: bool = field(default_factory=lambda: env.bool("APP_DEBUG", False))

    # 运行模式的命令行覆盖位（见 apply_flags）
    _force_debug: bool = False
    _force_dev: bool = False

    # ==================== 数据库 ====================
    DATABASE_HOST: str = field(default_factory=lambda: env.str("DATABASE_HOST", "localhost"))
    DATABASE_PORT: int = field(default_factory=lambda: env.int("DATABASE_PORT", 5432))
    DATABASE_NAME: str = field(default_factory=lambda: env.str("DATABASE_NAME", "webx"))
    DATABASE_USER: str = field(default_factory=lambda: env.str("DATABASE_USER", "postgres"))
    DATABASE_PASSWORD: str = field(default_factory=lambda: env.str("DATABASE_PASSWORD", ""))
    DATABASE_SCHEMA: str = field(default_factory=lambda: env.str("DATABASE_SCHEMA", "public"))
    DATABASE_POOL_MIN_SIZE: int = field(default_factory=lambda: env.int("DATABASE_POOL_MIN_SIZE", 2))
    DATABASE_POOL_MAX_SIZE: int = field(default_factory=lambda: env.int("DATABASE_POOL_MAX_SIZE", 10))
    DATABASE_CONNECTION_LIFETIME: float = field(
        default_factory=lambda: env.float("DATABASE_CONNECTION_LIFETIME", 3600.0)
    )
    # 会话级超时（毫秒，PostgreSQL GUC），建连时经 server_settings 下发。
    # ⚠️ transaction_timeout 仅 PG 17+ 支持；低版本请置 0 跳过下发，否则建连即报
    # "unrecognized configuration parameter"。值 <= 0 一律视为不下发。
    DATABASE_STATEMENT_TIMEOUT_MS: int = field(
        default_factory=lambda: env.int("DATABASE_STATEMENT_TIMEOUT_MS", 25000)
    )
    DATABASE_LOCK_TIMEOUT_MS: int = field(
        default_factory=lambda: env.int("DATABASE_LOCK_TIMEOUT_MS", 3500)
    )
    DATABASE_TRANSACTION_TIMEOUT_MS: int = field(
        default_factory=lambda: env.int("DATABASE_TRANSACTION_TIMEOUT_MS", 0)
    )

    # ⚠️ 这里**没有**模型模块配置。模型跟随模块，由 `apply_module_models` 按约定
    # 扫 `<pkg>/models` 解析，结果存在模块级的 `_RESOLVED_MODELS` 里，用
    # `resolved_models()` 读。不想加载某个模块的模型，就把 `models.py` 改名成
    # `_models.py` 或删掉——让"有没有"由文件系统表达，而不是由一份要跟文件系统
    # 对账的清单表达。

    # ==================== 令牌 / 鉴权 ====================
    # WebxToken（AES-256-GCM）主密钥。长度 < 16 会被 fix_key 拒绝，务必配置真值。
    TOKEN_SECRET: str = field(default_factory=lambda: env.str("TOKEN_SECRET", "change-me-in-production"))
    TOKEN_EXPIRE_MINUTES: int = field(default_factory=lambda: env.int("TOKEN_EXPIRE_MINUTES", 1440))
    # 签发/校验时可用的 token id 集合（多端隔离：user / admin / ...）
    TOKEN_IDS: list[str] = field(default_factory=lambda: env.list("TOKEN_IDS", default=["user", "admin"]))
    # 内部服务间回调鉴权（X-Internal-Token 头）
    INTERNAL_TOKEN: str = field(default_factory=lambda: env.str("INTERNAL_TOKEN", ""))

    # ==================== 服务器 ====================
    SERVER_HOST: str = field(default_factory=lambda: env.str("SERVER_HOST", "0.0.0.0"))
    SERVER_PORT: int = field(default_factory=lambda: env.int("SERVER_PORT", 8000))
    REQUEST_MAX_SIZE: int = field(
        default_factory=lambda: env.int("REQUEST_MAX_SIZE", 100 * 1024 * 1024)
    )

    # ==================== CORS ====================
    CORS_ALLOWED_ORIGINS: list[str] = field(
        default_factory=lambda: env.list("CORS_ALLOWED_ORIGINS", default=[])
    )
    CORS_ALLOW_HEADERS: str = field(
        default_factory=lambda: env.str(
            "CORS_ALLOW_HEADERS",
            "Content-Type,Authorization,X-Internal-Token,X-Lang,Accept-Language",
        )
    )
    CORS_ALLOW_METHODS: str = field(
        default_factory=lambda: env.str(
            "CORS_ALLOW_METHODS", "GET,POST,PUT,PATCH,DELETE,OPTIONS"
        )
    )

    # ==================== 日志 / 存储 ====================
    # 空串 → 仅控制台（systemd 下进 journald）；非空 → 按小时滚动落盘。
    # 相对路径锚定【项目根】而非进程 CWD（见 webx.paths.resolve_path）。
    WEB_LOG_DIR: str = field(default_factory=lambda: env.str("WEB_LOG_DIR", ""))
    STORAGE_DIR: str = field(default_factory=lambda: env.str("STORAGE_DIR", "storage"))

    # ==================== 文件上传 / 下载 ====================
    # 单文件上限。整传是流式落盘（峰值内存 = 单块），这个上限管的是磁盘与业务合理性，
    # 不是内存。超大文件建议走分片上传端点。
    UPLOAD_MAX_SIZE: int = field(
        default_factory=lambda: env.int("UPLOAD_MAX_SIZE", 100 * 1024 * 1024)
    )
    # 单个分片的体积上限。分片是逐块追加到磁盘的，上限管的是单次请求的合理性
    UPLOAD_CHUNK_MAX_BYTES: int = field(
        default_factory=lambda: env.int("UPLOAD_CHUNK_MAX_BYTES", 64 * 1024 * 1024)
    )
    # 对象键的命名规则：strict（字母数字 _ - .，默认）/ lenient（允许中文、空格、括号，
    # 仍排除路径分隔符与控制字符）。需要保留用户原始文件名时用 lenient
    STORAGE_KEY_RULE: str = field(default_factory=lambda: env.str("STORAGE_KEY_RULE", "strict"))
    # 分片上传会话的最长存活时长；超龄未完成的会话在启动时被回收，防磁盘泄漏
    UPLOAD_SESSION_MAX_AGE_HOURS: float = field(
        default_factory=lambda: env.float("UPLOAD_SESSION_MAX_AGE_HOURS", 24.0)
    )
    # 下载签名密钥。**独立于 TOKEN_SECRET**，可单独轮换；
    # 留空则回落 TOKEN_SECRET 并告警（能跑，但两套密钥绑一起、泄漏面变大）
    STORAGE_SIGN_SECRET: str = field(
        default_factory=lambda: env.str("STORAGE_SIGN_SECRET", "")
    )
    STORAGE_SIGN_TTL_SECONDS: int = field(
        default_factory=lambda: env.int("STORAGE_SIGN_TTL_SECONDS", 3600)
    )

    # ==================== 验证码（webx.models）====================
    # 同一标识两次发码的最小间隔
    VERIFY_CODE_COOLDOWN_SECONDS: int = field(
        default_factory=lambda: env.int("VERIFY_CODE_COOLDOWN_SECONDS", 60)
    )
    VERIFY_CODE_VALID_MINUTES: int = field(
        default_factory=lambda: env.int("VERIFY_CODE_VALID_MINUTES", 5)
    )
    # 公开发码端点对同一 identifier 的每日上限（邮件比短信便宜，故额度更高）
    VERIFY_CODE_DAILY_LIMIT_EMAIL: int = field(
        default_factory=lambda: env.int("VERIFY_CODE_DAILY_LIMIT_EMAIL", 8)
    )
    VERIFY_CODE_DAILY_LIMIT_MOBILE: int = field(
        default_factory=lambda: env.int("VERIFY_CODE_DAILY_LIMIT_MOBILE", 5)
    )
    # 登录态绑定（bind）发码按 user_id 维度统计的每日上限
    VERIFY_CODE_DAILY_LIMIT_BIND: int = field(
        default_factory=lambda: env.int("VERIFY_CODE_DAILY_LIMIT_BIND", 8)
    )
    VERIFY_CODE_MAX_RETRIES: int = field(
        default_factory=lambda: env.int("VERIFY_CODE_MAX_RETRIES", 5)
    )
    VERIFY_CODE_LOCK_MINUTES: int = field(
        default_factory=lambda: env.int("VERIFY_CODE_LOCK_MINUTES", 15)
    )

    # 单 IP 跨 identifier/channel 的发码总量帽，防单 IP 轮换轰炸大量目标。
    # 与「按 identifier 冷却 + 每日上限」叠加，两层各管一个维度。
    IP_RATE_LIMIT_PER_MIN: int = field(default_factory=lambda: env.int("IP_RATE_LIMIT_PER_MIN", 30))
    IP_RATE_LIMIT_PER_HOUR: int = field(
        default_factory=lambda: env.int("IP_RATE_LIMIT_PER_HOUR", 150)
    )

    # ==================== 登录安全 ====================
    # 连续失败达此次数后锁定账号（User 与 Admin 共用同一策略）
    LOGIN_MAX_FAILED: int = field(default_factory=lambda: env.int("LOGIN_MAX_FAILED", 5))
    LOGIN_LOCK_MINUTES: int = field(default_factory=lambda: env.int("LOGIN_LOCK_MINUTES", 10))

    # ==================== 邮件 ====================
    MAIL_USERNAME: str = field(default_factory=lambda: env.str("MAIL_USERNAME", ""))
    MAIL_PASSWORD: str = field(default_factory=lambda: env.str("MAIL_PASSWORD", ""))
    MAIL_SERVER: str = field(default_factory=lambda: env.str("MAIL_SERVER", ""))
    MAIL_PORT: int = field(default_factory=lambda: env.int("MAIL_PORT", 587))
    MAIL_USE_SSL: bool = field(default_factory=lambda: env.bool("MAIL_USE_SSL", False))
    MAIL_USE_TLS: bool = field(default_factory=lambda: env.bool("MAIL_USE_TLS", True))
    MAIL_DEFAULT_SENDER: str = field(default_factory=lambda: env.str("MAIL_DEFAULT_SENDER", ""))

    # ==================== 短信（适配层见 webx.lib.sms）====================
    # 总开关。判断是否真正可用请一律用 ``webx.lib.sms.is_ready()``（= 开关 + 驱动凭证
    # 齐备），不要直读本字段：开关开了但凭证没配是「半启用」态。
    # 默认开启——没配凭证时 is_ready() 仍为 False，手机号入口照常提示「暂未开通」；
    # 要明确关掉短信就置 false。
    SMS_ENABLED: bool = field(default_factory=lambda: env.bool("SMS_ENABLED", True))
    # 驱动名，对应 webx.lib.sms.register_driver 注册的名字（内置 aliyun）
    SMS_PROVIDER: str = field(default_factory=lambda: env.str("SMS_PROVIDER", "aliyun"))
    # 凭证与签名。字段名沿用 deepmice-api，.env 可直接复用；各驱动按需取用
    SMS_ACCESS_KEY_ID: str = field(default_factory=lambda: env.str("SMS_ACCESS_KEY_ID", ""))
    SMS_ACCESS_KEY_SECRET: str = field(default_factory=lambda: env.str("SMS_ACCESS_KEY_SECRET", ""))
    SMS_SIGN_NAME: str = field(default_factory=lambda: env.str("SMS_SIGN_NAME", ""))
    # 为空时用驱动默认接入点，仅私网代理 / 联调场景需覆盖
    SMS_ENDPOINT: str = field(default_factory=lambda: env.str("SMS_ENDPOINT", ""))
    # 号码区号：业务侧存裸号，发送时由驱动补前缀，⚠️ 绝不回写 DB
    SMS_DEFAULT_COUNTRY_CODE: str = field(
        default_factory=lambda: env.str("SMS_DEFAULT_COUNTRY_CODE", "86")
    )
    # 业务场景 → 模板码映射（JSON），新增场景只加配置不改代码
    SMS_TEMPLATES: dict[str, str] = field(default_factory=_sms_templates_from_env)
    # 双超时：httpx 的 read 是【空闲超时】（块间间隔）而非总时长，只能当卡死探测用；
    # 「整体不超过 N 秒」须另用 asyncio.timeout 包一层，故 TOTAL 必须 > READ。
    SMS_CONNECT_TIMEOUT_MS: int = field(default_factory=lambda: env.int("SMS_CONNECT_TIMEOUT_MS", 5000))
    SMS_READ_TIMEOUT_MS: int = field(default_factory=lambda: env.int("SMS_READ_TIMEOUT_MS", 10000))
    SMS_TOTAL_TIMEOUT_MS: int = field(default_factory=lambda: env.int("SMS_TOTAL_TIMEOUT_MS", 20000))

    # ==================== 任务模块 ====================
    # true = 只有 register_task_type 登记过的类型才能建任务；false（默认）= 未登记按默认单阶段处理
    TASK_REQUIRE_REGISTERED_TYPES: bool = field(
        default_factory=lambda: env.bool("TASK_REQUIRE_REGISTERED_TYPES", False)
    )
    # 每用户"等待子任务的顶层任务"上限，0=不限，越界按 [0, 100] 钳。等待态不占并发名额，
    # 所以需要这道独立的堆积闸
    TASK_WAITING_PARENT_MAX: int = field(
        default_factory=lambda: env.int("TASK_WAITING_PARENT_MAX", 10)
    )

    # ==================== 租约 / 分发 ====================
    # ⚠️ 租约参数的单一来源是这里：领取服务（写短租约）、Web（续租）、补偿器（回收）三个进程各读各的，
    # 但读的是同一份配置。三处若各存一份副本，漂移会让 worker 的自熔时刻与服务端的回收时刻错位——
    # 即"服务端已租给 B、A 仍认为自己持有"的窗口。
    #
    # 认领时的短租约：兜住"领取响应丢在路上"。worker 首次 ack 后续到常规租约
    LEASE_CLAIM_SECONDS: int = field(default_factory=lambda: env.int("LEASE_CLAIM_SECONDS", 60))
    # 常规租约：每次心跳把到期时刻推到 now + LEASE_SECONDS + grace
    LEASE_SECONDS: int = field(default_factory=lambda: env.int("LEASE_SECONDS", 100))
    # grace = clamp(α × 已投入时长, MIN, MAX)。误抢占的损失 = 接手方要从当前步骤开头重跑 = 已投入时长，
    # 所以跑得越久越有耐心；MIN 覆盖网络抖动（与任务无关，该固定），MAX 防无限增长
    LEASE_GRACE_ALPHA: float = field(default_factory=lambda: env.float("LEASE_GRACE_ALPHA", 0.12))
    LEASE_GRACE_MIN_SECONDS: int = field(default_factory=lambda: env.int("LEASE_GRACE_MIN_SECONDS", 180))
    LEASE_GRACE_MAX_SECONDS: int = field(default_factory=lambda: env.int("LEASE_GRACE_MAX_SECONDS", 18000))
    # 连续无进展的被动重投上限，超限直接判失败
    LEASE_REQUEUE_LIMIT: int = field(default_factory=lambda: env.int("LEASE_REQUEUE_LIMIT", 10))

    # 领取服务（task_server）：Web / 补偿器通过它的 URL "戳"一下；worker 通过反代访问它的 /task/leases
    TASK_SERVER_URL: str = field(default_factory=lambda: env.str("TASK_SERVER_URL", "http://127.0.0.1:9020"))
    TASK_SERVER_HOST: str = field(default_factory=lambda: env.str("TASK_SERVER_HOST", "127.0.0.1"))
    TASK_SERVER_PORT: int = field(default_factory=lambda: env.int("TASK_SERVER_PORT", 9020))
    # 单次领取请求最长挂起（秒）。必须小于反代的 proxy_read_timeout，否则连接会被反代掐断；
    # 被掐断也不损失正确性——挂起中的请求尚未认领任何任务
    TASK_LEASE_HOLD_SECONDS: float = field(default_factory=lambda: env.float("TASK_LEASE_HOLD_SECONDS", 25.0))
    # 挂起期间的兜底重试间隔：唤醒（poke）是主路径，这只是兜住"poke 丢了"的安全网
    TASK_LEASE_FALLBACK_SECONDS: float = field(
        default_factory=lambda: env.float("TASK_LEASE_FALLBACK_SECONDS", 1.0)
    )

    # ==================== 补偿器 ====================
    RECONCILER_SCAN_INTERVAL: int = field(default_factory=lambda: env.int("RECONCILER_SCAN_INTERVAL", 600))
    RECONCILER_STALE_THRESHOLD_MINUTES: int = field(
        default_factory=lambda: env.int("RECONCILER_STALE_THRESHOLD_MINUTES", 60)
    )
    RECONCILER_QUEUED_TIMEOUT_MINUTES: int = field(
        default_factory=lambda: env.int("RECONCILER_QUEUED_TIMEOUT_MINUTES", 60)
    )
    RECONCILER_ABANDON_THRESHOLD_HOURS: int = field(
        default_factory=lambda: env.int("RECONCILER_ABANDON_THRESHOLD_HOURS", 168)
    )
    RECONCILER_NONE_TASK_TIMEOUT_HOURS: int = field(
        default_factory=lambda: env.int("RECONCILER_NONE_TASK_TIMEOUT_HOURS", 3)
    )
    RECONCILER_ADMIT_INTERVAL: int = field(default_factory=lambda: env.int("RECONCILER_ADMIT_INTERVAL", 30))
    # 租约回收循环：秒级点查，独立于上面的慢扫描，绝不可合并
    RECONCILER_LEASE_INTERVAL: int = field(default_factory=lambda: env.int("RECONCILER_LEASE_INTERVAL", 5))

    # ==================== 存储服务（客户端侧）====================
    # 任务模块调用独立存储服务（删除任务产物等）。未配置 URL = 静默跳过远端删除，
    # 只清本地（同机部署时恰好是同一棵目录树）
    STORAGE_INTERNAL_URL: str = field(default_factory=lambda: env.str("STORAGE_INTERNAL_URL", ""))
    STORAGE_INTERNAL_TOKEN: str = field(
        default_factory=lambda: env.str("STORAGE_INTERNAL_TOKEN", env.str("INTERNAL_TOKEN", ""))
    )

    # ==================== 派生属性 ====================

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgres://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}"
            f"@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        )

    @property
    def storage_path(self):
        """存储根目录的绝对路径（相对值锚定项目根）。"""
        return resolve_path(self.STORAGE_DIR)

    @property
    def debug(self) -> bool:
        return self._force_debug or self.APP_DEBUG

    @property
    def is_dev(self) -> bool:
        return self._force_dev or self.APP_ENV == "dev"

    @property
    def is_prod(self) -> bool:
        return not self.is_dev and self.APP_ENV == "prod"

    def apply_flags(self, debug: bool = False, dev: bool = False) -> None:
        """应用命令行标志（run.py --debug / --dev）。"""
        object.__setattr__(self, "_force_debug", debug)
        object.__setattr__(self, "_force_dev", dev)

    # ==================== Tortoise ORM ====================

    def db_server_settings(self) -> dict[str, str]:
        """会话级超时 → asyncpg server_settings（值须为字符串，单位毫秒）。

        值 <= 0 视为「不下发」——用于在 PG < 17 上关闭 transaction_timeout。
        """
        mapping = {
            "statement_timeout": self.DATABASE_STATEMENT_TIMEOUT_MS,
            "lock_timeout": self.DATABASE_LOCK_TIMEOUT_MS,
            "transaction_timeout": self.DATABASE_TRANSACTION_TIMEOUT_MS,
        }
        return {k: str(v) for k, v in mapping.items() if v and v > 0}

    def tortoise_config(self) -> dict[str, Any]:
        """按当前配置生成 Tortoise ORM 配置字典。

        模型来源是 `resolved_models()`——由装配的模块按约定扫出来，没有配置项。
        """
        return {
            "connections": {
                "default": {
                    "engine": "tortoise.backends.asyncpg",
                    "credentials": {
                        "host": self.DATABASE_HOST,
                        "port": int(self.DATABASE_PORT),
                        "user": self.DATABASE_USER,
                        "password": self.DATABASE_PASSWORD,
                        "database": self.DATABASE_NAME,
                        # 每个 Sanic worker 独立一个连接池
                        "minsize": self.DATABASE_POOL_MIN_SIZE,
                        "maxsize": self.DATABASE_POOL_MAX_SIZE,
                        # 主动回收长期空闲连接，防 PG/网络层静默关闭后拿到死连接
                        "max_inactive_connection_lifetime": self.DATABASE_CONNECTION_LIFETIME,
                        "server_settings": self.db_server_settings(),
                    },
                }
            },
            "apps": {
                "models": {
                    "models": resolved_models(),
                    "default_connection": "default",
                }
            },
        }


# ── 全局单例与代理 ────────────────────────────────────────────────

_settings: BaseSettings | None = None


def configure_settings(instance: BaseSettings) -> BaseSettings:
    """注册业务配置实例为全局单例，返回它本身（便于 `settings = configure_settings(...)`）。"""
    global _settings
    _settings = instance
    return instance


def get_settings() -> BaseSettings:
    """取全局配置。业务未注册时懒实例化一个 BaseSettings 兜底（便于纯框架测试）。"""
    global _settings
    if _settings is None:
        _settings = BaseSettings()
    return _settings


def module_exists(name: str) -> bool:
    """子模块是否存在。父包不存在时 ``find_spec`` 会抛，一并当作「不存在」。"""
    import importlib.util

    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


# 本进程解析出的模型模块列表。**不是配置项**，没有环境变量、也不需要业务声明——
# 它是 `apply_module_models` 按约定扫出来的结果，只经由 `resolved_models()` 读取。
_RESOLVED_MODELS: list[str] = []


def apply_module_models(modules: Iterable[str]) -> list[str]:
    """按约定解析出模型模块列表并记录下来，返回结果。

    规则只有一条：**装配了哪个模块，就带上它的 ``<pkg>/models``**（存在才带）。
    ``webx`` 自己也走这套——挂了内置路由组时它就是一个普通模块，没有特殊待遇。
    不想要某个模块的模型，把 ``models.py`` 改名 ``_models.py`` 或删掉即可，
    探测不到自然不加载。

    **服务进程与 CLI 必须共用这一个函数**。``create_app`` 调它，``tools/dbsync.py``
    这类脚本也调它——两边若各算各的，就会出现「``WEBX_MODULES=app,appx`` 启动的服务
    路由齐全，但 dbsync 只建了 app 的表」，接口一查库就 no such table，
    而且启动阶段毫无征兆。

    语义是**完整替换**而非累加：一次调用就表达「本进程装配这些模块」的全部结论，
    重复调用不会把上一次的残留带进来（进程内只装配一次，测试之间也因此天然隔离）。

    Args:
        modules: 模块包名，如 ``["app", "appx", "webx"]``。

    Returns:
        解析出的模型模块列表，保序去重。
    """
    global _RESOLVED_MODELS

    out: list[str] = []
    for m in modules:
        name = f"{m}.models"
        if name not in out and module_exists(name):
            out.append(name)
    _RESOLVED_MODELS = out
    return list(out)


def resolved_models() -> list[str]:
    """本进程要交给 ORM 的模型模块列表（`apply_module_models` 的结果）。"""
    return list(_RESOLVED_MODELS)


class _SettingsProxy:
    """延迟解析的配置代理。

    框架模块在 import 期就 `from webx.config import settings`，但那时业务
    的 Settings 往往还没注册。代理把解析推迟到**属性访问时**，从而允许
    「框架先 import、业务后 configure」这一自然顺序。
    """

    def __getattr__(self, name: str) -> Any:
        return getattr(get_settings(), name)

    def __repr__(self) -> str:
        return f"<SettingsProxy → {get_settings()!r}>"


settings = _SettingsProxy()
