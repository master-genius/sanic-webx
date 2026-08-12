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

from dataclasses import dataclass, field
from typing import Any, Iterable

from environs import Env

from webx.paths import project_root, resolve_path

# ── 全局 Env 单例：import 期即读取项目根下的 .env ────────────────────
# 时序：project_root() 依赖 WEBX_ROOT 环境变量或 configure_root()，故业务项目
# 必须在 import 本模块【之前】调用 configure_root()（见 webx.paths）。
env = Env()
env.read_env(path=str(project_root() / ".env"), recurse=False)


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
