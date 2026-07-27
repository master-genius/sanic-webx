# webx

面向**纯 Web 服务**的通用 Sanic 框架，从 deepmice-api 的自建体系中抽离、去业务化重构而来。
不含 worker / 补偿器 / ZMQ —— 那些是计算平台的东西，不是 web 框架的。

## 能力清单

**框架骨架（core）**

| 能力 | 模块 | 说明 |
|---|---|---|
| 加密令牌 | `webx.lib.token` | AES-256-GCM，libcrypto 优先 + 纯 Python 兜底，零第三方依赖 |
| ORM 表结构同步 | `webx.lib.schema_sync` | 模型即权威，对比 information_schema 生成最小 DDL |
| 自动蓝图路由 | `webx.core.auto_blueprint` | 递归扫描 controller 包，自动发现并注册蓝图 |
| 层级中间件 | `webx.core.middleware` + `webx.middleware/` | `__mid__.py` 目录继承 + 模块级追加 |

**内置路由（`webx/controller/`）** —— 装了就有全套端点，不用一个项目写一遍

| 目录 | 端点 |
|---|---|
| `controller/auth/` | 账密登录注册、管理员登录、验证码登录、找回密码、登出、刷新令牌 |
| | 注册与找回都是**邮箱/手机二选一**；短信通道未注册时自动挡住手机路径 |
| `controller/api/` | 个人中心 + 文件：上传（整传/分片）、下载、签发限时链接、删除 |
| `controller/admin/` | 账号 CRUD、角色/权限查询、用户角色管理、单独授权 |
| `controller/public/` | 签名下载（无需登录，靠限时签名授权） |

内置路由**走的是同一套 `__mid__.py` 扫描机制**，没有任何特殊待遇——
每个目录自己声明中间件，`create_app` 一并扫描。不需要就 `builtin_controller=False`。

## 相对原架构的三处改造

**1. 中间件不再焊死在 core**

原项目的痛点是"加中间件要改 `core/middleware.py`"。这里的解法是**拆目录 + 分层**，
不是加抽象层：

- `webx/core/middleware.py` —— **核心层保留**，做认证与角色闸门（不需要查库的那部分）。
- `webx/middleware/` —— **独立的可插拔目录**，一个中间件一个文件（`client_ip` / `lang` / `request_id`）。
- 需要查库的中间件（如 RBAC 权限加载）也在 `core/middleware.py`。
  **中间件直接 import models / services 是正常的**，那是框架内部的分层依赖，
  不必为此套一层依赖注入。
- 业务项目建自己的 `app/middlewares/`，结构完全相同，通过 `__mid__.py` 挂载。

**加中间件永远不需要改框架文件。**

**2. 路径不再靠 `__file__` 上溯，配置也不在 core 里**

webx 是独立安装的包，`Path(__file__).parent.parent` 只会溯到 site-packages。
项目根统一由 `webx.paths` 裁定：`configure_root()` → `WEBX_ROOT` 环境变量 → `CWD`。
controller 目录由 `importlib.util.find_spec(包名)` 反查，包与目录永远一致。
`paths` 与 `config` 提到顶层，解掉了 `core ⇄ lib` 的包级循环（见上方结构说明）。

**3. 配置与异常不再写死在框架里**

- `BaseSettings` 只有通用字段，业务继承后 `configure_settings()` 注册；框架内部拿到的是延迟解析的代理。
- 模型模块由 `MODELS_MODULES` 声明，框架不假设业务模型叫什么。
- i18n 改注册表（`register(lang, {...})`），文案不再是框架里的一个 dataclass。
- 异常收敛成 `WebxError` 基类 + 一个处理器，加异常不改主入口。

## 快速开始

**本仓库不做成安装包**——拉下来就是项目，直接在里面开发。

```bash
git clone <repo> myproject && cd myproject
uv sync                 # 只装第三方依赖，不安装本仓库自身
cp .env.example .env    # 改 APP_NAME / 数据库 / TOKEN_SECRET
python tools/dbsync.py  # 建表
python run.py --dev
```

`pyproject.toml` 里 `[tool.uv] package = false` 就是这个意思：代码靠 `sys.path`
直接 import，改完即生效，不需要重新安装。

目录长这样：

```
myproject/
├── run.py            启动入口
├── .env
├── tools/            运维脚本，直接 python tools/xxx.py 跑
│   ├── dbsync.py     表结构同步
│   ├── admin.py      管理员账号
│   └── org.py        组织与权限初始化
├── app/              ← 你的业务代码，改这里（自带的 note/welcome 是示例，可直接删）
│   ├── config.py     class Settings(BaseSettings) + configure_settings()
│   ├── main.py       app = create_app("app.controller")
│   ├── i18n.py       register("zh", {...})
│   ├── models/       MODELS_MODULES 指向这里
│   ├── middlewares/  自己的中间件，一件一文件
│   └── controller/
│       ├── __mid__.py   MIDDLEWARES = [request_id, client_ip, lang]
│       ├── public.py    公开接口
│       └── api/
│           ├── __mid__.py   MIDDLEWARES = [auth_middleware]  ← 继承 + 追加
│           └── note.py
├── webx/             ← 框架代码，一般不用改
└── tests/
```

`webx/` 与 `app/` 结构对称，且**严格自底向上分层**（下层绝不 import 上层）：

```
webx/
├── paths.py       项目根定位，零依赖
├── config.py      BaseSettings，只依赖 paths + environs
├── lib/           令牌、密码哈希、表结构同步、权限注册表、发送通道、短信（零 ORM）
├── models/        user / admin / verify_code / rbac / dtos
├── services/      user / admin / verifycode / permission / init_permission
├── core/          蓝图扫描、中间件、应用工厂、ORM 初始化、异常、i18n、日志
├── middleware/    可插拔中间件，一件一文件
├── controller/    内置路由，与业务项目同一套 __mid__.py 机制
│   ├── auth/      公开
│   ├── api/       需登录
│   └── admin/     需管理员 + 权限加载
```

`config` 与 `paths` **不在 core 里**：它们是被所有层使用的叶子模块，放进 core 会让
`lib` / `services` 反向依赖 `core`，而 `core` 又依赖 `lib.token`——构成
`core ⇄ lib` 的包级循环。同理 `init_permission` 因要写库而属于 `services` 而非 `lib`。

这条分层约束由 `tests/test_layering.py` 用 AST 扫描全部 import 焊死，
一次随手的反向 import 就会让测试失败。

开箱即有 39 条路由，其中 33 条是 `webx/controller/` 内置的，业务侧一行挂载代码都不用写。
`app/` 里自带的 `Note` 模型与 `/public/welcome`、`/api/notes` 只是示例，
用来演示中间件链与目录约定——起项目时删掉即可（`Note` 表会跟着 `MODELS_MODULES` 建出来）。

三个文件的核心内容：

```python
# app/config.py
from dataclasses import dataclass, field
from webx.config import BaseSettings, configure_settings, env

@dataclass
class Settings(BaseSettings):
    MODELS_MODULES: list[str] = field(default_factory=lambda: ["app.models"])
    WELCOME_TEXT: str = field(default_factory=lambda: env.str("WELCOME_TEXT", "hi"))

settings = configure_settings(Settings())

# app/main.py
from webx.core.app import create_app
app = create_app("app.controller")

# app/controller/api/__mid__.py
from webx.core.middleware import auth_middleware
MIDDLEWARES = [auth_middleware]
```

## 中间件机制

`__mid__.py` 的 `MIDDLEWARES` 按目录层级**继承**，模块内的 `__mid__` 变量按模块**追加**：

```
controller/__mid__.py       → [request_id, client_ip, lang]
controller/api/__mid__.py   → [auth_middleware]
controller/api/note.py      → 实际链条 = 上面两条合并去重（保序）
```

webx 内置的 `controller/` 用的是**同一套规则**，没有任何特殊待遇。

角色闸门与内部接口隐藏：

```python
from webx.core.middleware import auth_middleware, admin_auth_middleware
from webx.core.middleware import make_auth_middleware, make_role_middleware

MIDDLEWARES = [auth_middleware]                             # 普通用户
MIDDLEWARES = [admin_auth_middleware]                       # 管理端（role == admin）
MIDDLEWARES = [make_auth_middleware(required_roles=["vip"])]      # 非 vip → 403
MIDDLEWARES = [auth_middleware, make_role_middleware("inner", status=404)]
#                                ↑ 非 inner → 404，连接口存在性都不暴露
```

自己写一条中间件就是写个普通 async 函数，**可以直接用 models 和 services**：

```python
# app/middlewares/active_user.py
from app.models import Membership

async def require_active_membership(request):
    if not await Membership.filter(user_id=request.ctx.user["user_id"], active=1).exists():
        return json({"error": "会员已过期"}, status=403)   # 返回 response 即短路
```

### 蓝图的写法

```python
bp = Blueprint("note", url_prefix="/api/notes")   # 模块级，自动被扫到
```

需要把同一个 controller 包注册进多个 app（少见，通常只有测试才这么干）时，
`__blueprints__` 也接受一个工厂函数，每个 app 拿到全新实例：

```python
__blueprints__ = lambda: [make_note_blueprint()]
```

## 运维脚本

都在 `tools/`，直接跑，不用装包也不用 `-m`：

```bash
# 表结构同步：模型即权威
python tools/dbsync.py
python tools/dbsync.py --debug     # 打印每条 DDL
python tools/dbsync.py --drop      # 删除模型中已不存在的列（危险）

# 组织
python tools/org.py list
python tools/org.py create -n "研发中心"
python tools/org.py setlevel -n "研发中心" -l 2
python tools/org.py init-perm --default-org      # 同步权限定义到库

# 管理员账号
python tools/admin.py show
python tools/admin.py create -n rootadmin -p 'secret123' --super --org 研发中心
python tools/admin.py passwd -u rootadmin -p 'newsecret'
python tools/admin.py addrole -u alice -r manager --org 研发中心
python tools/admin.py delete -u alice
```

冷启动顺序：`dbsync` 建表 → `org create` 建组织 → `org init-perm` 同步权限 →
`admin create --super` 建第一个超管。

脚本自己把项目根塞进 `sys.path`（见 `tools/_bootstrap.py`），所以从任何目录执行都成立。

## 用户体系与权限体系

启用只有两步：把 `"webx.models"` 列进 `MODELS_MODULES`，跑一次 `dbsync` 建表。
路由由 `create_app` 自动挂载（`builtin_controller=True` 是默认值）。

```python
# app/config.py
MODELS_MODULES: list[str] = field(
    default_factory=lambda: ["app.models", "webx.models"]
)
```

表：`user` / `superadmin` / `verify_code` / `organization` / `user_org` /
`user_role` / `permission` / `role_permission` / `user_permission`。

`User`（C 端）与 `Admin`（管理端）是**两套独立身份**，不共表、不互认，
令牌里用 `role="admin"` 区分。权限判定优先级：
**组织超管（`UserOrg.is_root`）/ `super` 角色 → 用户单独权限 → 角色权限**，
单独权限可以显式 `deny` 掉角色自带的权限。

⚠️ 两级超管是**不同概念，不共用 key**：

| | 出处 | 落点 | `is_privileged()` 认吗 |
|---|---|---|---|
| 系统级 | `Admin.is_root` → token | `ctx.user["sys_root"]` | **不认** |
| 组织级 | `UserOrg.is_root` → 查库 | `ctx.is_root` | 认 |

系统超管若不属于某组织，就不该越过该组织的权限体系——这是多租户下想要的行为。
确需跨组织通吃时，显式判 `ctx.user["sys_root"]`。

### 验证码发送通道

邮件走 stdlib（永远可用），短信是可选依赖。通道用注册表管理，
未注册的通道返回明确的"暂未开通"，而不是 ImportError：

```python
from webx.lib.senders import register_sender
from webx.lib.sms import SmsScene, send_scene_sms

async def send_sms_code(identifier, code, *, valid_minutes, **kw):
    await send_scene_sms(scene=SmsScene.VERIFY_CODE, to=identifier,
                         params={"code": code}, validity_period=valid_minutes * 60)

register_sender("mobile", send_sms_code)
```

短信另需 `uv sync --extra sms`（拉 httpx），业务 Settings 继承 `webx.lib.sms.SmsSettingsMixin`。

## 文件上传下载

存储层分三块，**下载与上传的"续传"不是一回事**：

| 模块 | 内容 | 有无状态 |
|---|---|---|
| `lib/storage.py` | 路径穿越守卫、流式落盘、原子落位 | 无状态 |
| `lib/ranges.py` | HTTP `Range` 解析（**下载**断点续传） | 无状态 |
| `lib/signer.py` | 限时签名（复用 WebxToken，零新依赖） | 无状态 |
| `lib/chunked_upload.py` | **大文件分片上传**（tus 语义会话） | 有状态，**独立可选** |

分片上传单独成模块：不用它的项目完全不会碰到会话目录、`meta.json`、超龄回收那一套。

```bash
# 整传（流式落盘，峰值内存 = 单个分块）
curl -H "Authorization: Bearer $T" -F file=@big.zip /api/files/mybucket

# 分片上传：init → 逐片 PATCH → complete
curl -X POST -H "Authorization: Bearer $T" -d '{"filename":"big.zip","total":10485760}' \
     /api/files/mybucket/uploads
curl -X PATCH -H "Upload-Offset: 0" --data-binary @part0 /api/files/uploads/$UID
curl -X POST /api/files/uploads/$UID/complete

# 下载（流式 + Range 断点续传）
curl -H "Range: bytes=0-1023" /api/files/mybucket/big.zip

# 签发限时链接 → 无需登录即可下载
curl -X POST -d '{"ttl_seconds":600}' /api/files/mybucket/big.zip/sign
```

三条从生产事故换来的约束，改动前先读懂：

- **下载必须 `file_stream` 不能 `file()`**。后者把整份文件读进内存再发，
  几十 MB 就能卡住事件循环、并发下载直接打满内存。
- **先写 `.tmp` 再 `os.replace`**，且 tmp 必须与目标同目录（只有同目录 rename 是原子的），
  否则下载方会读到半截文件。
- **分片追加要在 flock 内复核 offset**。"先读大小再写"存在 TOCTOU，
  多 worker 并发 PATCH 同一会话会串写错位。

安全上：对象键逐段校验 + 解析后必须仍在存储根内（后者才挡得住符号链接逃逸）；
签名与对象键绑定，挪不到别的文件；签名无效回 404 而非 403，不泄露文件是否存在。

### 权限定义

走注册表，框架不预置任何业务权限：

```python
# app/permissions.py —— 必须被 import 到（注册靠 import 副作用）
from webx.lib.permissions import register_module, register_permissions

register_module("order", "订单管理")
register_permissions({
    "view-order": {"display_name": "查看订单", "module": "order",
                   "role": ["super", "manager"]},
})
```

`init_permissions()` 会把注册表同步进库，并删除代码里已移除的权限。
注册表为空时**一律跳过**（不会误删）——空表意味着"还没注册"，不是"全删了"。

## 未搬运的部分

- **`openapi_enricher.py`**（原项目 816 行）：直接 import 业务 DTO 与具体控制器常量，
  是业务件而非框架件，**有意未搬**，不是遗漏。
- worker / 补偿器 / ZMQ broker / 存储服务：按设计目标排除，webx 只面向纯 web 服务。
- 原 `tools/` 里的 `keygen.py`、`fetch_cert.py`（ZMQ CURVE 证书）、
  `reconciler_server.py`、`cleanup_chain.py`（业务任务链）、`codeprompt.py`（开发辅助）：
  非 web 框架件。与 web 相关的 `admin_cmd` / `setauth_cmd` / `init_permission_cmd` /
  `dbsync` 已整合进 `tools/`。
- 业务模型（Task / Notice 等）与业务 services：留在原项目。

关于用户模型：webx 给的是**具体的 `User` / `Admin` 表**，不做「可替换用户模型」那套机制。
实测 Tortoise 的 `Meta` 是普通类属性、子类会直接遮蔽父类，抽象基类上的
`indexes` / `unique_together` 在子类表上会**静默消失**——是最危险的那类失败。
需要额外字段就建关联表，或用 `User.extra`（JSON 文本）兜零散扩展。

## 测试

```bash
.venv/bin/python -m pytest tests/ -q
```

143 个用例，八层覆盖：

| 文件 | 覆盖 |
|---|---|
| `test_smoke.py` | 装配/路由/中间件链/令牌/异常渲染（"硬编码路径已拔干净"的验收） |
| `test_rbac.py` | 权限判定逻辑（纯函数，不查库） |
| `test_auth_db.py` | 注册登录锁定、验证码三层防刷与状态流转（真查库） |
| `test_rbac_db.py` | `role_perm_middleware` 三表查询、权限同步与清理、单独授权（真查库） |

| `test_http_e2e.py` | 真起应用跑完整 HTTP 链路：发码 → 注册 → 登录 → 个人中心 → 管理端鉴权 |
| `test_layering.py` | 包级依赖必须无环且自上而下（AST 扫描全部 import） |
| `test_storage.py` | 路径穿越、原子落位、Range 三态、签名防挪用 |
| `test_chunked_upload.py` | 分片上传：offset 冲突自愈、不完整不落位、会话回收 |
| `test_cli.py` | 命令行工具：建账号事务性、组织解析、权限初始化（真查库） |

查库测试用 sqlite 内存/文件库，不需要本地起 PostgreSQL。**框架代码不为测试做任何让步**——
夹具直接 `Tortoise.init(db_url="sqlite://...")`，绕开只面向 PG 的 `tortoise_config()`。

`schema_sync` 是从 deepmice-api 逐字搬运的（仅改 docstring 路径字样，无逻辑改动），
已在真实项目中长期验证，此处不再重复覆盖。
