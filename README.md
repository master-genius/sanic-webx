# webx

通用 Sanic 服务框架，从 deepmice-api 的自建体系中抽离、去业务化重构而来：
账号 / 权限 / 文件 / 验证码这些 Web 基础件，加上一套**通用的异步任务调度**（任务模块、领取服务、补偿器、独立存储服务、算力 worker）。
框架里没有任何具体的业务任务——任务类型由使用方注册，计算由 worker 的 channel 脚本承担。

## 组成与文档

| 部分 | 在哪 | 是什么 | 文档 |
|---|---|---|---|
| **webx 核心** | `webx/` | 框架骨架、账号 / 权限 / 文件 / 验证码、**任务模块**（模型、服务、`api` / `admin` / `internal` 三组接口，默认启用） | `docs/task-api.md`、`docs/api-conventions.md` |
| 领取服务 | `task_server/` | worker 用 HTTP 长连接来领任务的单进程服务 | `docs/task-scheduling.md` |
| 补偿器 | `tools/reconciler.py` | 租约回收、子任务准入、巡检；全局唯一一份 | `docs/task-scheduling.md` |
| 存储服务 | `webx_storage/` | 独立部署的对象存储（整传 / 分片 / 签名 / 任务产物策略） | `docs/storage-service.md` |
| worker | `webx_worker/` | 算力节点：拉取任务、执行 channel 脚本、上传产物。**完全独立**（仅标准库 + httpx），可单独打包 | `docs/worker.md`、`docs/worker-protocol.md` |
| 部署与验证 | `deploy/`、`server.sh`、`tools/smoke.py` | systemd 单元、角色化启动、上线冒烟 | `deploy/README.md`、`docs/verification.md`、`docs/testing.md` |

依赖方向（由 `tests/test_layering.py` 焊死）：webx 核心**不依赖** `webx_storage` / `webx_worker`；`webx_storage` 可以依赖 webx 的基础服务；
`webx_worker` 不 import webx 的任何东西——两边只通过 `docs/worker-protocol.md` 定义的 HTTP 协议对话。

## 能力清单

**框架骨架（core）**

| 能力 | 模块 | 说明 |
|---|---|---|
| 加密令牌 | `webx.lib.token` | AES-256-GCM，libcrypto 优先 + 纯 Python 兜底，零第三方依赖 |
| ORM 表结构同步 | `webx.lib.schema_sync` | 模型即权威，对比 information_schema 生成最小 DDL |
| 自动蓝图路由 | `webx.core.auto_blueprint` | 递归扫描 controller 包，自动发现并注册蓝图 |
| 层级中间件 | `webx.middleware/` | 一个中间件一个模块；`__mid__.py` 目录继承 + 模块级追加 |

**内置路由（`webx/controller/`）** —— 装了就有全套端点，不用一个项目写一遍

| 目录 | 端点 |
|---|---|
| `controller/auth/` | 账密登录注册、管理员登录、验证码登录、找回密码、登出、刷新令牌 |
| | 注册与找回都是**邮箱/手机二选一**；短信未配置或被关闭时自动挡住手机路径 |
| `controller/api/` | 个人中心 + 文件（整传 / 分片 `/api/uploads`、下载、签发限时链接 `…/signatures`、删除）+ **任务**（`/api/tasks`：创建、编辑、提交、取消、重试…） |
| `controller/admin/` | 账号 CRUD、角色/权限查询、用户角色管理、单独授权、全站任务列表与计数 |
| `controller/public/` | 签名下载（无需登录，靠限时签名授权） |
| `controller/internal/` | **服务间回调**（worker → web，`/internal/tasks/<id>/…`）；整组由 `X-Internal-Token` 鉴权，令牌未配置则全拒 |

内置路由**走的是同一套 `__mid__.py` 扫描机制**，没有任何特殊待遇——
每个目录自己声明中间件，`create_app` 一并扫描。不需要就 `builtin_controller=False`。

## 相对原架构的三处改造

**1. 中间件不再焊死在 core**

原项目的痛点是"加中间件要改 `core/middleware.py`"。这里的解法是**拆目录 + 分层**，
不是加抽象层——`core/middleware.py` 这个文件**已经不存在了**：

| 模块 | 内容 |
|---|---|
| `middleware/auth.py` | 阶段 1 认证：`auth_middleware`、`make_auth_middleware`、`make_role_middleware` |
| `middleware/admin.py` | 管理端认证（`role == admin`），就是工厂的一次带参调用 |
| `middleware/perm.py` | 阶段 2/3 RBAC：`role_perm_middleware`、`@require_permission`、`has_perm` |
| `middleware/cors.py` | CORS 预检与响应头（应用级装配） |
| `middleware/{client_ip,lang,request_id}.py` | 通用件，一件一文件 |

- **一个中间件一个模块**，唯一的例外是 `perm.py`：写 `ctx.permissions` 的中间件与读它的
  装饰器共用同一份数据契约，拆开只会变成互相 import。
- **中间件直接 import models / services 是正常的**，那是框架内部的分层依赖，
  不必为此套一层依赖注入。正因为它要查库，`middleware/` 在分层里排在 `models`/`services`
  **之上**、`core` **之下**——搬出去之后 `core` 不再依赖任何业务模型。
- 业务项目建自己的 `app/middlewares/`，结构完全相同，通过 `__mid__.py` 挂载。

**加中间件永远不需要改框架文件。**

`__mid__.py` **加载失败不会静默降级**：文件不存在 = 该目录不追加中间件（合法配置）；
文件存在但导入/求值失败 = 配置意图未知，抛 `MiddlewareConfigError` → 该目录**及其所有
子目录**一律不注册路由（对外 404 不可达），并记一条 ERROR。理由是鉴权配置的失败方式
只能是「不放行」，不能是「当作没配」——否则 `__mid__.py` 里一个笔误就让整组接口裸奔。
根 `__mid__.py`（`create_app` 每次扫描的那个包自己那份）失败则直接抛：它决定所有组的
基础链，失败即零路由空服务，还会骗过健康检查。

**全站中间件用 `create_app(global_middleware=[...])`**。中间件共四层，从外到内是
「全站 → 包（根 `__mid__.py`）→ 目录 → 模块（`__mid__` 变量）」，全站这层跟随**部署**：
同一份链传给每一次扫描，故**内置路由与业务路由都吃得到**——此前业务的
`app/controller/__mid__.py` 盖不到 `/api/user` 这类内置端点，且没有任何提示。
与包级重复声明**不会双跑**（按 identity 去重），迁移可以分两步走。

⚠️ 只放**幂等的上下文注入**（`request_id` / `client_ip` / `lang` / `trace`）。
**做准入决策的一律不准进**（鉴权、限流、权限）：全站链对所有包生效，包括 `/auth/login`
与签名下载端点，把 `auth_middleware` 放进去一行配置就锁死全站。另外蓝图级中间件在
路由匹配成功后才执行，**不覆盖 404**——需要覆盖 404 的（访问日志、trace）用 Sanic
原生的 app 级中间件，不要混进 `__mid__` 体系。

**`controller_package` 可以收列表**：`create_app(["app.controller", "appx.controller"])`
——一个进程按需装配若干业务包，用于「不同模块部署到不同服务」。各包独立扫描、各自继承
自己的根 `__mid__.py`，**互不影响**（中间件是逐个复制到蓝图对象上的，不存在共享容器）。
唯一的跨包约束是蓝图名全局唯一。

**内置蓝图统一带 `webx_` 前缀**（`webx_user` / `webx_file` / `webx_auth` …），业务侧可以
自由使用 `user`、`file` 这些自然名字。蓝图名是**全局唯一的命名空间**，与 URL 前缀无关——
两个蓝图哪怕路径毫不相干，重名照样抛 `AssertionError: Blueprint names must be unique`。
反过来，`/api` 这个前缀**不被任何人占有**：它只是字符串，业务用 `/api/order`、`/api/note`
不受任何限制，只要具体路径不与内置的 `/api/user/*`、`/api/files/*`、`/api/admin/*`、
`/auth/*`、`/public/files/*` 相撞即可（撞了会 `RouteExists`，启动即崩，不会带病上线）。

中间件是**蓝图级**的：请求命中哪个蓝图的路由，就跑那个蓝图的链。所以 URL 前缀相同的
两个蓝图（内置 `/api/user` 与业务 `/api/order`）各走各的中间件，不存在任何串扰。

**跨模块 import 的蓝图会被自动剔除**：扫描器按路由 handler 的 `__module__` 判定蓝图归属，
`from ..task import *` 顺手带进来的别组蓝图**无声跳过**，不会被挂上本组的中间件。
判据管不到的两种情况各有兜底、且都是启动即失败，绝不静默改动鉴权链：显式
`__blueprints__` 里写了别人的蓝图 → `DuplicateBlueprintError`；蓝图被判为外来却无人
注册（接口会静默 404）→ 扫描后核对，抛 `OrphanBlueprintError`，修法是在真正拥有它的
模块里声明 `__blueprints__ = [该蓝图]`。

**2. 路径不再靠 `__file__` 上溯，配置与文案也不在 core 里**

webx 是独立安装的包，`Path(__file__).parent.parent` 只会溯到 site-packages。
项目根统一由 `webx.paths` 裁定：`configure_root()` → `WEBX_ROOT` 环境变量 → `CWD`。
controller 目录由 `importlib.util.find_spec(包名)` 反查，包与目录永远一致。
`paths` / `config` / `i18n` 三个叶子模块提到顶层，解掉了 `core ⇄ lib` 的包级循环，
也让 `middleware/` 能排在 `core` 之下（见上方结构说明）。

**3. 配置与异常不再写死在框架里**

- `BaseSettings` 只有通用字段，业务继承后 `configure_settings()` 注册；框架内部拿到的是延迟解析的代理。
- 模型模块**跟随装配的模块**按约定扫出来（`<pkg>/models`），没有配置项，框架也不假设业务模型叫什么。
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

### 启动方式

```bash
python run.py --dev                       # 开发：单进程，好断点
./server.sh --dev --workers 1             # 同上，但走 sanic CLI（热重载）
./server.sh --workers 4                   # 生产：4 个 worker，崩了自动重启
./server.sh --port 9000 --modules app,appx  # 指定端口与装配的业务模块

# 其它角色同一个入口（不写角色 = web）：
./server.sh task                          # 领取服务（单进程）
./server.sh storage --workers 4           # 存储服务
./server.sh reconciler                    # 补偿器
./server.sh worker --home /data/worker    # 算力 worker
```

两种入口共用 `run.py` 里的**模块级** `app` 变量：`python run.py` 时
`__main__` 守卫触发 `app.run()`；`sanic run:app` 时 Sanic 自己接管服务器，
守卫不成立所以不会重复启动（重复调用 `app.run()` 会以
`RuntimeError: context has already been set` 直接起不来）。

### 装配什么：两个独立开关

| 变量 | 管什么 | 取值 |
|---|---|---|
| `WEBX_BUILTIN` | **内置路由组**（框架自带的） | 不配 = 全挂 / `false` = 全不挂 / `auth,api` = 子集 |
| `WEBX_MODULES` | **业务模块**（你写的代码） | `app` / `app,appx` |

加载顺序是 **内置组恒在前、业务模块在后**。两者互不影响——`WEBX_MODULES=app`
**不会**屏蔽内置路由，要关得用 `WEBX_BUILTIN=false`。

```bash
./server.sh --no-builtin             # 纯业务服务，零内置端点
./server.sh --builtin auth,api,public  # 只要登录、用户/文件与签名下载；不要 admin 后台，也没有任务回调（internal）
```

`WEBX_BUILTIN` 的取值语义——**只有显式 `false` 才不加载**：

| 配置 | 结果 |
|---|---|
| 不配 / 留空 / 值看不懂（如 `xxx`） | 全挂 + WARNING（若是看不懂的值） |
| `false` / `0` / `no` / `none` / `off` | 全不挂 |
| `auth,api` | 只挂这两组 |
| `auth,nope` | 只挂 `auth`，`nope` 剔除 + WARNING |

组名写错**不中止启动**，剔除后继续；一个有效的都不剩就回退全加载。取舍是可用性优先
——静默关掉全部内置路由会让 `/auth/login` 一起消失，比多挂几组更难排查。但每次回退
都会打 WARNING，否则"我以为只挂了 auth"和"实际全挂了"就永远暴露不出来。

⚠️ **默认全挂会带上用户 CRUD 与整套 admin 权限端点**，用不到就配 `false`，别白送攻击面。

两个变量都有三个配置位，优先级由高到低：

| 位置 | 写法 | 适用 |
|---|---|---|
| 命令行 | `./server.sh --modules app,appx --no-builtin`<br>`python tools/dbsync.py --modules app,appx` | 临时覆盖 |
| shell 环境变量 | `WEBX_MODULES=app,appx WEBX_BUILTIN=false sanic run:app` | 容器 / systemd / CI |
| `.env` | `WEBX_MODULES=app`<br>`WEBX_BUILTIN=` | **常规配置位** |

⚠️ 两个容易踩的点：

1. **`sanic` CLI 严格拒绝未知参数**（`sanic: error: unrecognized arguments`），不是忽略。
   所以 `WEBX_MODULES=app,appx sanic run:app` 可以，`sanic run:app --modules=...` 不行。
   `python run.py --modules app,appx` 则两条路都通，`server.sh` 已经处理好这个差异。
2. 框架内部读它用的是 `env.str` 而**不是** `os.environ`——environs ≥ 15 的
   `read_env()` 只写进 Env 实例、不回写 `os.environ`，用后者会让 `.env` 里的配置
   静默失效。你自己写业务配置时同理，一律 `from webx.config import env`。

目录长这样：

```
myproject/
├── run.py            启动入口（模块级 app，两种启动方式共用）
├── server.sh         各角色统一启动脚本（web / task / storage / reconciler / worker）
├── .env
├── deploy/           systemd 单元模板 + 部署说明
├── docs/             规范与说明文档（任务接口、调度、存储、worker 协议、验证…）
├── tools/            运维脚本，直接 python tools/xxx.py 跑
│   ├── dbsync.py     表结构同步
│   ├── admin.py      管理员账号
│   ├── org.py        组织与权限初始化
│   ├── reconciler.py 补偿器（独立进程）
│   ├── smoke.py      上线冒烟（提交真实任务走一遍全链路）
│   └── clear_cache.sh 清 __pycache__
├── task_server/      领取服务（独立进程，认领 SQL 在 webx/services/claim.py）
├── webx_storage/     独立存储服务
├── webx_worker/      算力 worker（完全独立，不 import webx）
├── app/              ← 你的业务代码，改这里（自带的 note/welcome 是示例，可直接删）
│   ├── config.py     class Settings(BaseSettings) + configure_settings()
│   ├── i18n.py       register("zh", {...})       ← 按约定自动 import
│   ├── models/       按约定自动挂接（改名 _models 即排除）
│   ├── middlewares/  自己的中间件，一件一文件
│   └── controller/   ← 按约定被扫描（必须存在）
│       ├── __mid__.py   MIDDLEWARES = [request_id, client_ip, lang]
│       ├── public.py    公开接口
│       └── api/
│           ├── __mid__.py   MIDDLEWARES = [auth_middleware]  ← 继承 + 追加
│           └── note.py
├── webx/             ← 框架代码，一般不用改
└── tests/
```

**业务侧不需要写入口模块**。`create_app(modules=["app"])` 按目录约定装配：
`<pkg>/controller/` 必须有（缺了报错——静默跳过等于"装配成功但一个接口都没有"），
`<pkg>/i18n.py` 与 `<pkg>/models` 存在就自动挂上、不存在就跳过。

开一个新业务模块只要三步：建 `controller/` 写路由、需要文案就加 `i18n.py`、
需要自己的配置就写 `config.py`（独立 dataclass，用 `from webx.config import env`
读同一份 `.env`，与全局互不干扰）。要 admin 角色控制就直接
`from webx.middleware.perm import require_permission`——没有任何注册或声明文件。

`webx/` 与 `app/` 结构对称，且**严格自底向上分层**（下层绝不 import 上层）：

```
webx/
├── paths.py       项目根定位，零依赖
├── config.py      BaseSettings，只依赖 paths + environs
├── i18n.py        文案注册表，零依赖（全框架共用，故必须是叶子）
├── lib/           令牌、密码哈希、表结构同步、权限注册表、发送通道、短信、存储、任务类型注册表 / 配额 / 点数接缝（零 ORM）
├── models/        user / admin / verify_code / rbac / task / dtos
├── services/      user / admin / verifycode / permission / init_permission / storage / task / lease / claim / reconciler
├── middleware/    可插拔中间件，一件一文件（认证 / RBAC / CORS / 通用件）
├── core/          应用工厂、蓝图扫描、ORM 初始化、异常、日志、路由级守卫
├── controller/    内置路由，与业务项目同一套 __mid__.py 机制
│   ├── auth/      公开
│   ├── api/       需登录
│   ├── admin/     需管理员 + 权限加载
│   ├── public/    签名下载
│   └── internal/  服务间回调（内部令牌）
```

`config` / `paths` / `i18n` **不在 core 里**：它们是被所有层使用的叶子模块，放进 core 会让
`lib` / `services` / `middleware` 反向依赖 `core`，而 `core` 又依赖 `lib.token`——构成
`core ⇄ lib` 的包级循环。同理 `init_permission` 因要写库而属于 `services` 而非 `lib`，
`get_client_ip` 因中间件与路由装饰器都要用而沉在 `lib` 而非 `core.guards`。

`middleware` 排在 `core` **之下**：鉴权中间件要查库（依赖 `models`/`services`），
而 `core.app` 要装配 CORS、`core.guards` 要复用 `DEFAULT_CLAIMS`，两条约束联立只有这一个解。

这条分层约束由 `tests/test_layering.py` 用 AST 扫描全部 import 焊死，
一次随手的反向 import 就会让测试失败。

开箱即有 86 条路由，其中 80 条是 `webx/controller/` 内置的（含任务接口），业务侧一行挂载代码都不用写。
路径设计遵循 `docs/api-conventions.md`；其中改过名的旧路径仍可用，响应带 `Deprecation` 头。
`app/` 里自带的 `Note` 模型与 `/public/welcome`、`/api/notes` 只是示例，
用来演示中间件链与目录约定——起项目时删掉即可（`Note` 表跟着 `app/models` 自动建出来）。

三个文件的核心内容：

```python
# app/config.py
from dataclasses import dataclass, field
from webx.config import BaseSettings, configure_settings, env

@dataclass
class Settings(BaseSettings):
    # 只写业务自有字段；模型模块按约定自动挂接，无需声明
    WELCOME_TEXT: str = field(default_factory=lambda: env.str("WELCOME_TEXT", "hi"))

settings = configure_settings(Settings())

# run.py —— 唯一入口，业务不需要写 app/main.py
app = create_app(modules=["app"])     # 按约定展开 app/controller、app/i18n

# app/controller/api/__mid__.py
from webx.middleware import auth_middleware
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
from webx.middleware import auth_middleware, admin_auth_middleware
from webx.middleware import make_auth_middleware, make_role_middleware

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
python tools/dbsync.py --modules app,appx   # 额外带上这些模块的 <pkg>.models
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

**建表范围与服务装配同源。** 同步的模型 = `--modules` / `WEBX_MODULES` 指定的各模块的
`<pkg>/models`，外加 `webx/models`（未显式关掉内置路由时）。**没有模型清单配置项**——
服务进程读同一个环境变量、走同一个 `apply_module_models`，所以

```bash
WEBX_MODULES=app,appx python tools/dbsync.py   # 建表
WEBX_MODULES=app,appx ./server.sh              # 起服务
```

两条命令覆盖的模块集合必然一致。若两边各算各的，就会出现「服务路由齐全但表没建」
——接口一查库就 `no such table`，而建表那一步毫无征兆。

## 用户体系与权限体系

启用只有一步：跑一次 `dbsync` 建表。路由与模型都自动挂接——只要没显式关掉内置路由
（`WEBX_BUILTIN=false`），`webx/models` 就跟着一起走，**不需要在任何地方声明**。

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

邮箱与手机号都是**基础能力**，两个通道都默认注册，不需要任何接线代码：

| 通道 | 实现 | 什么时候可用 |
|---|---|---|
| `email` | stdlib `smtplib` | 配了 `MAIL_*` |
| `mobile` | 短信适配层 `webx.lib.sms` | `SMS_ENABLED=true`（默认）**且**所选服务商的凭证、签名齐备 |

不可用的通道，所有入口（发码、注册、找回密码、绑定手机）一律回明确的「暂未开通」，
并且在**限流统计与落库之前**就挡住——不会白写一行验证码、白占该号码的当日额度。
所以：**配好 `.env` 就能发短信；不想要就 `SMS_ENABLED=false`。**

```bash
# .env
SMS_PROVIDER=aliyun            # 服务商驱动，默认 aliyun
SMS_ACCESS_KEY_ID=...
SMS_ACCESS_KEY_SECRET=...
SMS_SIGN_NAME=你的签名
SMS_TEMPLATES={"verify_code":"SMS_123456789"}   # 场景 → 控制台报备的模板码
```

模板变量名必须是 `${code}`。字段名与 deepmice-api 一致，`.env` 可直接复用。

**服务商是可换的**：`webx/lib/sms/` 是适配层——`base.py` 定义驱动契约 `SmsDriver`，
`gateway.py` 是与服务商无关的统一入口，`aliyun.py` 是目前唯一的驱动。业务代码只面对：

```python
from webx.lib.sms import SmsScene, send_scene_sms, is_ready

await send_scene_sms(SmsScene.VERIFY_CODE, "13800138000", {"code": "123456"})
```

接入新服务商 = 实现 `SmsDriver`（`is_configured()` + `send()`）、`register_driver("名字", 工厂)`，
再把 `SMS_PROVIDER` 指过去，调用方与验证码服务一行不用动。`SMS_PROVIDER` 写了没注册的名字时，
短信视为不可用，并打一条 ERROR（同一个名字只打一次）。

⚠️ 阿里云驱动的签名与请求构造由单元测试（`httpx.MockTransport`）覆盖，但**没有对着真实
阿里云验证过**——首次启用请用真实 AK/SK 与已审核的模板联调一次。

邮件与短信之外的渠道，或想整个换掉默认实现，用 `webx.lib.senders.register_sender(渠道, 函数)`
覆盖即可（可选 `enabled=` 传入「当前是否可用」的判断）。

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
# （UP 是上一步响应里的 upload_id；别用 $UID——那是 bash 的只读变量，值是你的用户数字 ID）
curl -X PATCH -H "Authorization: Bearer $T" -H "Upload-Offset: 0" --data-binary @part0 /api/uploads/$UP
curl -X POST -H "Authorization: Bearer $T" /api/uploads/$UP/complete

# 下载（流式 + Range 断点续传）
curl -H "Range: bytes=0-1023" /api/files/mybucket/big.zip

# 签发限时链接 → 无需登录即可下载
curl -X POST -H "Authorization: Bearer $T" -d '{"ttl_seconds":600}' /api/files/mybucket/big.zip/signatures
# ⚠️ 需要先配置 STORAGE_SIGN_SECRET，否则签名功能 fail-closed（503）。tasks/ 开头的 bucket 属于任务模块，通用文件接口一律 403
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

**存储契约要点**（改动前先读）：

- **断点续传客户端读 `Upload-Offset` 响应头**：分片的 `GET`/`PATCH`/`complete` 与 409 都带这个头，
  同时在 JSON 顶层给 `offset`（`data.offset` 仍保留）。409 时头里就是服务端真实 offset，客户端据此重新对齐。
- **分片限制**：单片不得超过 `UPLOAD_CHUNK_MAX_BYTES`（413），空分片回 400；被拒的分片不推进 offset。
- **整传**用 python-multipart 的推式解析器（`webx/lib/multipart_stream.py`），可选表单字段 `filename` 覆盖文件部分的名字。
- **键规则**：`STORAGE_KEY_RULE=strict|lenient`。任何规则下，**点开头的段都是保留区**（`.uploads/`、`.staging/`、
  `.upload.*.tmp`），既不能写也不会被列出。
- **HEAD**：`HEAD /api/files/<bucket>/<name>` 与签名链接的 HEAD 只回 `Content-Length` / `Accept-Ranges`。
- **启动时**（只要挂了 `api` 或 `public` 组）自动建存储目录并回收超龄的分片会话。

## 未搬运的部分

- **`openapi_enricher.py`**（原项目 816 行）：直接 import 业务 DTO 与具体控制器常量，
  是业务件而非框架件，**有意未搬**，不是遗漏。
- **ZMQ**：分发早已改为 worker 经 HTTP 长连接主动领取，ZMQ broker 与 CURVE 证书工具（`keygen.py` / `fetch_cert.py`）已过时，不迁。
- **业务任务类型与业务服务**（具体的计算任务、任务链清理 `cleanup_chain.py`、业务通知等）：留在原项目；
  任务类型由使用方用 `register_task_type` 登记，补偿器的业务动作用 `register_sweep` / `register_failed_hook` 挂钩子。
- **点数 / 计费**：只有接口和闸门（`PricingPolicy`、估算钩子、流水表模型），**没有余额、预扣、退款的实现**——待设计，不在这里臆造。
- **存储保留期清理的自动启动**：原项目的存储启动脚本无条件拉起清理进程，与其“已停用”的文档矛盾，不照搬；
  现在是可配置的独立进程，默认关闭（见 `docs/storage-service.md`）。
- 开发辅助 `codeprompt.py`：非框架件。与 web 相关的 `admin_cmd` / `setauth_cmd` / `init_permission_cmd` / `dbsync` 已整合进 `tools/`。

关于用户模型：webx 给的是**具体的 `User` / `Admin` 表**，不做「可替换用户模型」那套机制。
实测 Tortoise 的 `Meta` 是普通类属性、子类会直接遮蔽父类，抽象基类上的
`indexes` / `unique_together` 在子类表上会**静默消失**——是最危险的那类失败。
需要额外字段就建关联表，或用 `User.extra`（JSON 文本）兜零散扩展。

## 测试

```bash
.venv/bin/python -m pytest tests/ -q
```

约 1000 个用例。认领 / 租约的 SQL 是 PostgreSQL 专有的，**这部分用例连真实 PG**（获取方式见 `docs/testing.md`，
拿不到时是失败而不是跳过）；账号 / 权限 / 文件那部分仍用 sqlite 内存库。完整的验证过程见 `docs/verification.md`。

| 层 | 文件 | 覆盖 |
|---|---|---|
| 框架 | `test_smoke.py` `test_layering.py` `test_create_app_composition.py` | 装配 / 路由 / 中间件链 / 令牌 / 异常渲染；包级依赖无环且自上而下、核心不依赖兄弟包、worker 完全独立 |
| 账号权限 | `test_rbac*.py` `test_auth_db.py` `test_sms.py` `test_mail.py` `test_cli.py` | 权限判定、注册登录锁定、验证码防刷、短信适配层、发件人、命令行工具 |
| 文件 | `test_storage.py` `test_chunked_upload.py` `test_http_e2e.py` `test_storage_service.py` `test_storage_live.py` `test_audit_storage.py` | 路径穿越、原子落位、Range、分片续传与竞争、签名 fail-closed；独立存储服务（含真进程）；通用文件接口不得触碰 `tasks/` |
| 任务（真 PG） | `test_task_core.py` `test_task_lease.py` `test_reconciler.py` `test_task_server.py` `test_task_api.py` `test_task_internal_api.py` | 类型注册表与配额、认领互斥与 fencing、补偿器、领取服务、用户 / worker 接口 |
| worker | `tests/worker/` | 领取 / 租约 / 取消 / 停机 / 恢复 / 上传 / 日志脱敏，对着协议替身 |
| **整栈** | `test_stack_e2e.py` | 五个独立进程拼起来：正常完成、失败、取消、优雅停机回池、`kill -9` 后补偿器回收 |

框架代码不为测试做任何让步：账号权限类夹具直接 `Tortoise.init(db_url="sqlite://...")`，绕开只面向 PG 的 `tortoise_config()`。

`schema_sync` 是从 deepmice-api 逐字搬运的（仅改 docstring 路径字样，无逻辑改动），
已在真实项目中长期验证，此处不再重复覆盖。
