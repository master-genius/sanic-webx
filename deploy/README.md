# 部署说明

webx 由五种角色的进程组成，各自独立启停；它们之间只通过**数据库**和**HTTP**协作，没有共享内存、没有消息队列。

```
用户 ──HTTPS──► 反代 ──┬──► webx-web        :8000   API / 管理端 / 内部回调（/internal 仅对 worker 开放）
                       └──► webx-task       :9020   仅 /task/leases（worker 长连接领任务）
                                │
worker ──HTTP 长连接主动领取────┘     （worker 不被投递，是它自己来领；服务端不存 worker 能力注册表）
worker ──HTTP 回调────────────► webx-web  /internal/tasks/<id>/...
worker ──HTTP 上传产物────────► webx-storage  :9010   /internal/*（仅 worker 与 web 可达）
用户   ──HTTP 下载产物────────► webx-storage  /public/*（签名链接，可对外）

webx-reconciler（全局唯一一份）：回收过期租约 / 准入子任务 / 巡检，只读写数据库
```

| 角色 | 入口 | 进程数 | 单元 |
|---|---|---|---|
| web | `./server.sh web` | 多（`--workers`，默认 4） | `deploy/systemd/webx-web.service` |
| task（领取服务） | `./server.sh task` | **必须 1** | `webx-task.service` |
| storage | `./server.sh storage` | 多（`STORAGE_WORKERS`） | `webx-storage.service` |
| reconciler（补偿器） | `./server.sh reconciler` | **全局只能 1** | `webx-reconciler.service` |
| worker（算力节点） | `./server.sh worker` / 生成器产出的单元 | 每台机器 N 个实例 | **由生成器产出**，见第三节 |

存储可以和 web 同机，也可以独立机部署；worker 可以在任意机房，只要能访问 web / task / storage 三处 HTTP。

---

## 一、云端部署（web + task + reconciler [+ storage]）

### 1. 依赖

```bash
sudo apt-get update && sudo apt-get install -y python3.13 python3.13-venv
curl -LsSf https://astral.sh/uv/install.sh | sh        # 若用 uv；有 .venv 就不需要

cd /opt/webx
uv sync                                                  # 或：python3.13 -m venv .venv && .venv/bin/pip install .
```

`server.sh` 选解释器的顺序是 **`.venv/bin/python` → `uv run` → 系统 `python3`**；systemd 的极简 `PATH` 里没有
`~/.local/bin`，用 uv 时会自己探测常见安装目录，也可在单元里设 `Environment=UV_BIN=/abs/path/to/uv`。

### 2. 配置

```bash
cp .env.example .env && vim .env
```

**必改项**（其余有可用的默认值）：

| 配置 | 说明 |
|---|---|
| `DATABASE_HOST / PORT / NAME / USER / PASSWORD` | PostgreSQL。**数据库钉死 PostgreSQL**：认领 / 租约的 SQL 用 `FOR UPDATE SKIP LOCKED`，不支持其它库 |
| `TOKEN_SECRET` | 用户令牌（AES-256-GCM）主密钥。**<16 字符直接抛异常**，默认值 `change-me-in-production` 绝不能上线 |
| `STORAGE_SIGN_SECRET` | 下载签名密钥，独立于 `TOKEN_SECRET` 以便单独轮换；留空会回落并告警 |
| `INTERNAL_TOKEN` | 服务间令牌。**未配置则 internal 组与存储的 `/internal` 拒绝一切请求**（空配置绝不等价于放行）。必须与每台 worker 的 `WEBX_WORKER_INTERNAL_TOKEN` **逐字一致**，否则回调全 401 |
| `APP_ENV` | 生产设 `prod`。留 `dev` 等于 CORS 对全网放行 |
| `TASK_SERVER_URL` | web / 补偿器“戳醒”领取服务的地址（同机默认 `http://127.0.0.1:9020`） |
| `STORAGE_INTERNAL_URL` | web 删除任务产物时调用存储服务的地址；**留空 = 静默跳过远端删除**，只清本地 |

> 租约、补偿器、领取服务的全部参数也在 `.env` 里。领取服务（task）、web、补偿器三个进程各读各的，但读的是**同一份**配置——
> 租约参数不要在三处各写一份副本，否则 worker 的自熔时刻与服务端的回收时刻会错位（“服务端已租给 B、A 仍认为自己持有”的窗口）。

### 3. 建表（**不会自动发生**）

```bash
python tools/dbsync.py                # 同步表结构；任务表（tasks）与认领索引也在这一步建
```

服务启动**不建表**。加了模型字段却没跑这条，症状是服务正常起来、涉及新列的 SQL 全报 `column does not exist`。
改了模型要重跑；权限定义改了也要重新初始化（见 `tools/org.py init-perm`），两者都是“改了定义才跑”的运维动作，不随重启发生。

首次部署后创建管理员和组织（详见各工具的 `--help`）：

```bash
python tools/org.py create -n "默认组织"
python tools/admin.py create -n admin -p '<密码>' --super --org 默认组织
python tools/org.py init-perm --org 默认组织
```

### 4. 安装单元并启动

单元文件里的 `WorkingDirectory` / `ExecStart` / `User` 写死为 `/opt/webx` 与 `www-data`，**路径或运行用户不同就先改**。

```bash
sudo cp deploy/systemd/webx-*.service /etc/systemd/system/
sudo systemctl daemon-reload
```

推荐启动顺序 **task → reconciler → web**（补偿器与 web 对领取服务只是软依赖：poke 送不到只损失延迟，
任务已经持久在 PENDING 池里，不会丢）：

```bash
sudo systemctl enable --now webx-task
curl -s http://127.0.0.1:9020/health                 # {"ok":true,...}
sudo journalctl -u webx-task -n 20                   # 应有一行启动日志，列出全部租约参数

sudo systemctl enable --now webx-reconciler
sudo journalctl -u webx-reconciler | grep "Reconciler 已启动"
#   这一行是“.env 到底有没有被读到”的唯一权威：它打印生效的各个间隔

sudo systemctl enable --now webx-web
curl -s http://127.0.0.1:8000/health
```

> ⚠️ **不要给任何 `ExecStart` 加参数**（`--scan-interval`、`--port` 之类）。优先级是
> 【命令行 > 环境变量 > `.env` > 默认值】，一旦写死，改 `.env` 将完全失效**且没有任何报错**。
> 唯一的例外是 web 的 `--workers`（没有对应的 `.env` 配置项）。web 的监听地址与端口由 `server.sh` 先从配置里解出来再交给
> sanic CLI（sanic CLI 自己不读我们的配置，不传就是 127.0.0.1:8000），所以改 `.env` 的 `SERVER_HOST` / `SERVER_PORT` 即可。

> ⚠️ **task 必须单进程，reconciler 全局只能一份。** 前者的挂起池活在单个事件循环里，多开会让 web 的一次 poke
> 只能唤醒其中一份；后者多开会重复准入、重复通知。

### 5. 独立存储服务

```bash
sudo systemctl enable --now webx-storage
```

关键配置（`.env`，存储服务读同一份或自己的）：`STORAGE_PORT`（默认 9010）、`STORAGE_DIR`、`STORAGE_WORKERS`、
`STORAGE_INTERNAL_TOKEN`（缺省回落 `INTERNAL_TOKEN`）、`STORAGE_POLICIES`（默认 `task_results`）。
**保留期清理默认不随存储启动**，也不应挂在多 worker 的应用里（每个 worker 各跑一份会互相删对方正在检查的东西）：
需要时单独跑 `python -m webx_storage retention --dry-run` / `--once`，见 `docs/storage-service.md`。

### 6. 反代

worker 领任务走 `/task/leases`。**它没配转发的表现是整站零分发**——任务全部安静停在 PENDING，而日志里只有
worker 侧一行 404，极易被忽略。

```nginx
# 领取服务：只转 /task/leases，不要转 /task/pokes
location /task/leases {
    proxy_pass http://127.0.0.1:9020;
    proxy_set_header Host              $host;
    proxy_set_header X-Real-IP         $remote_addr;
    proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;

    # 🔴 必须 > TASK_LEASE_HOLD_SECONDS（默认 25s）：无任务时请求会挂起，
    # 超时太短会让每次正常挂起都被反代掐断（不损失正确性，但 worker 白重连、日志刷屏）
    proxy_read_timeout 60s;
    proxy_buffering off;
}

# 内部回调：只对 worker 的出口地址开放
location /internal/ {
    allow 10.0.0.0/8;      # worker 所在网段，按实际改
    deny  all;
    proxy_pass http://127.0.0.1:8000;
}
```

> ⚠️ **不要转 `/task/pokes`**：它不鉴权（靠领取服务只绑 127.0.0.1 防护），暴露出去等于让任何人都能唤醒分发循环。
> 存储服务同理：`/internal/*` 只对 worker 与 web 可达，`/public/*` 才可对外。

---

## 二、数据库与升级

- **升级 = 停服务 → 更新代码 → `python tools/dbsync.py` → 启动**。`dbsync` 默认只加列 / 建表，不删；`--drop`（删除模型里已没有的列）
  与 `--force`（类型不兼容则重建列、**清理非模型索引**）有破坏性：`--force` 会把任务认领的手写索引 `idx_tasks_claim`
  当“野索引”删掉。这个索引缺了**不会报错、只会慢约 400 倍**，所以 `dbsync` 在同步结束时会检查并补建它，
  领取服务每次启动也会再确保一次（日志里能看到“已存在 / 已创建”）；你自己手动跑过 DDL 的话也别忘了确认它还在。
- 租约参数（`LEASE_*`）改动对**在途任务**即时生效于下一次心跳；调小之前确认没有长任务正依赖原来的宽限。
- 清缓存：`tools/clear_cache.sh`（升级 Python 小版本后偶发陈旧字节码）。

---

## 三、worker 部署

worker 是**完全独立**的包（`webx_worker`，只依赖标准库 + httpx，不 import webx 的任何东西），可以单独拷到算力机上。
配置、channel（任务类型 → 脚本）、Lane（并发槽位）、环境变量一律见 `docs/worker.md`，这里只讲上线动作。

### 生成 systemd 单元（**不要手写**）

```bash
# 在 worker 主目录里（含 .env 与 channel_config/）
python -m webx_worker service one   --unit-name webx-worker                  # 单实例
python -m webx_worker service multi --help                                   # 多实例：模板单元 + target（参数见 --help）
```

生成器产出单元与（用 conda 时）启动脚本到 `<主目录>/deploy/`，**不改 `/etc/systemd`**，由你拷过去。
用 conda 时需要生成时固化 conda 根目录（systemd 运行时没有 `.bashrc`、`PATH` 里没有 conda，运行时探测不到），
所以请在已 `conda activate` 的 shell 里跑，或显式给 `--conda-root`。

### 必配环境变量（worker 主目录的 `.env`）

| 变量 | 说明 |
|---|---|
| `WEBX_WORKER_WEB_BASE_URL` | web 的地址（回调 `/internal/tasks/...`） |
| `WEBX_WORKER_TASK_SERVER_URL` | 领取服务地址；**留空回落到 `WEB_BASE_URL`**——此时打到的是 web 的端口，上面未必有 `/task/leases` |
| `WEBX_WORKER_STORAGE_URL` | 存储服务地址（上传产物） |
| `WEBX_WORKER_INTERNAL_TOKEN` | 与服务端 `INTERNAL_TOKEN` 逐字一致 |
| `WEBX_WORKER_WORK_DIR` | 任务工作目录。**多个实例必须各自独占**（见 `docs/worker.md` 的 K4） |

### 并发

一个 worker 实例 = 一条 Lane 起步（空 `LANES` 把所有 channel 归入一条）。**并发的扩展轴是进程**：同一台机器多起实例，
各配互不相同的 `WEBX_WORKER_WORK_DIR` 和 channel 配置（比如每块 GPU 一个，`run_env` 里绑设备）。

---

## 四、上线验证

按层从内到外。每一层通过再做下一层，出问题能立刻知道坏在哪一段。

| # | 验证什么 | 命令 | 通过标准 |
|---|---|---|---|
| 1 | 领取服务可达、鉴权、数据库 | `python -m task_server verify --url http://127.0.0.1:9020 --token <INTERNAL_TOKEN>` | 退出码 0，逐项 ✓。不改数据 |
| 2 | 存储服务的对象 / 分片 / 签名全链路 | `python -m webx_storage verify --url http://127.0.0.1:9010 --token <INTERNAL_TOKEN>` | 退出码 0 |
| 3 | worker 的配置、脚本、令牌、三个通道 | `python -m webx_worker doctor --home <worker 主目录>` | 退出码 0（1 = 有失败，2 = 配置不可用） |
| 4 | **整条链路**：提交 → 领取 → 执行 → 回调 → 完成 → 产物可下载 | `python tools/smoke.py --web https://api.example.com --login 用户:密码 --storage https://files.example.com` | 退出码 0，末行 `✅ 全链路通` |

第 4 步会在线上创建一个真实任务（默认类型 `demo`，睡一秒、写两个文件，不碰业务；结束后删除），需要有至少一台跑着 `demo`
channel 的 worker——`webx_worker/examples/` 里就是现成的最小配置。要验证业务类型：`--task-type <类型> --params '<JSON>'`。

自动化版本（开发 / CI，不碰线上）：`pytest tests/test_stack_e2e.py` 用真实 PostgreSQL 拉起 web / task / storage / reconciler / worker
五个独立进程，覆盖正常完成、脚本失败、用户取消、优雅停机回池、worker 被 `kill -9` 后补偿器回收。详见 `docs/verification.md`。

---

## 五、故障对照

| 现象 | 先查 |
|---|---|
| 任务一直 `pending`，没有任何报错 | 领取通道：反代是否转发了 `/task/leases`；worker 的 `TASK_SERVER_URL`；worker 日志里有没有 `404 ← .../task/leases`；`doctor` 第“领取通道”项 |
| worker 领到了任务但一直 `queued`，随后被回收重投 | `ack` 失败：worker 到 web 的回调通道（`INTERNAL_TOKEN` 不一致 = 401，反代没放行 `/internal/`）；`doctor` 第“回调通道”项 |
| 任务反复被回收，最终 `failed`，错误里写着“连续 N 次被回收” | worker 心跳没到达（网络 / 反代超时），或任务本身让 worker 崩溃；看 worker 日志与 `LEASE_REQUEUE_LIMIT` |
| 任务做完了但 `result_path` 为空 | 存储通道：worker 的 `STORAGE_URL` / 令牌；`doctor` 的存储项；worker 工作目录里 `ok.json` 的 `server_confirmed` |
| 所有受权限保护的端点 403 | 没跑 `tools/org.py init-perm` |
| 服务起来了但接口报 `column does not exist` / `no such table` | 没跑 `tools/dbsync.py`，或 `dbsync` 的 `--modules` 与服务的 `WEBX_MODULES` 不一致 |
| 改了 `.env` 不生效 | 单元的 `ExecStart` 是否写了参数；补偿器启动日志那一行打印的才是生效值 |
| 停服务时有任务被判 `failed` | 不应发生：优雅停机会让在跑任务回池。看 worker 的 `TimeoutStopSec` 是否 ≥ 30 |
