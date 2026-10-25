# webx_worker —— 异构算力 worker 框架

`webx_worker` 是一个**与具体计算任务无关**的 worker：它从任务服务**拉取**任务，用租约 + fencing
保证同一时刻只有一个 worker 在跑某个任务，把任务当作子进程执行，周期性续租、感知取消、上报进度，
并把产物上传到存储服务。具体“算什么”完全由你写的任务脚本决定。

- 与任务服务之间**只通过 HTTP** 通信，协议见 [`worker-protocol.md`](worker-protocol.md)（唯一权威）；
- 产物上传走存储服务，见 [`storage-service.md`](storage-service.md)；
- 路径规范见 [`api-conventions.md`](api-conventions.md)。

> **完全独立**：只依赖 Python ≥ 3.11 标准库 + `httpx`，不 import `webx` / `webx_storage` /
> `sanic` / `tortoise` / `environs`（`tests/test_layering.py` 守护）。它要能被单独打包，发给没有服务端
> 框架的机器。配置读取（`.env` 解析）、日志、systemd 单元生成都是包内自带的精简实现。

## 1. 架构总览

```
                    ┌─────────────────────── worker 主进程（asyncio，单进程）───────────────────────┐
 领取服务           │  LeaseDispatcher ──每个空闲 Lane 一个槽位──▶ POST /task/leases（长连接，挂起）  │
 (task_server)      │       │ 领到任务                                                              │
                    │       ▼                                                                       │
 任务服务           │  _run_one: 注册 fencing 令牌 → ack → 上报 running → executor.execute          │
 (/internal/tasks)  │       │                                                                       │
                    │       ├─ HeartbeatManager：心跳续租 / 感知取消 / 租约自熔 / 增量日志推送       │
                    │       ├─ executor：下载输入 → 写 params.json/meta.json → 拉起子进程 → ok.json │
                    │       └─ callback：分层重试的回调客户端（失败的转 PendingReporter）           │
                    │  PendingReporter ◀── IPC server（AF_UNIX）◀── 子进程 SDK（失败事件）         │
                    │  Recovery（启动扫描 ok.json）   cleanup（周期清理子进程）                      │
                    │  UploaderSupervisor ──拉起/看住──▶ uploader 子进程（独立进程）                 │
                    └──────────────────────────────────────────────────────────────────────────────┘
 存储服务  ◀── uploader：磁盘队列 → 分片上传 → POST /deliveries（交付地址，不带 fencing）
           ◀── 子进程 SDK：同步上传 / 取回产物
```

**关键设计**（都是线上验证过的，不要“优化”）：

| 设计 | 要点 |
|---|---|
| **拉取模型** | 分发权威只有任务服务的数据库；worker 自己来领，没有会过期的任务镜像。没空槽就不发领取请求 = 天然背压 |
| **Lane 是槽位，不是队列** | 一条 Lane 同时最多一个任务，跑完才领下一个。并发的扩展轴是**进程**，不是同类型多槽 |
| **能力由 worker 声明** | `CHANNELS` 的键（task_type）经 Lane 映射，随每次领取请求带给服务端；服务端不存能力注册表 |
| **租约 + fencing** | 之后所有写操作用 `(worker_id, lease_seq)` 证明自己仍是租户；令牌必须注入子进程环境 |
| **先写本地标记，再通知服务端** | `ok.json` 记录终态，重启后凭它与服务端协商 |
| **重试分层** | 可丢弃的消息不阻塞计算；必须送达的消息不只试一次；永久拒绝不进“永不放弃”的队列 |

### 目录布局

```
webx_worker/
  __main__.py        子命令分发：run | cleanup | uploader | service | doctor | selftest
  app.py             worker 主程序（启动顺序、SIGTERM 停机三步）
  config.py          配置：环境变量 + .env + 实例覆盖层；worker_id 派生；筛选配置
  envnames.py        父子进程共用的环境变量名（唯一来源，只依赖标准库）
  plugin.py          TaskKindPlugin —— 业务接缝
  lane_manager.py    Lane 划分与 task_type→Lane 映射（能力声明）
  lease.py           领取客户端 + 分发循环 + 停机三步用的函数
  broker.py          领取循环 + 周期清理循环
  executor.py        通用任务执行器（合并后的唯一一份）
  heartbeat.py       心跳 / 取消 Event / 租约自熔 / 增量日志推送
  fencing.py         fencing 令牌注册表
  callback.py        任务服务回调客户端（分层重试）
  pending_reporter.py  失败上报的持久重放队列
  ipc_server.py      接子进程失败事件的 AF_UNIX server
  recovery.py        启动恢复
  cleanup.py         任务目录清理
  uploader.py / uploader_supervisor.py   独立上传进程及其守护
  link_health.py     链路健康记账与启动自检
  log_writer.py / log_config.py          截断日志 + 前端可见脱敏 / 滚动文件日志
  channel_loader.py  channel 配置加载
  http_file_client.py  输入文件下载
  service.py         systemd 单元生成器
  doctor.py / selftest.py   部署自检 / 本机完整生命周期自检
  fake_server.py     协议参考实现（selftest 与测试共用）
  sdk/               子进程侧 SDK：result_client progress ipc_client upload_route upload_queue logsetup
  examples/          最小示例：channels.py + demo_task.py
```

## 2. 快速开始

```bash
pip install httpx                       # 唯一的第三方依赖
python -m webx_worker selftest          # 无需任何外部依赖：本机假服务端跑完整生命周期，15/15 通过即环境就绪

mkdir -p myworker/channel_config && cd myworker
cp <repo>/webx_worker/examples/channels.py   channel_config/default.py
cp <repo>/webx_worker/examples/demo_task.py  channel_config/demo_task.py
cat > .env <<'EOF'
WEBX_WORKER_WEB_BASE_URL=https://tasks.example.com
WEBX_WORKER_INTERNAL_TOKEN=<与服务端 INTERNAL_TOKEN 一致>
WEBX_WORKER_STORAGE_INTERNAL_URL=https://storage.example.com
WEBX_WORKER_WORK_DIR=/data/worker-tasks
EOF
python -m webx_worker doctor            # 对真实服务自检（不会领取任务）
python -m webx_worker                   # 运行（等价于 run）
```

包不需要安装：把仓库根放进 `PYTHONPATH`，或 `cd` 到仓库根执行 `python -m webx_worker`。executor 会把包所在目录
自动放进子进程的 `PYTHONPATH`，所以任务脚本里 `import webx_worker.sdk` 在任意解释器（conda / uv venv）下都可用
（子进程所在环境需装有 `httpx`）。

## 3. 任务生命周期

下表把每一步对应到协议（路径与方法见 `worker-protocol.md`），worker 严格按**新路径**调用：

| # | 动作 | HTTP | 失败处理 |
|---|---|---|---|
| 1 | 领取（长连接） | `POST /task/leases` `{slots, max_level?, max_compute_points?}`，头 `X-Worker-Id` | 退避重连（1s→30s，±30% 抖动）；401/403/404 打 ERROR 等运维 |
| 2 | **登记 fencing 令牌** | 本地 | 必须先于任何上报，否则 ack 不带令牌 |
| 3 | ack（把短租约续成常规租约） | `POST /internal/tasks/<id>/ack` | 失败 → 放弃执行、本 Lane 冷却 5s（任务短租约到期后自动回池） |
| 4 | 上报 running | `PUT /internal/tasks/<id>/status` | 3 次重试仍失败 → 放弃执行并 `DELETE /lease` 归还 |
| 5 | 查询任务状态 | `GET /internal/tasks/<id>` | 404 → 记 `not_found`；`cancelled` → 记 `cancelled`；网络故障不阻塞 |
| 6 | 起心跳（**先于下载**） | `POST /internal/tasks/<id>/heartbeats` | 见 §8 |
| 7 | 下载输入文件 | `GET /internal/tasks/<id>/files/<name>`（名字百分号编码） | 任一失败 → 任务失败 |
| 8 | 写 `params.json` / `files.json` / `meta.json`，拉起子进程 | 本地 | — |
| 9 | 增量日志（第 3 跳起，心跳节拍驱动） | `POST <存储>/internal/tasks/<id>/results` + `PATCH /fields?mode=merge` | 失败不影响心跳与取消 |
| 10 | 子进程内的步骤/字段/产物上报 | `PATCH /steps`、`PATCH /fields`、存储服务 | 见 §10 |
| 11 | 步骤日志上传（**先于 complete**） | 同 9 | 两个静默 return 都有告警 |
| 12 | 写 `ok.json(completed, 未确认)` → 完成 | `POST /internal/tasks/<id>/complete` | 5 次退避重试 → 入持久队列 |
| 13 | 标记 `ok.json` 已确认 | 本地 | — |
| — | 失败 | `POST /internal/tasks/<id>/fail`（带日志尾部摘要） | 同 12；snapshot 不持久化 fail |
| — | 归还租约（停机 / 恢复） | `DELETE /internal/tasks/<id>/lease`（仅请求头带令牌，无请求体） | 被拒是正常结果 |

**令牌必须进子进程环境**：executor 注入 `WEBX_WORKER_ID` / `WEBX_WORKER_LEASE_SEQ` / `WEBX_WORKER_TASK_ID`，
子进程的所有回调都带它。**上报全被 403 的第一嫌疑就是令牌没进环境。**

## 4. Lane 与能力声明

- `CHANNELS` 的键 = `task_type` = 本 worker 的**能力声明**；`LANES` 把 task_type 分组。
- 每个空闲 Lane 在领取请求里占一个 `slots` 元素（该 Lane 能跑的类型列表）；**没有 `n` 参数**——一条 Lane 恒等于一个槽位。
- 回包按 `task_type` 查 Lane（**不能按下标**：服务端只返回认领成功的，重型槽没领到时按下标会把轻型任务派给重型 Lane）。
- **空 `LANES`（缺省）= 所有已声明的 channel 归入一条 Lane**（单 Lane 串行）。
  > 历史缺陷（已修）：旧实现在 `LANES` 为空时 `task_to_lane` 为空，领取循环每 5 秒空转、永远领不到任务，
  > 而默认配置里 `LANES` 恰好是注释掉的。由 `tests/worker/test_lane_manager.py` 守护。
- `LANES` 非空但漏写了某个已声明的 channel：该类型永远领不到任务——启动时打一条 WARNING，**不替你补进去**。
- 需要更高并发：**多起 worker 进程**（`--config gpu0`、`--config gpu1`……），不要“同类型多槽”。

## 5. channel 配置与插件接缝

channel 配置是一个 Python 文件（被 exec 加载），约定下列名字：

```python
CHANNELS = {                       # 必填
    "demo": {
        "script": "demo_task.py",  # 相对 SCRIPT_ROOT（缺省 = 本文件所在目录）；也可写绝对路径；.sh / .js 用 bash / node
        "env": "",                 # "" 直接用 executor；"conda" → conda run -n <env_name>；"uv" → uv run [--python <env_name>]
        "env_name": "", "executor": "python",
        "run_env": {"CUDA_VISIBLE_DEVICES": "0"},   # 注入子进程的环境变量（机器级设备绑定）
    },
}
LANES = []                         # 可选
SCRIPT_ROOT = "/opt/jobs"          # 可选
CONDA_ROOT = "/opt/conda"          # 可选，缺省自动探测
PLUGIN = MyPlugin()                # 可选，TaskKindPlugin 子类实例
```

### TaskKindPlugin：业务接缝

通用机制里凡是“取决于任务类型”的规则，都不写死，而是插件钩子（默认实现全部中性）：

| 钩子 | 作用 | 默认 |
|---|---|---|
| `log_target(task_type, sub_step, steps_done) -> (key, filename)` | 日志的 `log_path` key 与上传文件名。需要“同一步可重复出现”时给 key 加序号，使 log key / 文件名 / 结果 key 严格 1:1 | 单阶段 `("log","run.log")`；多步 `(步骤名, 步骤名+".log")` |
| `select_inputs(task_type, files) -> list` | 挑出需要下载的输入文件条目 | 名字非空的全部 |
| `extend_env(task_type, task_id, env) -> dict` | 追加子进程环境变量。**在 fencing 令牌之前注入——令牌最后写入，插件覆盖不了它** | 无 |
| `soft_clean_policy(task_type) -> frozenset \| None` | 崩溃目录软清理：返回名字集合 = 只保留这些昂贵产物；`None` = rmtree 整目录 | `None` |
| `redactors: list[Callable[[str], str]]` | 前端可见日志的脱敏注册表：整段文本变换，在内置规则**之前**依次执行；抛异常的被跳过 | 空 |

内置脱敏规则（始终生效）只与协议/本机相关：整行剔除内部 API 路径与 SDK 自己的上传/回调日志、遮蔽 URL、
把已知根路径替换成语义标签（`[WORKER]` `[ENV]` `[CONDA]` `[WORK_DIR]` `[TASK_DIR]` `[SCRIPTS]` `[HOME]`）、
把其余绝对路径折叠成末尾两段。**本机日志不脱敏**（排障唯一来源）。

## 6. 配置

### 优先级与主目录

`真实环境变量` > `实例覆盖层（<channel 配置同目录>/<配置名>_env）` > `<主目录>/.env` > 默认值。

- 主目录 = `--home` / 环境变量 `WEBX_WORKER_HOME` / 当前目录。**刻意不锚定包源码目录**（包可能装在 site-packages）。
  `.env` 与 `channel_config/` 都在主目录下；`--config gpu0` ⇒ `channel_config/gpu0.py` 与 `channel_config/gpu0_env`。
- `.env` 语法：`KEY=VALUE`，可带 `export`；`#` 开头为注释；成对引号会被剥掉；未加引号的值里“空白 + `#`”起为行内注释；
  同名键取**最后一行**；不做变量展开、不做 `~` 展开（路径请写绝对路径）。
- ⚠️ 文件层（`.env`/覆盖层）**只对主进程可见**。子进程需要的几项由 executor 显式注入；其它子进程级参数写进 channel 的 `run_env`。
- worker_id = `{WEBX_WORKER_PREFIX 或主机名第一段}-{--config 裸名}`（缺省 `default`）。**必须跨重启稳定**
  （recovery 靠它归还上一轮租约），所以不含 PID/随机数。同机多实例切勿显式设 `WEBX_WORKER_ID`。

### 配置项

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `WEBX_WORKER_HOME` | 当前目录 | 主目录（`.env`、`channel_config/`） |
| `WEBX_WORKER_ID` | 派生 | 显式 worker_id（逃生舱，同机多实例勿设） |
| `WEBX_WORKER_PREFIX` | 主机名第一段 | 机器标识前缀 |
| `WEBX_WORKER_WEB_BASE_URL` | `http://localhost:9009` | 任务服务（`/internal/tasks/...`）基础 URL |
| `WEBX_WORKER_TASK_SERVER_URL` | 空 = 回落 `WEB_BASE_URL` | 领取服务地址（同一反代下挂路径前缀时留空） |
| `WEBX_WORKER_INTERNAL_TOKEN` | 空 | `X-Internal-Token`；空 = 服务端拒绝一切请求 |
| `WEBX_WORKER_LEASE_HOLD_SECONDS` | 25 | 预期的服务端挂起时长；只用来推导本地超时（+15s），须与服务端一致 |
| `WEBX_WORKER_LEASE_MAX_LEVEL` | 空 = 不限 | 只领档位 ≤ 此值的任务（**上限**语义；`0` 有意义，故用空串区分“没配”） |
| `WEBX_WORKER_LEASE_MAX_COMPUTE_POINTS` | 空 = 不限 | 只领算力点 ≤ 此值的任务 |
| `WEBX_WORKER_WORK_DIR` | `~/webx-worker-tasks` | 任务工作目录根（**每个实例必须不同**） |
| `WEBX_WORKER_STORAGE_URL` | 空 | 存储服务【对外】URL，恒用于落库的 `download_url` |
| `WEBX_WORKER_STORAGE_INTERNAL_URL` | 空 = 回落对外 URL | 存储服务【内网】URL，worker 实际请求走它 |
| `WEBX_WORKER_STORAGE_INTERNAL_TOKEN` | 空 = 回落主令牌 | 存储服务专属令牌 |
| `WEBX_WORKER_UPLOADER_ENABLED` | true | uploader 总开关（出问题先关它） |
| `WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB` | 15 | `deliver_only` 产物走 uploader 的体积门槛 |
| `WEBX_WORKER_UPLOAD_REQUIRED_GLOBS` | 空 | 逗号分隔的 fnmatch 模式，命中的远端文件名恒同步上传（`deliver()` 用） |
| `WEBX_WORKER_UPLOADER_CONCURRENCY` | 3 | uploader 内并发上传数（别调大：带宽守恒） |
| `WEBX_WORKER_UPLOADER_SCAN_INTERVAL` | 3.0 | 扫描 pending/ 的间隔（秒） |
| `WEBX_WORKER_UPLOADER_PENDING_LIMIT_GB` | 100 | 队列积压水位（见已知限制 K9：背压函数尚未接线） |
| `WEBX_WORKER_LOG_HEAD_LINES` / `WEBX_WORKER_LOG_TAIL_LINES` | 100 / 2000 | 上传日志截断：头 N + 尾 M；总行数 ≤ N+M 时全量 |
| `WEBX_WORKER_LOG_DIR` | 空 = 仅控制台 | 后台日志目录（`worker_task_{时}_{pid}.log`，单文件 ≤10MB 或 ≤20000 行滚动） |
| `WEBX_WORKER_LOG_MAX_BYTES` / `WEBX_WORKER_LOG_MAX_LINES` | 10MB / 20000 | 日志滚动上限 |
| `WEBX_WORKER_HEARTBEAT_INTERVAL` | 30 | **已废弃**，不生效（节拍由内置序列驱动） |

命令行：`--home` `--config` `--ext-env` `--work-dir` `--debug` `--log`。未知参数被忽略并告警（便于包装脚本透传）。

## 7. 本地完成标记 `ok.json`

位置 `<WORK_DIR>/<task_id>/ok.json`。**先写后通知**：先写 `server_confirmed=false`，回调成功后再标记 `true`。

```json
{"task_id": "...", "status": "completed|failed|cancelled|not_found", "completed_at": "ISO8601",
 "server_confirmed": false, "server_confirmed_at": "…", "steps": [], "sub_step": "", "end_step": "",
 "result_path": "<本地产物目录，仅诊断>", "log_path": "<本地截断日志，仅诊断>",
 "error_message": "(仅 failed)", "not_found_at": "(仅 not_found)"}
```

| 谁写 | 何时 | 内容 |
|---|---|---|
| executor | 接收新任务时 | **先删旧 ok.json**（避免上一轮标记误导 recovery/清理） |
| executor | 子进程成功 | `completed, confirmed=false` → `complete` 成功后 `true` |
| executor | 子进程失败 | `failed, confirmed=false`（含 error_message）→ `fail` 成功后 `true` |
| executor | 心跳取消（`returncode == -1`） | `cancelled, confirmed=true`（那一轮确实被服务端确认取消）；404 则 `not_found` |
| executor | **进程退出导致的 `CancelledError`** | **不写**（被打断的一轮什么都没确认；写了 recovery 会整个跳过该目录） |
| cleanup | 只清 `confirmed=true` 且超龄的 `failed`(2d)/`completed`(7d)，并跳过仍被上传队列引用的目录 | — |

## 8. 恢复 / 自熔 / 停机语义

### 取消与自熔（心跳）

- 心跳节拍 `5,7,11,13,17,19,23,29` 秒后封顶 29s；单次超时 10s（不能取全局 30s）。
- **取消判据**：心跳响应 `status != "running"`（含撤销的 `revoked`——协议规定撤销回 200 + 非 running）→ 触发取消 Event →
  **先同步 `killpg`，再 await 收尾**。起跑前的 `queued` 不是取消信号（心跳从下载之前就开始，RUNNING 上报可能晚到）。
- **自熔**：用本地**单调时钟**判断租约是否临期（`lease_until` 由服务端每跳告知，不能本地推算——grace 随已投入时长变化），
  截止 = 上次成功续租 + 剩余时长 − 10s 余量；续租持续失败到期即自行终止子进程，浪费上界 = 租约时长。
  局限见 K1、K2。
- 子进程收尾：`start_new_session=True`（等价 `setsid`）；`finally` 里**先同步 `killpg(SIGKILL)` 再 await**
  （worker-cancel-task-fix 方案 A：二次取消打在 await 上也不会漏杀子进程组，含孙进程）。

### 启动恢复（recovery）

扫描工作目录下**非点号开头**的子目录（`.uploads/` 等基础设施目录不是任务）：

| 本地状态 | 动作 |
|---|---|
| `confirmed=true` | 跳过 |
| `not_found` | 超 7 天清理，否则跳过 |
| `completed`/未确认 | 重试 `complete`；服务端已是 completed 视为已确认；404 → `not_found`（局限见 K2） |
| `cancelled`/未确认 | 标记已确认 |
| `failed`/未确认 | **不重试**（避免过期失败信号覆盖补偿器重投后的工作），交服务端补偿器 |
| 无 ok.json，服务端 404 | 写 `not_found` 标记，保留目录 |
| 无 ok.json，网络故障 | 保留目录，下次再查 |
| 无 ok.json，服务端 `completed` | 保留目录（避免数据丢失） |
| 无 ok.json，服务端 `queued/running` 且租户是本机 | **带令牌主动归还租约**（任务立刻回池），再按插件策略清理 |
| 无 ok.json，其它终态/崩溃 | 按 `plugin.soft_clean_policy(task_type)`：名字集合 → 软清理；`None` → rmtree |

### 失败上报的持久重放（PendingReporter）

前台 5 次退避重试仍失败的状态迁移（status/complete/fail）与子进程经 IPC 转来的事件进入队列：
新事件立刻试一次，其余每 20s 一轮，**永不退出、永不放弃**；退出时 snapshot 到
`<WORK_DIR>/pending_reports.snapshot.jsonl`（`fail_task` 不持久化），启动时恢复。
令牌**随条目快照落盘**（走 JSON 体第二通道），因为重放恒发生在租约注销之后。
**永久拒绝的判据是响应体 `reason`，不是状态码**：`not_lessee`/`gone` 丢弃，`already_final` 视为成功，
`400`/`404` 业务拒绝丢弃；`401` 与不带终态 reason 的 `403` 保留（令牌填错不能清空整个队列）。
`fail_task` 重试前先 `GET` 任务状态，服务端已非 `running` 则丢弃。

### 优雅停机（SIGTERM / SIGINT）

信号只置 Event（CPython 对 SIGTERM 不装 Python 级 handler，不装则 `finally` 一行都跑不到）；真正的收尾在协程里，
**不等在跑的任务**（一个计算步可能跑十几小时）：

1. **同步捕获**在手租约的令牌（②会清空注册表，不先捕获则③归还空手被拒）；
2. **同步取消**在跑任务（`Task.cancel()` 在任何 await 之前生效，失败链拿到 `CancelledError` 走中断分支——
   不写 ok.json、**绝不上报 fail**。若只做③，归还要 await，await 期间失败链照样推进，先后退回概率问题）；
3. 用捕获的令牌**并发**归还租约（单次超时 10s，串行会随 Lane 数吃掉 `TimeoutStopSec`）。

然后停 uploader（宽限 10s，传不完的下次接着传）、停心跳、落 PendingReporter 快照、关 IPC。
**第二次信号 = 强退**（`os._exit(1)`），收尾卡住时仍可 Ctrl-C。

## 9. uploader（独立进程）

- 由 worker 父进程拉起并看住（崩溃按 1→30s 退避重启），生命周期与 worker 一致；配置按**真环境变量**注入子进程。
- **磁盘目录即消息**：子进程把条目写进 `<WORK_DIR>/.uploads/pending/{task_id}__{key}.json`（只记路径、不复制文件），
  写文件成功就是送达；uploader 扫描目录，`pending → inflight`（原子 rename）→ 上传 → 交付 → 删除。条目文件名由
  `(task_id, key_path)` 派生，重复投递天然幂等。
- **上传前必先复查任务状态**（`GET /internal/tasks/<id>`）：404 / `failed` / `cancelled` → 丢弃条目；问不到或鉴权错（401/403）
  → 保留（修好即自愈，不能因一次令牌填错清空队列）；每传 8 个分片再复查一次，取消即放弃会话。
- 传输：≤ 8MB 整传 `POST /internal/tasks/<id>/results`（201）；更大走 `POST .../results/uploads` → `PATCH /internal/uploads/<id>`
  （`Upload-Offset`）→ `POST /internal/uploads/<id>/complete`；崩溃重启凭 `session_id` 取 `Upload-Offset` 响应头接着传，
  `409` 以响应头里的真实 offset 重新对齐。
- 交付：`POST /internal/tasks/<id>/deliveries`，`{"result_path": {<嵌套 key>: <地址>}}`，**不带 fencing**（它手里永远没有 per-task 令牌）。
  `404` 且响应体带 `error` → 任务没了，丢弃；`404` 但没有 `error` 键（路由缺失）→ 保留。
- 两阶段：上传成功后原地记 `uploaded_url`，交付失败只重做交付，绝不重传几个 GB。
- 重试按 attempts 指数退避（5s→300s 封顶），永不放弃；每 10 次连续失败升一级 ERROR。
- **没有存储地址就不拉起**（协议规定产物只传存储服务，无回退路径），子进程 SDK 的 `deliver_*` 自动退回同步上传。

## 10. 子进程 SDK 与环境变量

任务脚本以**任务目录为 cwd**、追加 `--task-id <ID>` 启动：

```
<WORK_DIR>/<task_id>/   params.json  files.json  meta.json  in/  out/  run.log  ok.json  run.truncated.log
```

| 环境变量 | 来源 | 用途 |
|---|---|---|
| `WEBX_WORKER_ID` / `WEBX_WORKER_LEASE_SEQ` | executor（fencing） | 子进程所有回调的令牌；缺了 = 全 403 |
| `WEBX_WORKER_TASK_ID` | executor | 当前任务 ID（便利项） |
| `WEBX_WORKER_INTERNAL_TOKEN` / `WEBX_WORKER_WEB_BASE_URL` | executor | 调任务服务 |
| `WEBX_WORKER_STORAGE_URL` / `_STORAGE_INTERNAL_URL` / `_STORAGE_INTERNAL_TOKEN` | executor | 存储服务（专属令牌仅在配置了时注入） |
| `WEBX_WORKER_WORK_DIR` | executor | 工作目录根（IPC 降级文件、上传队列） |
| `WEBX_WORKER_IPC_SOCK` | executor | 失败事件交回父进程的 AF_UNIX socket |
| `WEBX_WORKER_UPLOADER_ENABLED` / `_UPLOAD_ASYNC_THRESHOLD_MB` / `_UPLOAD_REQUIRED_GLOBS` | executor | 产物分流 |
| `WEBX_WORKER_TASK_LOG_LEVEL` | 你（走 `run_env`） | 子进程日志级别 |
| `PYTHONPATH` | executor | 前置本包所在目录，使 `import webx_worker.sdk` 可用 |

`webx_worker.sdk`：

- `result_client`：`upload_result` / `update_task_fields` / `update_step_status` / `download_artifact(_stream)` / `list_artifacts`。
  重试分层：状态迁移 5 次退避后转 IPC 兜底；**进度消息**短超时 2 次即弃（过期进度补发是倒退）；**结果上传** 5 次/累计 30s 退避，
  超时四项分开（`connect` 不继承大文件长超时；`write` 是空闲超时不是总时长）。
- `progress.report_step_message(task_id, sub_step, message)`：最佳努力，永不抛出。`sub_step` 空串合法（单阶段任务）。
- `upload_route`：`deliver_required`（下游依赖，恒同步）/ `deliver_only`（纯交付，按阈值分流）/ `deliver`（按
  `WEBX_WORKER_UPLOAD_REQUIRED_GLOBS` 规则）。框架**不内置任何扩展名判据**。进队列与彻底失败对调用方都返回 `""`。
- `ipc_client`：`send_report` fire-and-forget，IPC 不通则降级写 `<task_dir>/pending_reports.jsonl`；令牌随条目快照。
- `logsetup.configure_task_logging()`：子进程日志配置（默认 INFO，把 httpx 等第三方压到 WARNING）。

最小示例见 `webx_worker/examples/`。

## 11. systemd 部署

```bash
python -m webx_worker service one   --home /srv/worker --unit-name my-worker            # my-worker.service
python -m webx_worker service multi --home /srv/worker --unit-name my-worker --instances gpu0,gpu1
# 需要 conda：--conda-env myenv [--conda-root /opt/conda] [--env /opt/dev.env]（会生成启动脚本，conda 根在生成时烤入）
sudo cp /srv/worker/deploy/my-worker@.service /srv/worker/deploy/my-worker.target /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now my-worker@gpu0
```

- 一 worker 一 unit；多实例用模板单元，**一名三用**：`%i` == `--config` 名 == 覆盖层名 == worker_id 后缀。
- unit 里 `WORKER_ARGS` **只有 `--config %i`**：业务配置一律进 `.env` / `<名>_env`，手动与托管行为等价。
- `TimeoutStopSec=30`（SIGTERM 后 worker 要做停机三步），`PrivateTmp=true`（日志目录别写 `/tmp/...`）。
- 多个实例**必须**各自独占 `WEBX_WORKER_WORK_DIR`（见 K4）；生成器为每个实例写出带互不相同工作目录的覆盖层样例。
- 改单元基础名不是迁移：旧单元若已 enable，需先 `disable --now`；生成器会检测同目录下其它基础名的残留并打印拆除指引。

## 12. 验证过程

```bash
# 1) 单元/集成/e2e（假服务端 + 真子进程 + 真信号），repo 根执行：
python -m pytest -q -p no:cacheprovider -W ignore tests/worker tests/test_layering.py
python -m pyflakes webx_worker tests/worker
# 2) 本机冒烟（无外部依赖）：
python -m webx_worker selftest
# 3) 部署验证（对真实服务；不会领取任务）：
python -m webx_worker doctor --home /srv/worker --config gpu0
```

`fake_server.FakeTaskServer` 是协议的参考实现，刻意做得“够严”：写端点强制 fencing、旧路径 404 / 错方法 405、
撤销回 `200 revoked`、存储创建回 201、分片 offset 不符回 409 + `Upload-Offset`。所以**违反协议的 worker 一定会在测试里红**。

两处关键防线做过变异验证（改坏 → 对应用例变红 → 还原）：

| 变异 | 变红的用例 |
|---|---|
| 去掉 executor 的 fencing 令牌注入 | `test_e2e_lifecycle::test_happy_path_full_lifecycle`、`test_fencing_token_paths::*` |
| 去掉空 `LANES` 修复 | `test_lane_manager::test_empty_lanes_*`、`test_dispatcher_with_empty_lanes_actually_sends_a_lease_request` |
| PendingReporter 不再按响应体 reason 丢弃 | `test_pending_reporter::test_fencing_terminal_reasons_are_dropped[*]` 等 5 条 |

`doctor` 检查项：channel 配置可加载与 Lane/能力声明、脚本与解释器、工作目录可写、令牌已配置、
**领取通道**（`POST /task/leases` 发空 `slots`：`400` = 可达且鉴权通过，`401` = 令牌错，`404` = 路由没转发；
空 slots 不可能领到任务）、**回调通道**（`GET /internal/tasks/<哨兵>`：`404`/`200` = 通，`401` = 令牌错）、
存储服务（`GET /internal/uploads/<哨兵>`：`404` = 通，`401` = 令牌错）。退出码 0/1/2。

## 13. 协议缺口的裁定（相对 `worker-protocol.md`）

迁移时 worker 对协议里没写清的地方采取了保守兼容。服务端（`webx/controller/internal/`）落地后逐条核对，
结论都已写进 `worker-protocol.md`；这里只记录裁定，以及 worker 是否因此改了行为：

| # | 缺口 | 裁定 | worker 侧 |
|---|---|---|---|
| G1 | `GET /internal/tasks/<id>` 响应形状 | **扁平对象**，无 `data` 外壳（协议 §4.10） | 两种都认，不用改 |
| G2 | `fields` 的 `result_path`/`log_path` 是字符串还是对象 | **两种都收**，`deliveries` 同（§4.8） | 不用改 |
| G3 | 缺令牌时的 `reason` | 写端点统一 `not_lessee`；`bad_lease_seq` / `missing_fencing_token` 是客户端 bug，**重放中不是终态**（§2） | 保持“保留不丢”，不用改 |
| G4 | 读接口与 fencing 的关系 | **读接口不要求 fencing**（§2） | 不用改 |
| G5 | `lease_until` 绝对时刻引入时钟偏移 | ack / 心跳响应**新增 `lease_remaining_ms`**（服务端同一事务里算）（§4.1/4.2） | **已改**：优先用它，缺失才回退旧算法（K2 就此解决） |
| G6 | 心跳被撤销怎么处理 | 撤销回 **200 + `status=revoked`**，任务不存在回 404（§4.2） | 不用改 |
| G7 | 无副作用的回调探针 | `GET /internal/tasks/<任意 id>` 即探针，id 无形态约束（§4.10） | 不用改 |
| G8 | `slots: []` 是否一定 400 | **一定**（§3.1），`task_server.verify` 也依赖它 | 不用改 |
| G9 | 存储接口成功状态码 | 创建 201、PATCH 200、complete 200（`storage-service.md`） | 不用改 |
| G10 | `deliveries` 成功响应 | `200 {"task_id", "result_path"}`（§4.9） | 不用改 |

## 14. 已知限制

**审计教训中与通用机制有关、且本次迁移没有修的**（来自原 `audit-2026-08-27.md`，如实保留，未顺手改）：

| # | 内容 |
|---|---|
| K1 | **自熔余量（10s）小于心跳检查粒度**：稳定期节拍 29s + 单次超时 10s ⇒ 续租持续失败时检查间隔 39s，最坏在服务端回收后约 29s 才停下，“严格早于”在长任务上不成立。修法需**同时**做两步：sleep 按 deadline 截断、醒来后先判自熔再发心跳。暂不修的理由：被回收的任务重新开跑要经认领→ack→下载输入→拉起子进程，那 ≤29s 通常不会变成两份同时算；若将来引入共享工作目录须重新评估 |
| K2 | ✅ **已解决**：自熔剩余时长优先取服务端给的 `lease_remaining_ms`（G5），不再把跨机时钟偏移带进判据。仅在对接旧服务端（只给绝对时刻 `lease_until`）时才回退到「绝对时刻 − 本机墙钟」，此时偏移仍会进入判据 |
| K3 | **recovery 期间收到 SIGTERM 不会与停机竞态**：停止信号的监听在 `recovery.recover()` 之后；恢复扫描是逐目录串行 `GET`（单次超时 30s），服务端不可达时最慢，可能超出 `TimeoutStopSec` 被 SIGKILL，snapshot 随之丢失（改造前同样硬杀，不是回归） |
| K4 | **recovery 的 rmtree 分支没有归属校验**，且共享工作目录有三处冲突：`ipc.sock` 被后启动者抢占、失败上报 snapshot 互相覆盖、recovery 误删兄弟实例正在执行的任务目录。今天安全仅因每个实例独占工作目录（`derive_worker_id` 取 `--config` 裸名保证 ID 不同）。**多实例务必配互不相同的 `WEBX_WORKER_WORK_DIR`**，且别指向它们的共同父目录 |

**迁移与核对中新发现的**（同样未修，记在这里）：

| # | 内容 |
|---|---|
| K5 | ✅ **已解决**（原：停机时先归还、后停领取循环，空闲 Lane 会把刚归还的任务又领回来，停在 queued 等 60s 短租约；`test_sigterm_end_to_end` 约三分之一概率失败即由此而来）。现在停机第一步 `begin_drain()` **同步**停领，长轮询在途期间收到的回包带令牌原样归还、不执行。残余：领取响应本身丢在路上（服务端已认领、worker 没收到）仍只能靠短租约到期回池，这是协议固有的 |
| K6 | ✅ **已解决**：`ok.json` 现在记 `worker_id` / `lease_seq`，recovery 重试 `complete` 前用它临时登记令牌（用完注销）。只认本实例自己的记录；令牌代次已过期（任务被回收重投过）则服务端拒绝，保持未确认，不会覆盖新一轮。升级前遗留的无令牌 `ok.json` 行为不变 |
| K7 | `PendingReporter.restore_from_snapshot` 在快照文件不存在时直接 `return 0`，**走不到扫描 per-task 降级文件**（子进程 IPC 不通时写的 `pending_reports.jsonl`）；这些事件只有在恰好存在快照时才会在启动恢复里被重放 |
| K8 | `PendingReporter._do_post` 在条目无 fencing 时会就地 `pop("_mode")`，使第二次重放丢失 merge 模式（按 overwrite 写）。有令牌的条目走拷贝路径不受影响；无令牌条目按协议本就会被永久拒绝，实际影响面很小。另 `_safe_to_fail` 的 docstring 写“读不到状态则 retry”，实现是 drop |
| K9 | **uploader 背压未接线**：`UploaderSupervisor.is_backpressured()` / `UPLOADER_PENDING_LIMIT_GB` 在原实现里就没有调用点（领取循环不查积压）。积压撑盘是 uploader 唯一会拖垮机器的失败模式，接入需在 `LeaseDispatcher.loop` 的发请求前判断 |
| K10 | 子进程 SDK 对 `403` 按 5xx 路径试满 5 次再转 IPC，而父进程 `callback` 对 `401/403` 只试 1 次（不一致，原实现遗留） |
| K11 | uploader 中途发现任务已取消时，先放弃会话并把条目退回队列（按退避延迟），下一轮才在“上传前复查”里丢弃，而不是立即丢弃 |
| K12 | 输入文件名未做路径穿越校验（`in/ + name`），依赖服务端保证文件名合法 |
| K13 | 仅支持 POSIX（`setsid`/`killpg`/AF_UNIX/信号）；Windows 未适配 |
| K14 | 非空 `LANES` 漏写的 channel 只打 WARNING，不会被领取（刻意不顺手补） |
| K15 | `WEBX_WORKER_HEARTBEAT_INTERVAL` 为已废弃的占位配置，不生效 |

**独立审计（2026-10）新记录、未修的**：

| # | 内容 |
|---|---|
| K16 | **磁盘无上限增长**：`cleanup` 默认策略不含 `cancelled`（被吊销 / 自熔 / 被抢走租约后写的 ok.json），`failed, server_confirmed=false` 的目录被规则 4 跳过且 recovery 不重试 failed；`complete` 入队后由 PendingReporter 重试成功时没有地方把 ok.json 标记为已确认（只有重启 recovery 会修正）；日志只滚动不清理 |
| K17 | uploader 队列同名条目（`{task}__{slug}.json`）被新条目覆盖后，旧条目上传完成时的 `drop()` 会把新条目一起删掉（用户 redo 同一步时，新产物永远不会交付） |
| K18 | 分片上传遇 `409` 且响应缺 `Upload-Offset` 头（反代吞头）时按 offset=0 无限重发 8MB 分片，没有计数与退避 |
| K19 | `--log DIR` 只存在局部变量，没回写 `worker_settings.LOG_DIR`，uploader 子进程的日志被丢弃 |
| K20 | uploader 崩溃重启的退避只增不减（注释说“活过一段时间会重置”，代码没有） |
| K21 | `cleanup` 可能在新一轮起点（旧 ok.json 要到 ack 与上报 running 之后才删）与新一轮并发，窗口内会 rmtree 多步任务上一步的中间产物 |
| K22 | 空领取（HTTP 200 空列表）立即重连没有最小间隔；服务端 HOLD 配成 0 或代理返回静态 200 JSON 时是紧循环 |
| K23 | `executor.py` 里 `work_base_dir / task_id`、`run_{sub_step}.log` 未做路径名归一化。服务端只有内部 `PATCH /steps`（DTO 限 30 字符）能写 `sub_step`，要先拿下任务脚本才能利用，属纵深防御缺口，与 K12 同类 |

## 15. 与旧实现的对照（迁移说明）

- **合并**：`executor.py` + `executor_o.py` → 唯一一份 `executor.py`（`asyncio.wait(timeout=)` 在 3.10/3.11+ 通用）。
  `preexec_fn=os.setsid` 换成 `start_new_session=True`（`preexec_fn` 在多线程进程里不安全）。
- **环境变量**：全部加 `WEBX_WORKER_` 前缀（含原先无前缀的 token 与日志项），不做旧名兼容。
- **配置**：不再依赖第三方配置库；`.env`/`channel_config/` 锚定到主目录而非包目录；脚本路径相对 `SCRIPT_ROOT`。
- **存储**：产物与步骤日志只走存储服务的规范路径，**没有回退到任务服务**；`download_url` 恒用对外地址归一化。
- **不迁移（属业务或已过时）**：具体计算任务脚本与其辅助库、业务 channel 模板内容、业务测试、
  GROMACS 输出噪音剔除（改成 `redactors` 接缝由业务自行注册）、力场缓存日志标记、`mds` 软清理白名单（改成 `soft_clean_policy`）、
  `executor_o.py`、ZMQ/MinIO 相关残留、基准/检查脚本。
- **新增**：`TaskKindPlugin` 接缝、`fake_server`、`doctor`、`selftest`、统一的 `service` 生成器、示例。
