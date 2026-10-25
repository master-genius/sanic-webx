# Worker 协议（任务服务 ↔ worker）

任务服务（`webx` 核心里的 task 模块 + `task_server` 领取服务）与 worker（`webx_worker`）之间
**只通过 HTTP 通信**，没有任何共享代码。这份文档是双方唯一的依据：服务端按它实现，worker 按它调用，
端到端测试用它防止两边漂移。

规范见 `docs/api-conventions.md`。本协议里的路径已按规范设计：路径只放名词，动作由 HTTP 方法表达。

## 1. 模型

- **拉取模型**：任务的分发权威只有一个——数据库里的任务表。Web 不投递，只把任务置为 `pending` 并
  「戳」一下领取服务；worker 自己来领。进程内没有会过期的任务镜像。
- **长连接**：worker 的领取请求在服务端**挂起**，有任务立刻返回，挂满 `HOLD_SECONDS` 返回空列表。
  空返回也是 `200`——挂起期间没有认领任何东西，超时或断开**零损失**，所以不需要保活长寿连接。
- **互斥**只来自一条带条件的 `UPDATE`，与服务端进程数无关。
- **能力由 worker 声明**：每次领取请求带上"我这个空槽能跑哪些 `task_type`"，服务端不存能力注册表
  （存了就是过期镜像）。
- **租约 + fencing**：认领即租用；之后所有写操作用 `(worker_id, lease_seq)` 证明自己仍是租户。

## 2. 鉴权与 fencing

所有请求带 `X-Internal-Token: <令牌>`（服务端配置 `INTERNAL_TOKEN`；**未配置则拒绝一切请求**）。

**fencing 令牌** = `(worker_id, lease_seq)`，随每个写请求发送：

| 通道 | 用途 |
|---|---|
| 请求头 `X-Worker-Id` / `X-Lease-Seq` | 首选，前台路径 |
| JSON 请求体的 `worker_id` / `lease_seq` | 第二通道：令牌来自**持久化条目快照**的异步路径（上传进程、失败上报重放） |
| 表单字段同名 | multipart 请求 |

两者都带且不一致时**以请求头为准**（要有确定的答案）。解析永不抛异常：取不到就是"无令牌"。

**写操作一律强制 fencing**（没有"无令牌放行"的兼容分支）。被拒绝时：

| 情形 | 响应 |
|---|---|
| 任务不存在 / 已软删 | `403 {"ok": false, "reason": "gone"}` |
| 令牌不是当前租户（过期租户） | `403 {"ok": false, "reason": "not_lessee"}` |
| 同一租户但任务已在终态 | `403 {"ok": false, "reason": "already_final"}` —— **这是幂等成功**，调用方视为成功后丢弃，不要告警 |

为什么是 `403`：它本来就是"业务 / 安全拒绝"，客户端不应重试，也不应触发全局鉴权告警（只有 `401` 才是鉴权故障）。
worker 的**持久重试队列**对"永不放弃"与"永久拒绝"的区分依据是响应体里的 `reason`，不是状态码。

**例外：心跳**被撤销时回 `200` 而不是 `403`，见 §4.2。

## 3. 领取服务（`task_server`，默认前缀 `/task`）

### 3.1 `POST /task/leases` —— 领取（长连接）

请求头：`X-Internal-Token`、`X-Worker-Id`（必填，也可放 body 的 `worker_id`）

```json
{
  "slots": [["type_a"], ["type_b", "type_c"]],
  "max_level": 0,
  "max_compute_points": 100
}
```

- `slots`：每个元素是**一个空闲 Lane** 能跑的类型列表。**没有 `n` 参数**——一条 Lane 恒等于一个槽位。
  最多 16 个槽、每槽最多 32 个类型；元素非法 → `400 {"error": "invalid slots"}`。
- `max_level` / `max_compute_points`：可选，**上限**语义，是整台 worker 的属性（这台机器服务谁），
  不是每个槽各自的。只缩小候选集，不改变互斥语义。`< 0` 或非整数 → `400`。
- 缺 `worker_id` → `400 {"error": "worker_id required"}`。

流程：先取唤醒事件的当前代次，**再**查库认领（顺序不可颠倒，反过来会丢唤醒）；没有任务就挂起到被戳醒或挂满
`HOLD_SECONDS`。数据库抖动 → `503 {"error": "claim failed"}`，worker 按退避重连，不要紧密重试。

响应 `200`：

```json
{"tasks": [ <Task>, ... ], "count": 1}
```

`<Task>`（**刻意不用 `RETURNING *`**，列清单固定；不含 `user_id` / `org_id` / `error_message` 等内部列）：

| 字段 | 说明 |
|---|---|
| `id` | 任务 ID |
| `name` | 任务名 |
| `task_type` | 路由键，也是 worker 的能力声明所对应的类型 |
| `files_json` | 输入文件清单（JSON 字符串） |
| `params_text` | 参数（JSON 字符串） |
| `steps` / `sub_step` / `end_step` | 多步任务：已完成步骤、当前步骤、最终步骤 |
| `timeout_seconds` | 子进程超时（秒），**本步上限**，宁可宽松 |
| `allowed_retries` | 允许重试次数 |
| `level` / `compute_points` | 公平排序键（快照） |
| `lease_seq` | **fencing 凭据的后半** |
| `worker_id` | 归属，**fencing 凭据的前半** |
| `status` | 认领后恒为 `queued` |

认领排序：`level DESC → compute_points DESC → rand_rank ASC → submitted_at ASC`。同档位内**不是 FIFO**
（刻意打散）。认领时：`status='queued'`、`lease_seq += 1`、写入短租约（默认 60 秒）。

### 3.2 `POST /task/pokes` —— 戳醒（仅本机）

空 body，**不带任务内容**。语义只是"有事了，去看看"。**不鉴权**，所以默认只监听 `127.0.0.1`；
需要对 worker 暴露 `/task/leases` 时在反代上挂路径前缀，不要改监听地址。
规则：**任何使任务进入 `pending` 的路径都要戳**（提交、租约回收、子任务准入、主动归还租约……）。
失败只损失延迟，不损失正确性（兜底轮询也会发现）。

### 3.3 `GET /health`

`{"ok": true, "service": "task_server", "pokes": <计数>}`

## 4. 任务回调（前缀 `/internal/tasks/<id>`，在 Web 服务里）

除特别说明外都需要 fencing。

### 4.1 `POST /internal/tasks/<id>/ack` —— 认领后的即时确认

领到任务后**立即**调用：把短租约续成常规租约。兜住"领取响应丢在路上"的情形。

- 成功 `200 {"ok": true, "lease_until": <毫秒时间戳>}`
- 已不是租户 `403 {"ok": false, "reason": "not_lessee"}` —— worker 应放弃本次执行
- 带了 `worker_id` 却没带 / 带坏 `lease_seq` → `403 {"ok": false, "reason": "bad_lease_seq"}`（响亮拒绝，不当作无令牌）

### 4.2 `POST /internal/tasks/<id>/heartbeats` —— 心跳（续租）

续租：`lease_until = now + 常规租约 + grace`，`grace = α × 已投入时长`（下限 180 秒，上限 5 小时），
**由服务端计算**，所有租约时间运算在 SQL 里用数据库的 `now()`，不用应用侧时钟。

- 成功 `200 {"ok": true, "status": "<任务状态>", "lease_until": <毫秒时间戳>}`
- **被撤销 `200 {"ok": false, "status": "revoked", "reason": "not_lessee"}`**（不是 403）

为什么撤销也回 `200`：worker 的取消判据是 `status != "running"`。只要回一个非 `running` 的 status，
被撤销租约的 worker 会在下一跳自行终止子进程，**无需额外分支**。`lease_until` 是**承重项**：
grace 随已投入时长变化，租约时长每跳都不同，worker 无法自行推算，必须由服务端每跳告知——
否则自熔时刻会漂到服务端回收之后（出现"服务端已租给 B、A 仍认为自己持有"的窗口）。

### 4.3 `PUT /internal/tasks/<id>/status` —— 状态上报

`{"status": "running", "error_message": ""}` → `200 {"message": ...}`；非法状态 `400`；非法迁移 `400`。

### 4.4 `POST /internal/tasks/<id>/complete` —— 完成

只触发状态迁移。多步任务停在 `running + success`（等用户推进下一步）。结果地址不在这里写。

### 4.5 `POST /internal/tasks/<id>/fail` —— 失败

`{"error_message": "..."}`。

### 4.6 `DELETE /internal/tasks/<id>/lease` —— 主动归还租约

worker 优雅退出 / 重启恢复时调用，任务立即回 `pending` 池（并戳醒领取服务）。**必须带 fencing**：
一台已经丢了租约、正在关闭的 worker 若无条件归还，会退掉别人的租约。
缺令牌 `403 {"ok": false, "reason": "missing_fencing_token"}`；已不是租户 `403 not_lessee`（什么都不做才是对的）。

### 4.7 `PATCH /internal/tasks/<id>/steps` —— 步骤进度

`{"steps": [...], "sub_step": "...", "sub_status": "...", "step_message": "..."}` →
`200 {"ok": true, "steps": [...], "sub_step": "...", "sub_status": "..."}`。

### 4.8 `PATCH /internal/tasks/<id>/fields?mode=overwrite|merge` —— 结果 / 日志字段

`{"result_path": ..., "log_path": ...}`。`overwrite`（默认）替换整个字段；`merge` 按 key 深度合并
（步骤 key 不存在则添加，存在则 dict-update 子字段）。不改变任务状态。非法 `mode` → `400`。
这条路严格 fencing——`log_path` 被上一轮覆盖正是要防的事。

### 4.9 `POST /internal/tasks/<id>/deliveries` —— 产物交付（**刻意不做 fencing**）

`{"result_path": {...}}`，按嵌套 key 幂等 `merge`。给**长驻的独立上传进程**用：它一辈子服务无数个任务，
手里永远没有 per-task 的租约令牌。多步推进后 `lease_seq` 早已 +1、甚至换了 worker，产物仍要交付，
所以对它做租户比对除"任务不存在"之外每一种拒绝都是误判。条目写的是带步骤序号的叶子 key，不碰当前轮的字段。

判据只有两条：任务不存在 / 已软删 → `404`（上传进程据此**丢弃**条目，不留僵尸）；其余 → merge 写入。
`log_path` 不在本端点职责内。

### 4.10 `GET /internal/tasks/<id>` —— 任务信息

供 worker 启动恢复协商状态用：`{id, status, lease_seq, worker_id, task_type, ...}`；不存在 `404`。
**不返回已软删任务**（否则上传进程会白传已删任务的产物）。

### 4.11 `GET /internal/tasks/<id>/files` 与 `GET /internal/tasks/<id>/files/<name>`

列出 / 下载任务的输入文件（worker 执行前下载）。`<name>` 需百分号编码。

## 5. 结果上传

worker 的产物上传到**存储服务**（见 `docs/storage-service.md`），不经过 Web 服务：

1. 小文件整传 `POST /internal/tasks/<task_id>/results`，大文件分片（`POST .../results/uploads` →
   `PATCH /internal/uploads/<id>` → `POST /internal/uploads/<id>/complete`，`Upload-Offset` 头续传）。
2. 上传成功得到 `download_url`，再 `POST /internal/tasks/<id>/deliveries` 把地址交付给任务。

**先写本地完成标记，再通知服务端，最后标记已确认**（顺序不可换）：`complete` 只触发迁移，
业务数据走 `fields` / `deliveries`。

## 6. 状态机

状态：`draft → pending → queued → running → completed | failed | cancelled`。

| 从 | 到 |
|---|---|
| `draft` | `pending`, `draft` |
| `pending` | `queued`, `running`, `failed`, `cancelled` |
| `queued` | `running`, `failed`, `cancelled` |
| `running` | `completed`, `failed`, `cancelled`, `queued`（补偿器回收） |
| 终态 | 无出边 |

**租约不变式**：`worker_id` 非空 ⟺ 持有租约 ⟺ 状态为 `queued` 或 `running`。`pending` 无租户。
**任何路径把任务放回 `pending`，都必须 `lease_seq + 1`**，并戳醒领取服务。

## 7. worker 必须遵守的行为

1. **背压**：没有空槽就不发领取请求，任务安静待在数据库里。
2. **认领后顺序**：注册 fencing 令牌 → `ack` → 上报 `running`；任一步失败即放弃并冷却，不执行。
3. **令牌必须注入子进程环境**，子进程的所有回调都带它。**上报全被 403 的第一嫌疑就是令牌没进环境。**
4. **取消**：心跳响应的 `status != "running"` 即触发取消，终止子进程（先同步杀进程组，再 await 收尾）。
5. **自熔**：用本地**单调时钟**判定租约是否临期，必须**严格早于**服务端回收；分区期间浪费的算力上界是租约时长。
6. **回调重试分层**：状态迁移有限次重试后转持久重试队列；进度消息失败即弃；结果上传有限次加退避。
   持久队列的判据是响应体 `reason`：`not_lessee` / `gone` 永久放弃，`already_final` 视为成功。
7. **优雅停机**：先同步捕获令牌 → 同步取消在跑任务 → 用捕获的令牌归还租约；被打断的一轮不写"已确认"。
8. **启动恢复**：按本地完成标记分流——已确认则跳过；未确认则重试回调；服务端 `404` 标记 not_found，
   一段时间后清理；网络故障保留目录下次再试；无标记时问服务端。
9. **空 `LANES` 配置**：所有已声明的 `task_type` 归入**一条** Lane（单 Lane 串行），不得空转。
10. **Lane 是槽位，不是队列**：一条 Lane 同时最多一个任务，跑完才领下一个。并发的扩展轴是**进程**，
    不是"同类型多槽"——那会让 Lane 退回队列语义，把队头阻塞带回来。

## 8. 服务端必须遵守的行为

1. 认领是单条带条件的 `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`；
   `SKIP LOCKED` 只防串行化，不是互斥的来源。
2. 租约时间运算全在 SQL 里用数据库 `now()`。
3. 并发名额按 `pending + queued + running` 计，闸门留在提交处，不放进领取服务（它必须保持"薄"）。
4. 被动重投（补偿器回池）**不消耗**用户配额。
5. 连续无进展的重投设上限，超限转失败；`requeues` 分散说明租约尺寸设错，集中说明该任务有问题。
6. 补偿器的租约回收循环（秒级，按 `lease_until` 点查）**不得**与慢扫描循环（分钟级）合并。
7. 回收判据必须排除"等待用户追加参数"的任务，否则会从健康 worker 手里抢任务。
8. 认领热点索引的混合排序方向无法由 ORM 声明，需要显式 DDL；缺了只会变慢，不报错。

## 9. 契约变更对照（相对旧版本）

仅供迁移参考；新实现不需要兼容旧路径。

| 旧 | 新 |
|---|---|
| `POST /task/lease` | `POST /task/leases` |
| `POST /task/poke` | `POST /task/pokes` |
| `POST /internal/tasks/<id>/received` | `POST /internal/tasks/<id>/ack` |
| `POST /internal/tasks/<id>/status` | `PUT /internal/tasks/<id>/status` |
| `POST /internal/tasks/<id>/heartbeat` | `POST /internal/tasks/<id>/heartbeats` |
| `POST /internal/tasks/<id>/lease/release` | `DELETE /internal/tasks/<id>/lease` |
| `POST /internal/tasks/<id>/step_status` | `PATCH /internal/tasks/<id>/steps` |
| `POST /internal/tasks/<id>/fields/update` | `PATCH /internal/tasks/<id>/fields` |
| `POST /internal/tasks/<id>/results/deliver` | `POST /internal/tasks/<id>/deliveries` |
| `POST /internal/tasks/<id>/results/update`（旧端点） | 删除，用 `fields` |
| fencing 对"无租户且无令牌"放行 | 删除，一律强制 |
| 响应字段 `expected_output` | 删除 |
