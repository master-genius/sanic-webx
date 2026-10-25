# 任务调度（租约 · 领取服务 · 补偿器）

任务模块是 webx 核心的一部分。本篇讲**调度机制**：任务怎么被 worker 领走、怎么保证只被一个 worker 跑、
worker 挂了怎么办。worker 侧的协议见 `docs/worker-protocol.md`，数据层（模型、类型注册表、配额）见源码注释。

## 一句话模型

**拉取模型，数据库是唯一权威。** Web 不投递，只把任务置为 `pending` 并"戳"一下领取服务；
worker 自己用长连接来领。互斥只来自一条带条件的 `UPDATE`，与进程数无关。

```
 用户提交 ──▶ Web ──UPDATE status='pending'──▶ [ 数据库：tasks ] ◀──UPDATE ... SKIP LOCKED──┐
                │                                    ▲                                       │
                └──── POST /task/pokes（空 body）────▶ task_server ◀── POST /task/leases ── worker
                                                     （挂起池）        （长连接，挂起 ≤ 25s）
                                         补偿器 ──按 lease_until 回收──▶ 数据库
```

## 三个进程

| 进程 | 入口 | 职责 | 实例数 |
|---|---|---|---|
| Web | `python run.py` / `server.sh` | 任务 API；提交时 poke | 任意 |
| 领取服务 | `python -m task_server` | 挂起 worker 的领取请求，被 poke 唤醒后回库认领 | **必须 1 个**（挂起池要集中） |
| 补偿器 | `python tools/reconciler.py` | 回收到期租约、慢扫描、准入 / 巡检钩子 | **必须 1 个**（多开会重复巡检） |

领取服务默认只绑 `127.0.0.1`：`/task/pokes` 不鉴权。对 worker 暴露 `/task/leases` 请走反代挂路径前缀，
**不要**把监听地址改成 `0.0.0.0`。反代必须转发 `/task/leases`，**漏转发会整站零分发**。
`TASK_LEASE_HOLD_SECONDS`（默认 25）必须小于反代的 `proxy_read_timeout`。

## 租约与 fencing

| 概念 | 含义 |
|---|---|
| 认领 | `status: pending → queued`，`lease_seq += 1`，写**短租约**（`LEASE_CLAIM_SECONDS`，60s） |
| ack | worker 领到后立即确认，短租约续成常规租约，兜住"响应丢在路上" |
| 心跳 | 每跳把 `lease_until` 推到 `now + LEASE_SECONDS + grace` |
| grace | `clamp(α × 已投入时长, MIN, MAX)`，默认 α=0.12、180s ~ 5h。**服务端算**，每跳回传给 worker |
| fencing 令牌 | `(worker_id, lease_seq)`。所有写操作必须出示，不是当前租户一律拒绝 |

**为什么 grace 按"已投入"而不是"预计还要多久"**：误抢占的损失 = 接手方从当前步骤开头重跑 = 已投入时长；
剩余时长不影响损失，所以也不需要任何耗时估算。

**不变量**：`pending` 无租户；`queued`/`running` 持有租约；终态与多阶段中间步（等用户追加参数）释放租约（`lease_until` 为空）
但保留 `worker_id`（排查线索，也是 `already_final` 判别所需），释放后不得被迟到的心跳 / 归还复活；
**任何路径把任务放回 `pending` 都必须 `lease_seq + 1`**（走 `lease.enter_pending_pool`），否则同一台 worker
再次领到该任务时，第一次运行残留的在途回调会匹配通过并污染第二次运行。

**租约的时间运算读写都不出 SQL**：Web / 领取服务 / 补偿器是不同进程，时钟漂移会让补偿器从健康 worker 手里回收任务。

### 认领排序

`level DESC → compute_points DESC → rand_rank ASC → submitted_at ASC`。`rand_rank` 是 fair-share 桶号：
`该用户在飞任务数 × 10000 + random(0,10000)`，**乘数必须等于抖动范围**，A 一次提交 30 个不会把 B 挤到 30 个之后。
同档位内不是 FIFO（刻意打散）；不保证无饥饿——持续无限新用户涌入时轮不到，那是过载表现。

认领热点索引 `idx_tasks_claim` 是**混合排序方向**，Tortoise 声明不了，用显式 DDL。缺了不报错，只慢 400 倍。
`dbsync` 同步完、领取服务启动时都会确保它存在（`lease.ensure_claim_index`）。

## 补偿器：三个独立节拍

| 循环 | 默认节拍 | 做什么 |
|---|---|---|
| 租约回收 | 5s | 按 `lease_until < now()` 回收；**先**判"连续无进展超限"失败，**再**回收（顺序不可颠倒） |
| 慢扫描 | 600s | 心跳陈旧的第二道网、放弃判定（>168h 判失败）、空任务清理、**待领取池滞留告警** |
| 准入 / 巡检 | 30s | 执行注册的钩子（`register_sweep`） |

**租约回收循环绝不能与慢扫描合并**：回收是走索引的点查、决定故障发现速度；慢扫描是多分支重扫。

要点：
- 只有 **RUNNING** 的回收计入 `requeues`（"连续无进展"）。从 QUEUED 回收意味着任务根本没跑起来，最常见的成因是
  Web 不可达——整池任务会被一起推向阈值，十来分钟的 Web 停机就能把用户任务成批判成 failed。
- 被动重投**不写 `retry`**：retry 是用户主动重试的配额计数，补偿器不蚕食它。
- 慢扫描排除"等待用户"的任务（多阶段、`sub_status=success` 且 `sub_step=""`），否则会从健康 worker 手里抢任务。
- 滞留告警：`pending` 是全系统唯一没有计时器会响的状态——没有能跑某个类型的 worker 在线时，任务会无声地躺到天荒地老。
  告警而不是自动失败：那是运维问题，不是任务问题。

### 钩子

补偿器不认识任务类型。拥有任务知识的模块注册进来：

```python
from webx.services.reconciler import register_failed_hook, register_sweep

async def admit_children(): ...          # 每个准入节拍各自 try 执行，一个异常不影响其余
register_sweep("admit-children", admit_children)

async def notify(task_ids, reason): ...  # 任务被补偿器批量判失败时回调（通知用户 / 告警）
register_failed_hook(notify)
```

钩子注册表在**模块级**，与补偿器实例无关：钩子在 import 期注册，而那时业务配置往往还没注册，
此时创建实例会读到过期的默认配置。`tools/reconciler.py` 按 `--modules` / `WEBX_MODULES` 加载每个业务模块的 **`<pkg>/tasks.py`**（存在才加载；web 同样按此约定加载）。
**任务类型的登记（`register_task_type`）与补偿器钩子都应写在这个文件里**——只写在 controller 或 `run.py` 里，补偿器进程看到的类型
注册表就是空的，它驱动的子任务准入、父任务提升会按默认单阶段处理（跳过参数校验 / precheck / 点数闸，超时被覆盖成默认值）。

## 配置

| 键 | 默认 | 说明 |
|---|---|---|
| `LEASE_CLAIM_SECONDS` / `LEASE_SECONDS` | 60 / 100 | 认领短租约 / 常规租约 |
| `LEASE_GRACE_ALPHA` / `_MIN_SECONDS` / `_MAX_SECONDS` | 0.12 / 180 / 18000 | grace 公式 |
| `LEASE_REQUEUE_LIMIT` | 10 | 连续无进展的被动重投上限，超限判失败 |
| `TASK_SERVER_URL` | `http://127.0.0.1:9020` | Web / 补偿器 poke 的地址 |
| `TASK_SERVER_HOST` / `_PORT` | `127.0.0.1` / `9020` | 领取服务监听 |
| `TASK_LEASE_HOLD_SECONDS` | 25 | 单次领取最长挂起，须小于反代超时 |
| `TASK_LEASE_FALLBACK_SECONDS` | 1.0 | 挂起期间的兜底重查间隔（poke 丢了的安全网，不是主路径） |
| `RECONCILER_*` | 见 `.env.example` | 各循环节拍与阈值 |
| `INTERNAL_TOKEN` | 空 | 服务间令牌，**空则拒绝一切**；领取服务与 Web 必须一致 |

租约参数的**单一来源**是核心配置：领取服务写短租约、Web 续租、补偿器回收三个进程读同一份。
若各存一份副本，漂移会让 worker 的自熔时刻与服务端回收时刻错位——"服务端已租给 B、A 仍认为自己持有"。

## 验证

三层，从快到慢：

1. **SQL 语义**（真实 PostgreSQL）：`pytest tests/test_task_lease.py tests/test_reconciler.py`。
   认领互斥、排序、筛选、进池不变量、fair-share、续租 grace、回收、fencing 的三种拒绝、热点索引。
2. **真实进程**：`pytest tests/test_task_server.py` —— 起 `python -m task_server`，验证挂起 → poke 唤醒 → 认领、
   兜底轮询、广播唤醒下的互斥、数据库故障时的 503。
3. **部署验证**（线上 / 预发，**不改数据**）：

   ```bash
   python -m task_server verify --url http://127.0.0.1:9020 --token "$INTERNAL_TOKEN"
   ```

   覆盖：存活、令牌（缺 / 错 → 401，对 → 通过）、请求校验、poke 通道、数据库连通。
   **绝不发起真实认领**（用的是不会匹配任何任务的随机类型）。退出码 0/1/2。

测试需要 PostgreSQL，获取方式见 `docs/testing.md`。

## 排障

| 现象 | 第一嫌疑 |
|---|---|
| 整站零分发 | 反代没转发 `/task/leases`；或 `INTERNAL_TOKEN` 两边不一致（401） |
| worker 的所有回调被 403 | fencing 令牌没注入子进程环境 |
| 任务长时间 `pending` | 没有能跑该类型的 worker 在线——看补偿器的"滞留"告警按类型的分布 |
| 一批任务同时被判 failed | 看 `requeues` 是**分散**（租约尺寸设错）还是**集中在一个任务**（任务本身有问题） |
| 认领变慢但不报错 | `idx_tasks_claim` 被 `dbsync --force` 清掉了——重启领取服务或跑 `dbsync` |
| 领取一直 503 | 数据库不可达；服务照常启动，恢复后自愈 |
