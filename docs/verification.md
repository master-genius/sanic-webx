# 验证过程

“验证”分四层，从内到外。**每一层回答的问题不同，下一层不能替代上一层**：单测全绿不代表几个服务拼起来能跑，
整栈用例过了也不代表你的线上环境配对了。

| 层 | 回答什么 | 在哪跑 | 命令 |
|---|---|---|---|
| ① 静态与架构 | 依赖方向没被破坏、没有残留的旧项目名、代码没有明显的未使用/未定义 | 开发 / CI | `pyflakes`、`tests/test_layering.py` |
| ② 单元与接口 | 每条规则、每个端点、SQL 的互斥性 | 开发 / CI（需 PostgreSQL） | `pytest -q` |
| ③ 整栈端到端 | 五个**独立进程**拼起来，从用户提交到产物下载，以及几种故障路径 | 开发 / CI（需 PostgreSQL） | `pytest tests/test_stack_e2e.py` |
| ④ 线上体检 | 你的这套部署（配置、令牌、反代、网络）是不是真的通了 | 部署后 | `verify` / `doctor` / `smoke`（见下） |

PostgreSQL 怎么获取见 `docs/testing.md`。认领 / 租约的 SQL 是 PG 专有的，**不 mock 数据库**，
拿不到 PG 时测试是**失败**而不是跳过。

---

## ① 静态与架构

```bash
.venv/bin/python -m pyflakes webx webx_storage webx_worker task_server tools tests app run.py
.venv/bin/python -m pytest -q tests/test_layering.py
```

`test_layering.py` 用 AST 扫描全部 import，焊死这些约束：包内无循环依赖；`lib` 不碰 ORM；
**webx 核心不依赖 `webx_storage` / `webx_worker`**；**`webx_worker` 完全独立**（只许标准库 + httpx + 自己，不 import webx 的任何东西，
这样它才能单独打包发给算力节点 / 普通用户）。

另有一条硬约束：代码、文档、环境变量里**不得出现旧项目名**（`tests/worker/test_docs_and_hygiene.py` 守着）。

## ② 关键不变量与守护它们的用例

这些是系统“不出错”的根本。改动相关代码前先看对应用例；改完后对防线做**变异验证**（改坏它、确认用例变红、还原）。

| 不变量 | 为什么重要 | 守护用例 |
|---|---|---|
| 同一任务同一时刻只被一个 worker 认领 | 否则同一份算力被烧两次 | `test_task_lease::test_claim_is_exclusive_under_concurrency` |
| 认领只取 PENDING 且存活的、请求类型内的任务 | 能力声明在 worker，服务端不存能力表 | `test_claim_only_takes_pending_live_tasks_of_the_requested_types` |
| 任何路径回 PENDING 都 `lease_seq += 1` | 上一轮 worker 的在途回调否则会写进新一轮 | `test_enter_pool_restores_the_invariants_and_bumps_the_fencing_seq`、`test_returned_to_pool_task_rejects_the_previous_lessees_callbacks` |
| fencing 强制：过期 / 别人的 / 缺失的令牌一律拒绝 | 防“僵尸 worker”覆盖正在执行的新一轮 | `test_check_fence_distinguishes_the_three_rejections`；HTTP 层见 `test_task_internal_api` |
| 同一租户对已终态任务的重复上报是**幂等成功** | 否则回调重投会淹没真正的告警 | `test_same_worker_on_a_finished_task_is_an_idempotent_success` |
| 只有 RUNNING 的回收计入 `requeues` | “Web 不可达”会让整池 QUEUED 任务被推向失败 | `test_reclaim_only_running_counts_toward_requeues` |
| 先判失败（超限）再回收 | 反过来阈值凭空少一次 | `test_fail_over_limit_runs_before_reclaim` |
| 租约回收循环与慢扫描独立 | 合并后秒级回收会被分钟级扫描拖慢 | `test_lifecycle_runs_all_three_loops_and_stops_cleanly` |
| poke 能立即唤醒挂起的领取请求；丢了 poke 兜底轮询仍能找到 | 唤醒是主路径、兜底是安全网，两者都要有 | `test_task_server::test_a_poke_wakes_a_hanging_request_immediately`、`test_without_a_poke_the_fallback_poll_still_finds_the_task` |
| 广播唤醒时单个任务只交给一个等待者 | 互斥靠数据库那条条件 UPDATE，不靠唤醒 | `test_a_broadcast_wakeup_still_hands_the_single_task_to_exactly_one_waiter` |
| 数据库故障回 503，让 worker 退避而不是紧密重试 | 防止故障时把库打得更死 | `test_database_outage_returns_503_so_workers_back_off` |
| 认领热点索引缺了会自己补回来 | 缺了不报错，只慢约 400 倍 | `test_ensure_claim_index_is_idempotent_and_repairs_a_dropped_index` |
| 优雅停机时：先停领取，再取消在跑任务，再带令牌归还 | 否则一次运维重启会把可重跑的任务判成 failed | `tests/worker/test_cancellation_and_shutdown.py`；整栈见 ③ |
| 崩溃重启后重试 `complete` 带得上令牌 | 否则已算完的任务只能等租约回收后重跑 | `tests/worker/test_recovery.py::test_complete_retry_uses_the_token_recorded_in_ok_json` |
| 自熔用服务端算好的剩余时长，不吃时钟偏移 | 偏快的机器会把健康任务掐掉 | `test_cancellation_and_shutdown.py::test_self_fence_prefers_server_side_remaining_over_skewed_wall_clock` |
| 废弃别名只转发，且不绕过鉴权 | 别名是旧调用方的路径，不能成为后门 | `test_http_e2e.py` 的 `*deprecated*`、`test_upload_session_routes_require_login_on_both_canonical_and_alias` |

## ③ 整栈端到端：`tests/test_stack_e2e.py`

用真实 PostgreSQL 拉起 **web / 领取服务 / 存储 / 补偿器 / worker 五个独立进程**（四个服务经 `server.sh <角色>` 启动，
即上线用的那条入口），走用户的视角。租约参数压短（常规租约 4s + grace 2s，worker 心跳 0.5s、自熔余量 1s），
否则默认的 100s + 180s 要等几分钟；生产参数不受影响。

| 用例 | 路径 | 证明了什么 |
|---|---|---|
| `test_submit_claim_execute_deliver_complete` | 提交 → 长连接领取 → 子进程执行 → 产物传存储 → completed | 全链路接缝：路径、鉴权头、令牌、响应形状；产物真的可下载且内容是本次任务写的；无多余回收 |
| `test_script_failure_marks_task_failed_with_reason` | 脚本非零退出 | 失败原因（退出码）上报，租约清空 |
| `test_user_cancel_stops_the_child_and_frees_the_lane` | 用户取消在跑任务 | 子进程被杀、Lane 释放（后续任务不必等睡完） |
| `test_sigterm_returns_the_task_to_the_pool_and_another_worker_finishes_it` | 优雅停机 | 在跑任务回池（不是 failed）、`lease_seq` 递增、换 worker 能做完 |
| `test_killed_worker_is_reclaimed_by_reconciler_and_rerun` | worker 被 `kill -9` | 租约到期后补偿器回收（计入 `requeues`）、重跑成功；孤儿子进程的迟到回调因 fencing 被拒 |
| `test_internal_group_rejects_missing_and_wrong_token` | 真进程里的 internal 组鉴权 | 令牌缺 / 错 / 用户令牌冒充都是 401 |
| `test_ops_verification_commands_pass_against_the_live_stack` | 三条运维验证命令 | `task_server verify`、`webx_storage verify`、`webx_worker doctor` 对活栈全过——体检手段自己不能是坏的 |
| `test_smoke_script_passes_against_the_live_stack` | `tools/smoke.py` | 成功时退出 0；任务停在 pending 时退出 1 且提示查 `/task/leases` |

这套用例做过变异验证：去掉补偿器 → kill -9 用例红；去掉停机时的两处租约归还 → SIGTERM 用例红。
（只去掉其中一处不会红：`app.py` 的停机三步与 `broker.py` 的 `finally` 是**两条独立的归还路径**，互为冗余。）

新增一种故障路径的做法：在 `Stack` 里加一个动作（起 / 停 / 杀某个进程），断言**数据库里的终态**而不是日志文案。
失败时 `wait_status` 会把各进程日志尾部一起打出来。

## ④ 线上体检

自底向上，前一层不过就别看后一层：

```bash
# 1. 领取服务：可达、鉴权、数据库（不改数据）
python -m task_server verify --url http://127.0.0.1:9020 --token $INTERNAL_TOKEN

# 2. 存储服务：对象 / 分片 / 签名全链路
python -m webx_storage verify --url http://127.0.0.1:9010 --token $INTERNAL_TOKEN

# 3. worker：channel 配置、脚本、令牌、领取 / 回调 / 存储三个通道（在 worker 机器上跑）
python -m webx_worker doctor --home /data/worker

# 4. 整条链路：提交 → 领取 → 执行 → 回调 → 完成 → 产物可下载（会创建并删除一个真实任务）
python tools/smoke.py --web https://api.example.com --login 用户:密码 --storage https://files.example.com
```

退出码约定：`0` 通过；`1` 有检查失败；`2` 前置条件不满足（连不上、登录失败、配置不可用）。
各项失败时的排查入口见 `deploy/README.md` 的“故障对照”。

### 它们各自能发现什么

- `task_server verify`：反代没转发 `/task/leases`（404）、令牌不一致（401）、领取服务连不上数据库。用空 `slots` 探测，
  **不可能领到任何任务**，所以可在线上放心跑。
- `webx_storage verify`：令牌配错、存储目录不可写、`Range` 被网关吞掉、分片协议（`Upload-Offset` 头、409 对齐）被反代改坏、
  签名下载的公开路径被拦截。所有测试数据写在 `__verify__/` 下，无论成败都清理。注意它测的是**存储服务自身**的签发 + 下载闭环，
  测不出“Web 签发、存储服务校验”两侧密钥是否一致。
- `webx_worker doctor`：worker 的 channel 脚本不存在、解释器不对、工作目录不可写、三条通道哪一条不通（以及是令牌错还是路由没转发）。
- `smoke`：以上都通但**组合起来**不通——比如 worker 的能力声明里没有该任务类型（任务永远停在 pending）、
  回调打到了错误的地址（领到任务但 ack 失败，任务反复被回收）。

## 已知的验证盲区

如实记录，别假装覆盖了：

- **Windows**：worker 只支持 POSIX（信号、`setsid`、AF_UNIX），未适配也未验证。
- **真实 GPU / conda 环境**：整栈用例的 channel 用的是示例里的无业务脚本（`env: ""`、当前解释器），
  conda / uv 环境激活路径只有单元测试，没有在整栈里跑过。
- **多机时钟偏移**：已让自熔采用服务端算好的剩余时长（`lease_remaining_ms`），但整栈用例都在同一台机器上，
  没有真实的跨机偏移场景；只有“偏移的墙钟下仍采用服务端值”的单元测试。
- **uploader 异步上传进程**：整栈里关闭了（`UPLOADER_ENABLED=false`，示例任务的产物小于阈值，同步上传）；
  它的行为由 `tests/worker/test_uploader*.py` 覆盖，与服务端的联调只到协议替身。
- **阿里云短信签名**：`webx/lib/sms` 的签名算法没有对着真实服务商验证过（只有适配层和假发送器的测试）。
