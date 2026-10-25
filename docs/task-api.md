# 任务模块：接口与扩展

任务模块是 webx 核心的一部分，**默认启用**（`WEBX_BUILTIN` 不配即全挂，含 `api` / `admin` / `internal` 组里的任务路由）。
调度机制（租约、领取、补偿器）见 `docs/task-scheduling.md`；worker 协议见 `docs/worker-protocol.md`。
路径规范见 `docs/api-conventions.md`。

## 三组接口

| 组 | 前缀 | 鉴权 | 用途 |
|---|---|---|---|
| `api` | `/api/tasks` | 用户令牌 | 用户创建、编辑、提交、取消、重试、查看自己的任务 |
| `admin` | `/api/admin/tasks` | 管理员令牌 + `list-task` 权限 | 全站任务列表与各状态计数 |
| `internal` | `/internal/tasks/<id>/...` | `X-Internal-Token`（+ 写端点 fencing） | worker 回调 |

### 用户接口 `/api/tasks`

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/tasks` | 创建。`{task_type, name?, params_text?, files?, submit?}`；`submit=true` 创建后立即提交 → 201 |
| GET | `/api/tasks` | 列表：本人、仅顶层、分页（`page` `page_size`≤100）、`status`、`task_type`（逗号多值） |
| GET | `/api/tasks?ids=a,b,c` | **轻量状态轮询**（≤100 个），返回精简视图，非本人 / 不存在的静默跳过 |
| GET | `/api/tasks/<id>` | 详情，顶层任务内联 `children` |
| GET | `/api/tasks/<id>/children` | 子任务，可按 `parent_step` 收窄 |
| PATCH | `/api/tasks/<id>` | 编辑 / 推进下一步：`{params_text?, step?, submit?}`（`submit` 默认 true） |
| PUT | `/api/tasks/<id>/info` | 辅助信息（`name`），**任意状态可改** |
| DELETE | `/api/tasks/<id>` | 删除（在飞的 400；子任务 409） |
| POST | `/api/tasks/<id>/submit` | 提交草稿（`none`/`draft` → `pending`），可整体覆盖 `params_text` |
| POST | `/api/tasks/<id>/cancel` | 取消（确有计算在进行 / 待投才行，否则 409） |
| POST | `/api/tasks/<id>/retry` | 失败 / 取消 → 立即排队（消耗 1 次重试） |
| POST | `/api/tasks/<id>/revise` | 失败 / 取消 → 回草稿慢慢编辑（消耗 1 次重试） |
| POST | `/api/tasks/<id>/redo` | 多阶段：回到某个已完成步骤的编辑窗口（不立即投递） |
| POST / GET / DELETE | `/api/tasks/<id>/files[/<name>]` | 输入文件（仅 `none`/`draft` 可改） |

动词只出现在无法映射成 CRUD 的**状态迁移**上（`submit` `cancel` `retry` `revise` `redo`）。

数据按用户隔离：身份取自令牌，非本人的任务一律 403（列表 / 轮询静默跳过）。
处于"待自动准入"队列的任务（`auto_post=1`）由补偿器择机投递，用户手动操作返回 409——
否则会绕过节流并遗留脏标记。

### 状态机

```
none ─▶ draft ─▶ pending ─▶ queued ─▶ running ─▶ completed
                    ▲  │         │         │ ╲─▶ failed / cancelled
                    │  └─────────┴─────────┘
                    └── 补偿器回收 / 主动归还 / retry / 用户推进下一步
```

多阶段任务（`end_step != ""`）：每步完成后停在 `running + sub_status=success + sub_step=""` 等用户追加参数
（**这个状态不占并发名额、不被回收**），用户 `PATCH` 带 `step` 与 `params_text` 推进；到了 `end_step` 才 `completed`。
`cancel` 之后任务进入 `cancelled + sub_status=draft`，既是"暂停"也是编辑窗口，`PATCH` 提交会消耗一次重试。

## 注册任务类型

框架不认识任何具体任务。每种类型在业务入口显式登记：

```python
from webx.lib.task_types import TaskStageConfig, TaskTypeSpec, register_task_type
from webx.lib.task_errors import ParamsError, QuotaExceededError
from webx.lib.task_pricing import CostEstimate

def check_params(params: dict) -> None:
    if params.get("frames", 1) < 1:
        raise ParamsError("frames_too_small", min=1)       # code 即 i18n key → 400

register_task_type(TaskTypeSpec(
    name="render",
    stage=TaskStageConfig(                                  # 默认单阶段；多阶段声明状态机
        end_step="encode",
        step_transitions={"": {"prepare"}, "prepare": {"render", "encode"}, "render": {"render", "encode"}},
        timeout_by_step={"render": 7200},
    ),
    validate_params=check_params,                           # 创建 / 编辑 / 提交都会跑
    estimate_cost=lambda ctx: CostEstimate(consumed_points=len(ctx.task.name)),
))
```

`TaskTypeSpec` 的全部扩展点（都可选）：

| 字段 | 作用 |
|---|---|
| `stage` | 阶段配置：`end_step`、`step_transitions`、`max_steps`、`aggregate_steps`、`timeout_by_step`、`timeout_per_unit` … |
| `validate_params` | 参数校验，失败抛 `ParamsError` |
| `estimate_cost` | 点数估算，返回 `CostEstimate`；不可执行时 `affordable=False` → 403 |
| `unit_count` | 串行重复的单元数，用于放大超时 |
| `resolve_timeout` | 完全接管超时派生 |
| `precheck_submit` | 提交前置闸（异步），在状态翻转**之前**抛业务异常，不留 PENDING 孤儿 |
| `hold_submit` | 提交时挂起：先建子任务、把本任务挂起为 `draft + auto_post=1`，子任务全部终态后由巡检投递 |
| `expected_child_tags` | 进入聚合步骤前应存在的子任务 tag；缺失的也算未就绪 |
| `resolve_step_token` / `normalize_params` / `prune_result_path` | 步骤令牌消歧、参数归一化、重置后的结果裁剪 |

**显式登记，不用装饰器自注册**：装饰器靠 import 副作用生效，漏 import 一个模块就悄悄少一种类型。
未登记的类型按默认单阶段处理；`TASK_REQUIRE_REGISTERED_TYPES=true` 则要求必须登记。

其它接缝：`QuotaPolicy`（档位表）、`UserResolver`（档位 / 特权 / 余额）、`PricingPolicy`（整套计价）、
`on_task_event`（完成 / 失败 / 取消 / 步骤完成事件）、`register_sweep`（补偿器的准入巡检钩子）。

## 父子任务

子任务（`parent_id` 非空）是父任务的内部展开，有**独立额度池**（顶层上限 + 1），不蚕食用户提交顶层任务的名额：

- `task_service.create_child(parent, task_type, tag=..., parent_step=...)` → `draft + auto_post=1`
- 补偿器的 `admit-children` 巡检按 per-user 子任务额度择机投递。**不要对子任务直发 `submit_task`**（会豁免并发闸、击穿额度）
- 父任务想等子任务：类型的 `hold_submit` 里建子任务后调用 `task_service.hold_parent(parent)`；
  `admit-waiting-parents` 巡检在子任务全部终态（成功或失败）后提升父任务

## 点数

**只有估算与可负担闸门，没有账务**（余额 / 预扣 / 结算 / 退款在原项目里不存在，不凭空发明）。
`PricingPolicy` 是整套计价的替换点，`TaskComputePoints` 流水表先就位但没有写入者。真实计费需要新设计。

## 验证

```bash
pytest tests/test_task_core.py            # 数据层：注册表 / 状态机 / 超时 / 配额（sqlite）
pytest tests/test_task_api.py             # 用户接口端到端（真实 PostgreSQL）
pytest tests/test_task_internal_api.py    # worker 回调端到端：完整生命周期、fencing、多阶段（真实 PostgreSQL）
```

PostgreSQL 的获取方式见 `docs/testing.md`。
