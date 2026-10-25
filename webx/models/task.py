"""任务模型。

字段设计原则：
  - 核心状态字段：固定类型，强校验
  - 参数 / 配置字段：TEXT + JSON，灵活扩展（任务类型各自约定内部 schema，框架不解析）
  - 文件字段：TEXT + JSON，支持多文件及类型标记

任务类型是**字符串**，没有枚举——有哪些类型由 ``webx.lib.task_types`` 的注册表决定。
"""

from __future__ import annotations

from enum import StrEnum
from typing import Set

from tortoise import fields
from tortoise.models import Model

from webx.lib.genid import gen_id


class TaskStatus(StrEnum):
    """任务状态。"""

    NONE = "none"            # 空任务（刚创建，未上传文件）
    DRAFT = "draft"          # 草稿（已有输入，未提交）
    PENDING = "pending"      # 待处理（已提交，等待 worker 领取）
    QUEUED = "queued"        # 已入队（worker 已认领，持有租约）
    RUNNING = "running"      # 执行中
    COMPLETED = "completed"  # 已完成（终态）
    FAILED = "failed"        # 已失败（终态）
    CANCELLED = "cancelled"  # 已取消（终态）


TERMINAL_STATUSES = frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED})
#: "进入了投递流水线"的状态——占并发名额、可能持有租约
ACTIVE_STATUSES = (TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING)


class SubStatus(StrEnum):
    """子步骤状态（多阶段任务内部的步骤进度，绑定 sub_step）。"""

    PENDING = "pending"  # 等待投递
    RUNNING = "running"  # 正在执行某步骤
    DRAFT = "draft"      # 用户已保存参数但未确认执行
    SUCCESS = "success"  # 步骤完成
    FAILED = "failed"    # 步骤失败


class Task(Model):
    """任务。

    状态机合法迁移::

        pending -> queued -> running -> completed / failed
                              running -> queued   (补偿器回收，绕过 transition_to)
                              running -> pending  (多阶段：用户追加参数，绕过状态机)

    多阶段任务：
      - ``end_step=""``  → 单阶段，completed 即终态
      - ``end_step!=""`` → 多阶段，仅 end_step 完成时 completed 才是终态
      - ``running + sub_status=success + sub_step=""`` → 等用户追加参数
    """

    # ==================== 核心字段 ====================
    id = fields.CharField(max_length=16, pk=True, default=gen_id, description="任务 ID")
    status = fields.CharEnumField(TaskStatus, default=TaskStatus.NONE, index=True, description="任务状态")
    task_type = fields.CharField(max_length=64, description="任务类型（见 webx.lib.task_types 注册表）")
    name = fields.CharField(max_length=128, default="", description="任务名称（方便标识和查找）")
    user_id = fields.CharField(max_length=18, default="", index=True, description="用户 ID")
    org_id = fields.CharField(max_length=16, default="", index=True, description="组织 ID")

    # ==================== 父子任务 ====================
    parent_id = fields.CharField(
        max_length=16, default="", index=True,
        description="父任务 ID（''=顶层；非空=某父任务的子任务）",
    )
    """子任务语义由 parent_id 承载，与 task_type 解耦——子任务在执行 / 上传逻辑上与普通任务一致。
    配额对子任务有独立额度池（不蚕食用户提交顶层任务的名额）；列表接口默认只返回顶层。"""
    tag = fields.CharField(max_length=50, default="", description="自由标签（前端展示 / 子任务回填对应）")
    parent_step = fields.CharField(
        max_length=30, default="",
        description="子任务服务于父任务的哪一步。(parent_id, parent_step, tag) 唯一定位一个子任务",
    )
    is_delete = fields.BooleanField(default=False, index=True, description="软删除标记，留给清理任务物理回收")
    auto_post = fields.SmallIntField(
        default=0, index=True,
        description="自动准入标记（0=普通；1=待自动准入）。子任务等的是 per-user 子任务额度；"
                    "顶层任务等的是自己的子任务算完。两个集合靠 parent_id 区分、不相交",
    )

    # ==================== 重试 / 心跳 / 超时 ====================
    retry = fields.IntField(default=0, description="当前重试次数（只由用户主动 retry/redo 消费）")
    allowed_retries = fields.IntField(default=10, description="允许的最大重试次数")
    last_heartbeat = fields.BigIntField(
        default=0, index=True, description="最后心跳时间（毫秒 Unix 时间戳，0=从未收到）",
    )
    timeout_seconds = fields.IntField(default=172800, description="子进程超时（秒），本步上限")

    # ==================== 分发优先级（认领排序键） ====================
    # 认领排序：level DESC → compute_points DESC → rand_rank ASC → submitted_at ASC
    #
    # ⚠️ 三个字段都必须冗余在 task 行上，绝不能 JOIN user 表取：
    #   ① 表间不建外键，关联只存裸 ID；
    #   ② 认领是全库最高频的写路径入口，JOIN 会毁掉索引（实测可差 100~500 倍）；
    #   ③ 快照让队列顺序不会因别处改了 user 表而突然跳变。
    level = fields.SmallIntField(default=0, description="进池时的用户档位快照（认领第一排序键）")
    compute_points = fields.IntField(
        default=0, description="任务级算力点汇总快照（认领第二排序键，0=免费档）",
    )
    rand_rank = fields.IntField(default=0, index=True, description="fair-share 序（认领第三排序键，正序）")
    """fair-share 排序键——**不是纯随机**。

    公式（在 SQL 内求值，见 services/lease 的进池语句）::

        rand_rank = 该用户当前在飞任务数 × 10000 + random(0, 10000)

    乘数必须等于抖动范围，桶才既不重叠也不留空隙。写成 ``N + random(0,10000)`` 会让 N 被随机项淹没，
    等于没有公平性。效果：A 一次提交 30 个得到桶 0..29，B 后到得到桶 0..2，合并后 B 与 A 交错。
    ⚠️ 不保证无饥饿：持续无限新用户涌入时轮不到——那是过载表现，任何调度都救不了。
    """

    # ==================== 任务归属（租约） ====================
    # 本组三字段共同表达"这个任务现在归谁、归到什么时候、这是第几次归属"。核心不变量：
    #   status ∈ {QUEUED, RUNNING} ⟹ 持有租约；终态与多阶段中间步释放租约（lease_until=NULL）但保留 worker_id
    #   status = PENDING ⟹ worker_id = '' 且 lease_until = NULL（公共池，无租户）
    #   任何路径 → PENDING ⟹ lease_seq += 1
    worker_id = fields.CharField(max_length=64, default="", index=True, description="当前 / 最近持有本任务的 worker 实例")
    lease_until = fields.BigIntField(
        null=True, index=True, description="租约到期时刻（毫秒 Unix 时间戳，NULL=未持有）",
    )
    """⚠️ 一切租约时间运算必须在 SQL 内用 now() 求值，不可传应用侧时间戳：
    task-server / 补偿器 / web 是不同进程，时钟漂移会让补偿器从健康 worker 手里回收任务。"""
    lease_seq = fields.IntField(default=0, description="租约序号（每次认领 +1），fencing 令牌的后半")
    requeues = fields.IntField(default=0, description="连续无进展的被动重投次数（推进一步即归零）")
    """数的是"连续无进展"，不是一生的重投次数。存在理由：last_heartbeat 每次被新 worker 接手都会刷新，
    故对"跑一会儿、发过心跳、然后反复失败"的任务永远够不着阈值，形成无限重投；本字段是那个缺口的封顶。
    不复用 retry——retry 是用户主动重试的配额计数，补偿器的被动重投不蚕食用户配额。
    归零点：进终态 / 用户显式 retry / sub_step 成功推进。"""

    # ==================== 多阶段进度 ====================
    steps = fields.TextField(default="[]", description="已完成步骤列表（JSON 数组）")
    sub_step = fields.CharField(max_length=30, default="", description="当前步骤名（''=无下一步 / 等用户追加参数）")
    end_step = fields.CharField(max_length=30, default="", description="最终步骤名（''=单阶段）")
    sub_status = fields.CharEnumField(SubStatus, default=SubStatus.PENDING, index=True, description="子步骤状态")
    step_message = fields.TextField(default="", description="当前步骤动态消息（执行期间可实时更新）")

    # ==================== 输入 / 参数 ====================
    files_json = fields.TextField(default="[]", description="输入文件清单（JSON）")
    params_text = fields.TextField(default="{}", description="计算参数（JSON 文本，内部 schema 由任务类型约定）")

    # ==================== 结果 ====================
    result_path = fields.TextField(default="", description="结果地址（JSON，可嵌套，按步骤 merge）")
    log_path = fields.TextField(default="", description="日志地址")
    error_message = fields.TextField(default="", description="错误信息")

    # ==================== 时间戳 ====================
    submitted_at = fields.DatetimeField(null=True, description="本轮提交进入待分发池的时刻（认领的确定性兜底排序键）")
    """不能用 created_at 代替：一个躺了三天的草稿一旦提交会凭空排到队首，插所有人的队。"""
    start_time = fields.DatetimeField(null=True, description="本轮计算开始时间（服务端在迁入 RUNNING 时统一写入）")
    """语义是"本轮"起点：每次迁入 RUNNING 都刷新。由服务端生成，不接受 worker 传值——
    worker 分布在多台机器，时钟 / 时区各异，时间基准必须单一。"""
    created_at = fields.DatetimeField(auto_now_add=True, description="创建时间")
    updated_at = fields.DatetimeField(auto_now=True, description="更新时间")

    class Meta:
        table = "tasks"
        table_description = "任务表"
        indexes = [
            # 滞留告警与按提交时间的运维查询。不服务认领。
            ("status", "submitted_at"),
            # 服务两处"按用户数在飞任务"的高频计数：并发配额闸、rand_rank 的 fair-share 桶号。
            # 实测（20 万行表 / 池中 5 万）该子查询 2.389ms → 0.327ms；rand_rank 在每次提交的写路径上。
            ("user_id", "status"),
        ]

    # ── 认领热点索引：不能由 Tortoise 声明，必须用显式 DDL ───────────────────
    #
    # 认领排序是 DESC/DESC/ASC/ASC 混合方向：
    #   · Meta.indexes 的元组形式只能建全升序索引——正扫反扫都对不上混合方向，规划器退回 Sort（实测 20ms）
    #   · Index(RawSQL(...)) 形式会被外包一层括号，实测语法错误
    # 名字刻意不带 `_idx` 后缀：lib/schema_sync 普通模式只管理 `_idx` / `_uidx` 后缀的索引，
    # 外部索引一概不碰。⚠️ 但 --force 硬同步会把它当野索引清掉——清掉后认领从 0.05ms 退化到 20ms，
    # 且不会报错，只是慢。所以任务服务启动时会重新确保它存在（services/lease.ensure_claim_index）。
    #
    # 实测（5 万行 PENDING，PostgreSQL 16）：不筛选 20.3ms → 0.053ms（383×）。
    CLAIM_INDEX_NAME = "idx_tasks_claim"
    CLAIM_INDEX_DDL = (
        'CREATE INDEX IF NOT EXISTS idx_tasks_claim ON "tasks" '
        '("status", "is_delete", "level" DESC, "compute_points" DESC, '
        '"rand_rank", "submitted_at")'
    )

    VALID_TRANSITIONS: dict[TaskStatus, Set[TaskStatus]] = {
        TaskStatus.NONE: {TaskStatus.DRAFT, TaskStatus.NONE},
        TaskStatus.DRAFT: {TaskStatus.PENDING, TaskStatus.DRAFT},
        TaskStatus.PENDING: {TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.FAILED, TaskStatus.CANCELLED},
        TaskStatus.QUEUED: {TaskStatus.RUNNING, TaskStatus.FAILED, TaskStatus.CANCELLED},
        TaskStatus.RUNNING: {
            TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED,
            TaskStatus.QUEUED,  # 仅供补偿器回收使用（filter().update()，不走 transition_to）
        },
        TaskStatus.COMPLETED: set(),
        TaskStatus.FAILED: set(),
        TaskStatus.CANCELLED: set(),
    }

    def can_transition_to(self, new_status: TaskStatus) -> bool:
        return new_status in self.VALID_TRANSITIONS.get(self.status, set())

    async def transition_to(self, new_status: TaskStatus, **extra_fields) -> None:
        """迁移状态（一次 save 同时更新额外字段）。非法迁移抛 ``ValueError``。"""
        if not self.can_transition_to(new_status):
            raise ValueError(f"非法状态迁移：{self.status} → {new_status}")
        self.status = new_status
        update_fields = ["status", "updated_at"]
        for field_name, value in extra_fields.items():
            setattr(self, field_name, value)
            update_fields.append(field_name)
        await self.save(update_fields=update_fields)

    def __str__(self) -> str:
        return f"Task({self.id}, status={self.status}, type={self.task_type})"
