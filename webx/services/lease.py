"""租约：服务端的全部租约写操作收口于此。

## 铁律：租约的时间运算**读写都不出 SQL**

绝不把 Python 时间戳传进来当判据。Web / 领取服务 / 补偿器是三个不同进程（将来可能不同机器），
时钟漂移会让补偿器从一台**健康** worker 手里回收任务——这与"租约设太短"是同一类静默故障，
而那条不等式管不到它。``last_heartbeat`` 由服务端写入，是同一条理由；改用应用侧时间戳是倒退。

因为 ``lease_until`` 是毫秒整数而 ``start_time`` 是 datetime，grace 的公式跨了两种表示——
那正是"顺手传个 Python 时间戳"最容易发生的地方，故本模块一律用裸 SQL。

## grace 为什么按"已投入"而不是"预计还要多久"

误抢占的损失 = 接手方要从**当前步骤开头**重跑（步骤内无检查点）= ``now - start_time``。
跑了 5h 还剩 10s 与跑了 5h 还剩 6h，损失一样大——**剩余时长不影响损失**。
由此也不需要任何耗时估算：elapsed 就是复杂度的显影，且比估算更准。

## 三个旋钮各管一个维度（不可混为一谈）

| 维度 | 是谁的属性 | 旋钮 | 该不该固定 |
|---|---|---|---|
| 任务复杂度 | 任务的 | ``α × elapsed`` | 否，必须自适应 |
| 网络抖动 | 机器 / 链路的 | ``GRACE_MIN`` | **是，本来就该固定** |
| 防无限增长 | 系统安全 | ``GRACE_MAX`` | 是 |

一个固定的阈值之所以"固定得不对"，不是因为固定，而是它一个数兼职了前两个维度，只能取两者的最大值。

## fencing 一律强制

没有"无令牌放行"的兼容分支：写操作必须出示 ``(worker_id, lease_seq)``。
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import Any, Optional

from sanic.response import json as _json
from tortoise import Tortoise

from webx.config import settings
from webx.models.task import Task
from webx.services.quota import get_user_resolver

logger = logging.getLogger(__name__)

#: 毫秒时间戳的 SQL 表达式——见模块 docstring 的铁律
NOW_MS = "(EXTRACT(EPOCH FROM now()) * 1000)::bigint"

# grace 表达式。
#
# ⚠️ ``start_time IS NULL`` 时取的是**下限而不是 0**（订正过的真 bug）：
# 该状态是"已认领、已 ack、但还没上报 running"，中间隔着环境激活、输入下载、子进程拉起。
# ack 的作用恰恰是把短租约替换成常规租约，替换之后短租约已经不存在了。给 0 的话这段就只有
# LEASE_SECONDS 的保护，大输入任务会在一台健康 worker 正在启动时被回收，然后两份并发跑。
# 取下限也符合 MIN 本身的定位：它覆盖的是网络抖动，与任务跑了多久无关，启动阶段同样需要。
_GRACE_MS = """
CASE WHEN start_time IS NULL THEN $2
     ELSE LEAST(
            GREATEST(
              ($1 * EXTRACT(EPOCH FROM (now() - start_time)) * 1000)::bigint,
              $2
            ),
            $3
          )
END
"""


def _grace_params() -> list[Any]:
    return [
        settings.LEASE_GRACE_ALPHA,
        settings.LEASE_GRACE_MIN_SECONDS * 1000,
        settings.LEASE_GRACE_MAX_SECONDS * 1000,
    ]


async def _q(sql: str, params: list[Any]) -> list[dict]:
    conn = Tortoise.get_connection("default")
    return await conn.execute_query_dict(sql, params)


# --------------------------------------------------------------------------
# 进池
# --------------------------------------------------------------------------

# fair-share 排序键的 SQL 表达式——**单一来源**。
#
# 有两处会写它：进池（_ENTER_POOL_SQL）与 auto_post 子任务的认领（_CLAIM_CHILD_SQL）。
# 两处各抄一份必然漂移，而漂移在这里的表现是"同一个池子里两半任务按不同公平口径排序"——
# 不会报错，只会不公平。
#
# ⚠️ 表达式内不含 $n 占位符。它是对 tasks 的相关子查询 count(*)，依赖外层的 ``tasks.user_id``，
#    因此只能内联进 UPDATE tasks 语句，不能单独求值。
_RAND_RANK_EXPR = """(
                       -- fair-share 排序键：**已占用量主导，随机只在桶内打散**。
                       --
                       -- 乘数必须等于抖动范围（都是 10000），这样桶既不重叠也不留空隙：
                       --   N=0 → [0,10000)   N=1 → [10000,20000)   N=5 → [50000,60000)
                       -- 若乘数小于抖动范围，桶会重叠、计数信号被稀释——极端情况（乘数=1）下
                       -- N 的量级(0~几十)会被 10000 的随机项完全淹没，一个已占 50 个位置的用户
                       -- 排在空手用户前面的概率仍有 49.75%，等于没有公平性可言。
                       --
                       -- 效果：用户 A 一次提交 30 个得到桶 0..29，用户 B 后到得到桶 0..2，
                       -- 合并后 B 与 A **交错**而不是排在 30 个之后。
                       COALESCE((
                         SELECT count(*) FROM tasks a
                          WHERE a.user_id = tasks.user_id
                            AND a.is_delete = false
                            AND a.id <> tasks.id
                            AND a.status IN ('pending', 'queued', 'running')
                            -- 与 quota 的"在飞"同口径：多阶段任务停在 sub_status=success
                            -- （等用户追加参数）时不算在飞——它没在争队列。
                            -- **唯一的差异是本处不限 parent_id**：队列公平看的是"这个用户总共占了
                            -- 几个位置"，fan-out 子任务同样在占，不该漏掉。
                            AND (a.end_step = '' OR a.sub_status IN ('pending', 'running'))
                       ), 0) * 10000
                       + (random() * 10000)::int
                     )::int"""

# 进池时同时刷新分发排序键。``rand_rank`` 用 SQL 的 random() 而不是应用侧随机：
# 多进程同种子风险，且与"分发相关的运算都不出 SQL"的口径一致。
# ``level`` 由调用方传入（$3）——从用户体系快照来，见 enter_pending_pool；
# **认领路径绝不 JOIN**（热点 + 无外键铁律）。
_ENTER_POOL_SQL = f"""
UPDATE tasks SET
    status         = 'pending',
    worker_id      = '',
    lease_until    = NULL,
    lease_seq      = lease_seq + 1,
    last_heartbeat = 0,
    submitted_at   = COALESCE(submitted_at, now()),
    rand_rank      = {_RAND_RANK_EXPR},
    level          = $3,
    updated_at     = now()
WHERE id = $1 AND status = ANY($2)
RETURNING id, lease_seq, level, rand_rank
"""


async def enter_pending_pool(
    task_id: str,
    from_statuses: list[str],
    *,
    reset_submitted_at: bool = False,
    level: Optional[int] = None,
) -> bool:
    """把任务放进待领取池（PENDING），并让租约相关字段满足不变量。

    **任何**让任务进入 PENDING 的路径都该走这里：

        status = PENDING  ⟹  worker_id = '' 且 lease_until = NULL
        任何路径 → PENDING ⟹ lease_seq += 1

    ``lease_seq += 1`` 不是回收路径的特例：用户 retry / redo / revise 若清了 worker_id 却没 bump，
    同一台 worker 再次领到该任务时，**第一次运行残留的在途回调会匹配通过**并污染第二次运行——
    从用户 API 就能触达，不需要网络分区。

    ``submitted_at`` 默认 **COALESCE 保留原值**：重投的任务本就等得最久，保留原提交时刻让它排在
    队首是公平的。反复失败导致的"霸占队首"由 ``requeues`` 封顶兜住。

    Args:
        from_statuses:      允许的来源状态（乐观锁：WHERE 不命中即视为已被并发改走）
        reset_submitted_at: True=重置为当前时刻（用于 retry 这类"重新排队"语义）
        level:              档位快照。None 则经 ``UserResolver`` 按任务的 user_id 解析——
                            这是全流程里唯一一次读用户体系，放在进池这个低频路径上

    Returns:
        True=本次成功翻入 PENDING；False=状态已被并发改走
    """
    if level is None:
        owner = await Task.filter(id=task_id).values_list("user_id", flat=True)
        level = await get_user_resolver().level(owner[0]) if owner and owner[0] else 0

    sql = _ENTER_POOL_SQL
    if reset_submitted_at:
        sql = sql.replace("COALESCE(submitted_at, now())", "now()")
    rows = await _q(sql, [task_id, list(from_statuses), int(level)])
    return bool(rows)


# --------------------------------------------------------------------------
# auto_post 子任务的认领（子任务 DRAFT → PENDING）
# --------------------------------------------------------------------------

# 与 _ENTER_POOL_SQL 的关系：**同一件事，多三个 WHERE 谓词**。
#
# 为什么不直接调 enter_pending_pool：它的 WHERE 只有 ``id AND status=ANY()``，而这里的认领必须是
# ``DRAFT AND auto_post=1 AND is_delete=false`` 的原子 CAS——后两个谓词挡的是"准入循环与用户取消 / 删除并发"，
# 丢了就会把一个刚被取消的子任务重新投出去。
#
# 刻意差异：不写 level（快照点是随后提交时的进池）；submitted_at 用 now()（子任务是首次进池）。
#
# ⚠️ 成本：rand_rank 是对 tasks 的相关子查询 count(*)。准入是低频路径，接受；
#    但别把这个形状搬到认领热路径上去。
_CLAIM_CHILD_SQL = f"""
UPDATE tasks SET
    status         = 'pending',
    worker_id      = '',
    lease_until    = NULL,
    lease_seq      = lease_seq + 1,
    submitted_at   = now(),
    rand_rank      = {_RAND_RANK_EXPR},
    updated_at     = now()
WHERE id = $1 AND status = 'draft' AND auto_post = 1 AND is_delete = false
RETURNING id
"""


async def claim_auto_post_child(task_id: str) -> bool:
    """把待准入的子任务原子地翻进待领取池。

    CAS 条件 ``DRAFT + auto_post=1 + 未软删`` 三者缺一不可。``auto_post`` **刻意保留为 1**：
    调用方在提交成功后才清它——这样"认领了但提交没走完"的子任务不会被误判成已准入。

    Returns:
        True=本次成功认领；False=状态已被并发改走（取消 / 删除 / 已被认领）
    """
    return bool(await _q(_CLAIM_CHILD_SQL, [task_id]))


# --------------------------------------------------------------------------
# 续租（心跳）
# --------------------------------------------------------------------------

_RENEW_SQL = f"""
UPDATE tasks SET
    last_heartbeat = {NOW_MS},
    lease_until    = {NOW_MS} + $4 + ({_GRACE_MS}),
    updated_at     = now()
WHERE id = $5 AND worker_id = $6 AND lease_seq = $7
  AND status = ANY($8)
  -- lease_until 为空 = 已主动释放租约（多阶段中间步等用户追加参数）。令牌仍匹配，但不能被心跳“复活”：
  -- 复活后租约到期，回收循环会把这个正在等用户的任务当成失联 worker 回收掉并重投
  AND lease_until IS NOT NULL
RETURNING id, status, lease_until,
          (lease_until - {NOW_MS}) AS lease_remaining_ms
"""


async def renew_lease(task_id: str, worker_id: str, lease_seq: int) -> dict[str, Any] | None:
    """心跳续租（带 fencing）。返回续租后的行；被 fencing 拒绝时返回 None。

    **续租的载体是心跳端点**，这必须显式设计而非顺带：QUEUED 阶段根本没有心跳
    （worker 在任务开始执行时才注册心跳）——这正是认领短租约存在的理由。
    """
    rows = await _q(
        _RENEW_SQL,
        _grace_params() + [
            settings.LEASE_SECONDS * 1000, task_id, worker_id, lease_seq, ["queued", "running"],
        ],
    )
    return rows[0] if rows else None


# --------------------------------------------------------------------------
# 回收
# --------------------------------------------------------------------------

# 先失败、再回收：顺序不可颠倒。若先回收，requeues 刚被 +1 的任务会在同一轮里又被判超限，
# 等于阈值凭空少一次。
_FAIL_OVER_LIMIT_SQL = f"""
UPDATE tasks SET
    status        = 'failed',
    sub_status    = 'failed',
    worker_id     = '',
    lease_until   = NULL,
    lease_seq     = lease_seq + 1,
    error_message = '[Lease] 连续 ' || requeues::text
                    || ' 次被回收且未推进，判定为异常任务（最后持有者: '
                    || worker_id || '）',
    updated_at    = now()
WHERE status = ANY($1)
  AND lease_until IS NOT NULL
  AND lease_until < {NOW_MS}
  AND requeues >= $2
RETURNING id, worker_id, requeues
"""

# 回收：QUEUED 与 RUNNING 都收，但**只有 RUNNING 计入"连续无进展"**。
#
# 为什么必须分开数：``requeues`` 的语义是"反复重投却从不推进"，用来揪出毒丸任务。而从 QUEUED 回收意味着任务
# **根本没跑起来**（领了但没 ack / 没起跑），这不是任务的问题，最常见的成因是 **Web 不可达**：
# worker 领得到（领取服务与 DB 都好好的）却 ack 不了，一放弃就再领下一个。整池任务会被一起推向阈值，
# 十来分钟的 Web 停机就能把用户任务成批判成 failed——把"运维故障"变成"数据损失"。
#
# 用 CTE 先取旧状态：RETURNING 只能给出更新**后**的值（这里恒为 'pending'），拿不到"回收前是 queued 还是
# running"，而日志和计数都需要它。外层 WHERE 重复一遍条件是防并发续租在 CTE 与 UPDATE 之间把租约推远。
#
# 代价（明知且接受）：一个从未成功起跑过的毒丸任务不会再被计数封顶，会安静地循环重投。它不烧算力
# （没人真正跑起来过），且每轮都留一条 warning 日志可查。相比"外部依赖一抖就批量判死用户任务"，划算。
_RECLAIM_SQL = f"""
WITH victims AS (
    SELECT id, status AS old_status
      FROM tasks
     WHERE status = ANY($1)
       AND lease_until IS NOT NULL
       AND lease_until < {NOW_MS}
       FOR UPDATE
)
UPDATE tasks t SET
    status         = 'pending',
    worker_id      = '',
    lease_until    = NULL,
    lease_seq      = t.lease_seq + 1,
    requeues       = t.requeues + CASE WHEN v.old_status = 'running' THEN 1 ELSE 0 END,
    last_heartbeat = 0,
    updated_at     = now()
FROM victims v
WHERE t.id = v.id
  AND t.status = ANY($1)
  AND t.lease_until IS NOT NULL
  AND t.lease_until < {NOW_MS}
RETURNING t.id, t.task_type, t.requeues, v.old_status
"""


async def fail_over_limit() -> list[dict[str, Any]]:
    """把"连续无进展重投"超限的到期任务直接判失败。

    为什么需要它：按 ``last_heartbeat`` 多老来判放弃，而 ``last_heartbeat`` 每次被新 worker 接手运行都会刷新——
    对"跑一会儿、发过心跳、然后反复失败"的任务永远够不着阈值，形成无限重投。回收周期缩到分钟级之后，
    同一个缺陷传播快几十倍，故必须封顶。

    ``error_message`` 里写明连续次数与最后持有者：一批超限**分散在不同任务**上读作"租约尺寸设错了"，
    **集中在同一个任务**才读作"这个任务本身有问题"。
    """
    return await _q(_FAIL_OVER_LIMIT_SQL, [["queued", "running"], settings.LEASE_REQUEUE_LIMIT])


async def reclaim_expired() -> list[dict[str, Any]]:
    """回收所有到期租约 → 放回 PENDING 池。

    **判据只有一次时间戳比较，没有任何业务阈值**：判断"多久算死"的那部分已经搬进续租时的 grace 计算了，
    补偿器因此变蠢，这是有意的。``lease_until IS NOT NULL`` 同时让从未持有租约的行天然不被本循环触碰。
    """
    return await _q(_RECLAIM_SQL, [["queued", "running"]])


_RELEASE_SQL = """
UPDATE tasks SET
    status         = 'pending',
    worker_id      = '',
    lease_until    = NULL,
    lease_seq      = lease_seq + 1,
    last_heartbeat = 0,
    updated_at     = now()
WHERE id = $1 AND worker_id = $2 AND lease_seq = $3
  AND status = ANY($4)
  AND lease_until IS NOT NULL      -- 已释放租约的等待态任务没有租户可归还，别把它拉回池里
RETURNING id
"""


async def release_lease_fenced(task_id: str, worker_id: str, lease_seq: int) -> bool:
    """带 fencing 的主动归还——立刻把任务放回待领取池。

    与 ``reclaim_expired`` 的差别只在触发方式：那边是租约到期被动回收，这边是持有者自己说"我不干了"。
    **都不写 ``requeues``**——主动归还不是失败，不该消耗"连续无进展"的额度
    （否则一次滚动重启就能把长任务的额度耗掉一大截）。

    fencing 条件不可省：一台已丢租约、正在关闭的 worker 若无条件归还，会把**别人的**租约退掉。
    """
    return bool(await _q(_RELEASE_SQL, [task_id, worker_id, lease_seq, ["queued", "running"]]))


# --------------------------------------------------------------------------
# 认领热点索引
# --------------------------------------------------------------------------


async def ensure_claim_index() -> str:
    """确保认领热点索引存在（幂等），返回状态描述。

    为什么放在服务启动而不是只写进部署手册：这个索引**缺了不会报错，只会慢 400 倍**（20ms vs 0.05ms），
    而认领是全库最高频的写路径入口——一个只表现为"有点卡"的退化最容易被漏掉。
    ``dbsync --force`` 会把它当野索引清掉，每次启动重建一遍是最省心的兜底。

    ⚠️ 用的是普通 ``CREATE INDEX``（非 CONCURRENTLY），会短暂阻塞写。首次在大表上部署时建议先手工用
    CONCURRENTLY 建好，这里就会因 IF NOT EXISTS 直接跳过。
    """
    try:
        rows = await _q(
            "SELECT 1 FROM pg_indexes WHERE tablename='tasks' AND indexname=$1",
            [Task.CLAIM_INDEX_NAME],
        )
        if rows:
            return "已存在"
        await Tortoise.get_connection("default").execute_query(Task.CLAIM_INDEX_DDL)
        return "已创建"
    except Exception as e:  # noqa: BLE001
        return f"失败（认领会退化为全表扫描，务必人工处理）: {e}"


# --------------------------------------------------------------------------
# fencing
# --------------------------------------------------------------------------

# 令牌的**首选**通道是 header：内部写端点的请求体各不相同（多个走 DTO 校验），
# 走 header 则一个装饰器统一覆盖，且与既有的 X-Internal-Token 同一通道。
HEADER_WORKER_ID = "X-Worker-Id"
HEADER_LEASE_SEQ = "X-Lease-Seq"

# 同名的 body 字段——**与 header 完全等价的第二条通道**。
BODY_WORKER_ID = "worker_id"
BODY_LEASE_SEQ = "lease_seq"

# 拒绝原因：``rowcount = 0`` 同时代表三种完全不同的情况，光看它分不出来。
# 不区分就会把三者压成一桶，**从此失去对真正异常的告警能力**。
REASON_NOT_LESSEE = "not_lessee"        # 确定是过期租户（正常的分区恢复，不该告警）
REASON_GONE = "gone"                    # 任务已不存在 / 已软删（不该告警）
REASON_ALREADY_FINAL = "already_final"  # 同一租户但已达终态——**这是幂等成功，绝不是错误**
REASON_BAD_SEQ = "bad_lease_seq"        # 带了 worker_id 却没带 / 带坏 lease_seq——响亮拒绝


class FenceReject(Exception):
    """fencing 拒绝。携带可区分的 reason，供端点选择答复措辞。"""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


_TERMINAL = {"completed", "failed", "cancelled"}


async def check_fence(task_id: str, worker_id: str, lease_seq: int | None) -> None:
    """校验调用方是否仍是本任务的租户。通过则返回，否则抛 ``FenceReject``。

    ## 为什么要读一次行

    写操作本身是条件 UPDATE（互斥由它保证），这里读是**只为选措辞**：三种拒绝原因对运维的含义完全不同，
    尤其 ``already_final`` 是幂等成功，当成"你丢了租约"会污染日志、淹没真正的异常。
    读不引入 TOCTOU，因为它不是判据——判据仍是那条 UPDATE 的 rowcount。

    ## 强制 fencing

    行上没有租户（任务回池后 worker_id 被清空）时，调用方出示的令牌必然是**过期租户**的——拒绝。
    曾经"行上无租户就放行"漏过这种情形：任务回池后，上一轮 worker 的在途回调会被放行，把上一轮的产物地址
    写进用户刚推进的这一轮。
    """
    if worker_id and lease_seq is None:
        raise FenceReject(REASON_BAD_SEQ, f"worker_id={worker_id} 但 lease_seq 缺失或非法")

    rows = await _q(
        "SELECT status, worker_id, lease_seq, is_delete FROM tasks WHERE id = $1", [task_id]
    )
    if not rows:
        raise FenceReject(REASON_GONE, "task not found")
    row = rows[0]
    if row["is_delete"]:
        raise FenceReject(REASON_GONE, "task deleted")

    holder = row["worker_id"] or ""
    if not holder:
        raise FenceReject(
            REASON_NOT_LESSEE, f"task has no lessee now (caller {worker_id}/{lease_seq})"
        )

    if holder == worker_id and row["lease_seq"] == lease_seq:
        return  # 正是当前租户

    if holder == worker_id and str(row["status"]) in _TERMINAL:
        # 同一台机器、任务已被自己推到终态：回调重投 / 超时后其实成功了。**这是幂等成功**
        raise FenceReject(REASON_ALREADY_FINAL, f"status={row['status']}")

    raise FenceReject(
        REASON_NOT_LESSEE,
        f"holder={holder} seq={row['lease_seq']} (caller {worker_id}/{lease_seq})",
    )


def extract_fencing(request) -> tuple[str, int | None]:
    """从请求里取出 fencing 令牌——**全服务端唯一的提取入口**。

    返回 ``(worker_id, lease_seq)``；取不到时是 ``("", None)``，即"调用方无令牌"。

    ## 三级回退：header → JSON body → form

    | 通道 | 谁在用 | 说明 |
    |---|---|---|
    | ``X-Worker-Id`` / ``X-Lease-Seq`` | 任务子进程、worker 前台回调 | 首选 |
    | body 的 ``worker_id`` / ``lease_seq`` | **令牌来自持久化条目的异步通道** | 上传进程、失败上报重放 |
    | form 的同名字段 | multipart 请求 | 那条路上没有 JSON body 可用 |

    header 那条钩子是从**父进程内存的活注册表**取令牌的，天然只覆盖"Lane 生命周期之内"；两条异步通道恰恰活在
    生命周期之外，它们持有的令牌来自**落盘条目里的快照**（"这份产物是哪一次运行产生的"），是随数据走的值，
    不是随进程走的状态——放进 body 与它的来源天然同源。

    两者都带且不一致时**以 header 为准**：理由不是谁更可信，而是要有确定的答案。

    ⚠️ 解析一律不抛异常：body 可能是 multipart、可能是空、可能压根不是 JSON。
    取不到就是"无令牌"，让 ``check_fence`` 去判，绝不能让守卫本身 500。
    """
    worker_id = (request.headers.get(HEADER_WORKER_ID) or "").strip()
    raw_seq = request.headers.get(HEADER_LEASE_SEQ)

    if not worker_id or raw_seq is None:
        for source in (_body_json(request), _body_form(request)):
            if not source:
                continue
            if not worker_id:
                worker_id = str(source.get(BODY_WORKER_ID) or "").strip()
            if raw_seq is None and source.get(BODY_LEASE_SEQ) is not None:
                raw_seq = source.get(BODY_LEASE_SEQ)
            if worker_id and raw_seq is not None:
                break

    try:
        lease_seq = int(raw_seq) if raw_seq is not None else None
    except (TypeError, ValueError):
        lease_seq = None  # 令牌畸形 = 没有令牌
    # 列是 integer：超出 int32 的值会让 SQL 抛 out of range（500）。代次从 0 起单调递增，越界必为伪造 / 畸形
    if lease_seq is not None and not (0 <= lease_seq <= 2**31 - 1):
        lease_seq = None

    return worker_id, lease_seq


def _body_json(request) -> dict | None:
    try:
        body = request.json
    except Exception:  # noqa: BLE001
        return None
    return body if isinstance(body, dict) else None


def _body_form(request) -> dict | None:
    """安全地取 form 字段（multipart / urlencoded）。Sanic 的 form 是 multi-dict，取值需要 ``.get()`` 拿首项。"""
    try:
        form = request.form
    except Exception:  # noqa: BLE001
        return None
    if not form:
        return None
    return {k: form.get(k) for k in (BODY_WORKER_ID, BODY_LEASE_SEQ) if k in form}


def fence_lease(f):
    """内部写端点的 fencing 守卫。

    ## 为什么拒绝时回 **403**

    403 本来就是"业务 / 安全拒绝"，客户端不应重试，也不应触发全局鉴权告警（只有 401 才是鉴权故障）。
    worker 的持久重试队列区分"永不放弃"与"永久拒绝"的依据是响应体里的 ``reason``，不是状态码。

    心跳端点是唯一的例外：它要回 200 + 非 running 的 status，让 worker 现有的
    ``status != "running"`` 判据触发取消，见 controller 里的心跳实现。
    """

    @wraps(f)
    async def wrapper(request, *args, **kwargs):
        task_id = kwargs.get("task_id") or (args[0] if args else None)
        worker_id, lease_seq = extract_fencing(request)
        if task_id:
            try:
                await check_fence(task_id, worker_id, lease_seq)
            except FenceReject as e:
                logger.info("[fence] 拒绝 task_id=%s reason=%s %s", task_id, e.reason, e.detail)
                return _json({"ok": False, "reason": e.reason}, status=403)
        return await f(request, *args, **kwargs)

    return wrapper
