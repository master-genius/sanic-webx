"""任务认领——全系统互斥性的唯一来源。

## 互斥性从哪来

来自**单条带条件的 UPDATE**，不是来自"只有一个进程"，也不是来自 ``SKIP LOCKED``：

  · ``UPDATE``      —— 一行要么进 RETURNING，要么没有。N 个并发请求里恰好一个拿到。**这就是互斥**，
                       与事务边界、与进程数都无关。
  · ``SKIP LOCKED`` —— 让 B 的 SELECT 不去等 A 的事务提交，直接跳过被锁的行取下一个。
                       **这是防串行化，不是防冲突**；去掉它仍然正确，只是吞吐塌掉。

由此也可以看出：把多个 Lane 的认领包进同一个事务**不是正确性所需**，只是让"响应里写的"与"DB 里存的"
一次对齐。因此"两个空槽只领到一个"是合法结果，不需要回滚。

## 为什么不做扁平的 LIMIT N

worker 的 Lane 是**串行**的（一条 Lane 同时最多跑一个任务），且并发扩展轴是多进程而非多槽。
若把多个 Lane 的类型合并再 ``LIMIT N``，可能取回"两个重型任务"这种 worker 装不下的组合——
第二个被认领却跑不了，要么白占租约挡住别人，要么得退回。故：**按 Lane 分组，每组恒 LIMIT 1**，
语句因此塌回最简形态。
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

from webx.config import settings
from webx.services.lease import NOW_MS, _q

logger = logging.getLogger(__name__)

# 认领排序键——四级，缺一不可：
#
#   ① level DESC          档位高的先跑（进池时从用户体系快照到任务行）
#   ② compute_points DESC 付了费的先跑（现阶段恒 0，等价于不生效；字段先就位，
#                         免得将来为加一个排序键去改热点索引）
#   ③ rand_rank ASC       fair-share 打散，避免同档位内按提交顺序扎堆
#   ④ submitted_at ASC    最后的确定性兜底（rand_rank 相同才用得上）
#
# ⚠️ ③ 排在 ④ 之前 ⇒ **同档位内不是 FIFO**。这是刻意的，但要知道代价：持续有新任务进来时老任务会被反复超越，
#    存在饥饿风险。
_ORDER_BY = "level DESC, compute_points DESC, rand_rank ASC, submitted_at ASC"

# 回包列清单——**不用 ``RETURNING *``**，两个理由：
#   ① ``*`` 会带回 datetime 列（created_at / updated_at / start_time / submitted_at），
#      它们不是 JSON 可序列化的，端点会在返回时直接抛异常；
#   ② ``*`` 把 user_id / org_id / error_message 等内部列一并发给 worker——worker 不需要也不该看到，
#      且将来加列会无声地改变回包形状。
# 本清单与协议文档（docs/worker-protocol.md §3.1）一一对应。
RETURN_COLS = (
    "id, name, task_type, files_json, params_text, "
    "allowed_retries, steps, sub_step, end_step, timeout_seconds, "
    "lease_seq, worker_id, status, level, compute_points"
)


def build_claim_sql(with_max_level: bool, with_max_points: bool) -> str:
    """按筛选形态拼装认领语句。两个筛选都是**范围条件**（``<=``），都可选。

    ## 为什么用 ``level <= N`` 而不是 ``level = ANY(集合)``

    实测各形态（``= ANY`` / ``IN`` / ``OR`` / 标量）耗时无差异：索引里根本不含 ``task_type``，
    它只是边扫边过滤的条件。而 ``<=`` 对应索引中一段**连续且已按 ORDER BY 排好序**的区间。

    ## 筛选与默认方式的兼容性

    筛选**只缩小候选集**，不改变互斥语义——互斥恒由那条带条件的 UPDATE 保证。"指定筛选的 worker"与
    "不指定的"可同时在线抢同一个池子而不冲突：同一行仍然只有一个能拿到。

    动态拼装而非 ``($n IS NULL OR ...)`` 的万能式：后者会让规划器无法在准备阶段确定选择性，容易退化成
    全表扫描。分支是有限的常量字符串，值一律走占位符。
    """
    where = ["status = 'pending'", "task_type = ANY($3)", "is_delete = false"]
    n = 3
    if with_max_level:
        n += 1
        where.append(f"level <= ${n}")
    if with_max_points:
        n += 1
        where.append(f"compute_points <= ${n}")

    return f"""
UPDATE tasks SET
    status         = 'queued',
    worker_id      = $1,
    lease_seq      = lease_seq + 1,
    lease_until    = {NOW_MS} + $2,
    last_heartbeat = {NOW_MS},
    -- 每次认领都是新的一轮：start_time 清空，grace 才从下限起算。留着上一轮（可能是几十小时前）的值，
    -- 重领后 ack 到上报 running 之间的租约会长达数小时，期间 worker 崩溃要等那么久才回收
    start_time     = NULL,
    updated_at     = now()
WHERE id = (
    SELECT id FROM tasks
     WHERE {' AND '.join(where)}
     ORDER BY {_ORDER_BY}
     FOR UPDATE SKIP LOCKED
     LIMIT 1
)
RETURNING {RETURN_COLS}
"""


async def claim_one(
    worker_id: str,
    task_types: Sequence[str],
    *,
    max_level: int | None = None,
    max_compute_points: int | None = None,
) -> dict[str, Any] | None:
    """为一个空闲 Lane 认领至多一个任务。

    Args:
        worker_id:          认领方的实例标识（写入归属字段，同时是 fencing 令牌的前半）
        task_types:         该 Lane 能跑的任务类型；空序列直接返回 None（不发 SQL）
        max_level:          只领档位 ≤ 此值的任务；None=不限。传 0 即只领免费档
        max_compute_points: 只领算力点 ≤ 此值的任务；None=不限。与 max_level 配合即"给免费任务配专用机器"

    Returns:
        认领到的任务行（dict）；该组无可领任务时返回 None。``RETURNING`` 就是唯一凭据——返回即认领成功。
    """
    types = [t for t in dict.fromkeys(task_types) if t]  # 去重且保序
    if not types:
        return None

    sql = build_claim_sql(max_level is not None, max_compute_points is not None)
    params: list[Any] = [worker_id, settings.LEASE_CLAIM_SECONDS * 1000, types]
    if max_level is not None:
        params.append(int(max_level))
    if max_compute_points is not None:
        params.append(int(max_compute_points))

    rows = await _q(sql, params)
    if not rows:
        return None
    row = rows[0]
    logger.info(
        "[claim] %s ← %s (type=%s level=%s points=%s lease_seq=%s)",
        worker_id, row.get("id"), row.get("task_type"),
        row.get("level"), row.get("compute_points"), row.get("lease_seq"),
    )
    return row


async def claim_for_slots(
    worker_id: str,
    slots: Sequence[Sequence[str]],
    *,
    max_level: int | None = None,
    max_compute_points: int | None = None,
) -> list[dict[str, Any]]:
    """按空闲槽位逐个认领。

    ``slots`` 的每个元素是一个空闲 Lane 能跑的类型列表，元素个数 = 空闲 Lane 数。**没有 ``n`` 参数**：
    一条 Lane 恒等于一个槽位。``max_level`` / ``max_compute_points`` 是**整台 worker 的属性**
    （这台机器服务谁），不是每个槽位各自的，故放在这一层。

    返回的任务数 ≤ len(slots)。**只领到一个也照常返回**——若非要等凑齐，池里只有一个任务时，
    两个空槽的 worker 会一直挂着不动。

    ⚠️ 顺序即优先级：靠前的槽位先取。worker 应把更稀缺 / 更重的 Lane 放前面。
    """
    claimed: list[dict[str, Any]] = []
    for types in slots:
        try:
            row = await claim_one(
                worker_id, types, max_level=max_level, max_compute_points=max_compute_points
            )
        except Exception:
            if not claimed:
                raise
            # 前面的槽位已经认领成功（行已翻成 QUEUED）：此时抛错会让这些任务无人知晓，只能等短租约到期回池。
            # 把已认领的先返回，出错的槽位下一轮再领
            logger.exception("[claim] 槽位认领出错，先返回已认领的 %d 个", len(claimed))
            break
        if row is not None:
            claimed.append(row)
    return claimed
