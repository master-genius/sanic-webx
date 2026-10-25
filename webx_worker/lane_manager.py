"""LaneManager —— Lane 划分与类型映射。

## Lane 是**槽位**，不是队列

一条 Lane 同时最多跑一个任务（跑完才领下一个），所以这里没有任何队列。
没有队列也就没有「队列满了 `await put()` 卡住分发器」的**跨 Lane 队头阻塞**。

## 并发轴是进程，不是 Lane

多条 Lane 是**设计意图**（把长任务与短任务隔开，互不阻塞），
不是「一个可以调大的并发度」。要提高并发就多起 worker 进程，
**不要**改成「同类型多槽」—— 那会让 Lane 退回队列语义，把队头阻塞带回来。

## 空 LANES = 一条 Lane 装下所有已声明的 channel

`LANES` 是可选配置。不配置（或配成空数组）时，**所有已声明的 channel（task_type）
归入同一条 Lane**（单 Lane 串行），而不是「没有任何类型可领」。

历史缺陷（已修）：旧实现只按 `lanes` 构造映射，`lanes` 为空时 `task_to_lane` 为空，
领取循环据此得到空的能力声明，于是每 5 秒空转一次、永远领不到任务——
而默认配置里 LANES 恰好是注释掉的。文档承诺的是「退化为单 Lane 串行」，实现没兑现。
"""

import logging
from typing import Iterable

logger = logging.getLogger(__name__)


class LaneManager:
    """Lane 划分：把 task_type 映射到 Lane 编号。

    映射本身就是 worker 的**能力声明** —— LeaseDispatcher 据此告诉领取服务
    「我这个空槽能跑哪些类型」。服务端不维护任何能力注册表：
    能力的权威在 worker，随请求带过去即可，存一份在服务端只会变成过期镜像。
    """

    def __init__(self, lanes: list[list[str]], channel_types: Iterable[str] = ()):
        """
        Args:
            lanes: Lane 分组配置，如 [["md", "ff"], ["image", "docking"]]
                   空数组 = 单 Lane 模式：所有 channel_types 归入一条 Lane
            channel_types: 已声明的 channel（task_type）名，即 CHANNELS 的键。
                   仅在 lanes 为空时用来构造那条唯一的 Lane，并用于配置一致性告警。
        """
        declared = list(dict.fromkeys(channel_types))   # 去重并保持声明顺序

        if not lanes and declared:
            # ★ 空 LANES 修复：所有已声明的类型归入一条 Lane
            lanes = [declared]

        self.num_lanes = max(len(lanes), 1)
        self.task_to_lane: dict[str, int] = {}

        for i, lane_tasks in enumerate(lanes):
            for task_type in lane_tasks:
                self.task_to_lane[task_type] = i

        # 一致性告警（只说不改）：已声明却没进任何 Lane 的 channel 永远领不到任务，
        # 属于配置遗漏，悄悄吞掉比报错更难排查。
        uncovered = [t for t in declared if t not in self.task_to_lane]
        if uncovered:
            logger.warning(
                "[LaneManager] 以下 channel 已声明但不在任何 Lane 里，将不会领取这些类型的任务: %s",
                uncovered,
            )

        logger.info(
            f"[LaneManager] 初始化: {self.num_lanes} 个 Lane（每 Lane 一个槽位）, "
            f"映射={self.task_to_lane}"
        )

    def get_lane(self, task_type: str) -> int:
        """取该任务类型所属的 Lane 编号；未配置的类型归 Lane 0。"""
        return self.task_to_lane.get(task_type, 0)

    def types_of(self, lane_id: int) -> list[str]:
        """该 Lane 能跑的全部任务类型（LeaseDispatcher 构造领取请求时用）。"""
        return [t for t, lid in self.task_to_lane.items() if lid == lane_id]
