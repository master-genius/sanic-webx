"""TaskKindPlugin —— 把「与具体任务类型有关的规则」收口成接缝。

worker 的执行机制（领取、租约、fencing、心跳、子进程、上报、恢复）是通用的；
但总有几处行为取决于「这是哪一类任务」。这些点不写死在机制里，而是以插件钩子的形式
暴露，默认实现全部是中性的。需要定制的部署在 channel 配置文件里提供一个子类实例：

    # channel_config/default.py
    from webx_worker.plugin import TaskKindPlugin

    class MyPlugin(TaskKindPlugin):
        def soft_clean_policy(self, task_type):
            return frozenset({"cache"}) if task_type == "heavy" else None

    PLUGIN = MyPlugin()

四个接缝：

  · log_target       日志的 log_path key 与上传文件名（多步任务可按步骤编号）
  · select_inputs    哪些输入文件条目需要从服务端下载
  · extend_env       向子进程环境追加变量（fencing 令牌之后注入，不可覆盖它）
  · soft_clean_policy 崩溃目录的软清理：保留哪些昂贵产物（recovery 用）

另有 `redactors`：面向前端可见日志的脱敏函数注册表（见 log_writer）。
"""

from __future__ import annotations

from typing import Callable


class TaskKindPlugin:
    """默认插件：所有钩子都是中性实现。子类按需覆盖。"""

    #: 日志脱敏函数列表：`Callable[[str], str]`，对整段日志文本做变换，
    #: 在内置的「内部路径/URL/本机路径」脱敏之前依次执行。
    #: 典型用途：剔除某个外部程序的启动横幅、屏蔽业务内部标记行。
    redactors: list[Callable[[str], str]]

    def __init__(self) -> None:
        self.redactors = []

    # ── 日志 key ─────────────────────────────────────────────────────────
    def log_target(
        self, task_type: str, sub_step: str, steps_done: list
    ) -> tuple[str, str]:
        """计算日志的 `log_path` key 与上传文件名 `(key, filename)`。

        默认：
          · 单阶段（sub_step 为空）：("log", "run.log")
          · 多步任务：以步骤名为 key，`{步骤名}.log` 为文件名

        需要「同一步可重复出现」的任务（如重复的平衡阶段）应覆盖本方法给 key 加序号，
        使 log_path key / 上传文件名 / 结果 key 严格 1:1，避免重复阶段互相覆盖。
        ⚠️ 口径一旦与服务端的裁剪规则漂移，重置时会把当前步的活日志当孤儿删掉。
        """
        if not sub_step:
            return "log", "run.log"
        return sub_step, f"{sub_step}.log"

    # ── 输入文件 ─────────────────────────────────────────────────────────
    def select_inputs(self, task_type: str, files: list[dict]) -> list[dict]:
        """从任务的输入文件清单里挑出需要下载的条目（默认：名字非空的全部）。

        规格驱动、不对应实际文件的条目（只靠 data 字段携带信息）应在这里被滤掉。
        """
        return [f for f in files if isinstance(f, dict) and f.get("name")]

    # ── 子进程环境 ────────────────────────────────────────────────────────
    def extend_env(self, task_type: str, task_id: str, env: dict[str, str]) -> dict[str, str]:
        """返回要追加到子进程环境的变量（默认无）。

        在 channel `run_env` 之后、fencing 令牌之前注入——令牌最后写入，
        插件无法（也不应）覆盖它。
        """
        return {}

    # ── 崩溃目录软清理（recovery）────────────────────────────────────────
    def soft_clean_policy(self, task_type: str) -> frozenset[str] | None:
        """崩溃目录的清理策略。

        返回 None（默认）→ 整个任务目录 rmtree，由补偿器重投后从零重跑；
        返回名字集合 → 只保留任务目录下这些名字的条目，其余删除
        （用于保留重算代价高昂的中间产物，让下次执行复用）。
        """
        return None
