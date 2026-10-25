"""任务算力点流水模型。

以 (task_id, sub_step) 为粒度记录单次步骤消耗的算力点，供按任务 / 按用户聚合统计与计费使用。

**现状：没有任何写入者。** 表结构先就位，写入语义（累加 / 去重 / 与余额的关系）等真实计费设计时再定，
见 ``webx.lib.task_pricing`` 的说明。

task_id / user_id 均为弱引用（不建外键），避免跨表强约束带来的删除级联与写入耦合。
"""

from tortoise import fields
from tortoise.models import Model

from webx.lib.genid import gen_id


class TaskComputePoints(Model):
    """任务算力点流水表。"""

    id = fields.CharField(max_length=16, pk=True, default=gen_id, description="记录 ID")
    task_id = fields.CharField(max_length=16, default="", index=True, description="所属任务 ID（弱引用，非外键）")
    user_id = fields.CharField(max_length=18, default="", index=True, description="所属用户 ID（弱引用，非外键）")
    sub_step = fields.CharField(max_length=30, default="", description="该笔算力点归属的步骤名")
    compute_points = fields.IntField(default=0, description="算力点消耗")
    created_at = fields.DatetimeField(auto_now_add=True, description="创建时间")
    updated_at = fields.DatetimeField(auto_now=True, description="更新时间")

    class Meta:
        table = "task_compute_points"
        table_description = "任务算力点流水表"
