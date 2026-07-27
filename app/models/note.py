"""示例模型：笔记。"""

from tortoise import fields
from tortoise.models import Model

from webx.lib.genid import gen_id


class Note(Model):
    id = fields.CharField(pk=True, max_length=16, default=gen_id)
    user_id = fields.CharField(max_length=16, default="")
    title = fields.CharField(max_length=200, default="")
    content = fields.TextField(default="")
    create_time = fields.BigIntField(default=0)

    class Meta:
        table = "note"
        indexes = (("user_id",), ("create_time",))
