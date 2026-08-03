"""业务文案注册。框架文案已内置，这里只加自己的。"""

from webx.i18n import register

register("zh", {
    "note_created": "笔记已创建",
    "note_not_found": "笔记不存在",
})
register("en", {
    "note_created": "Note created",
    "note_not_found": "Note not found",
})
