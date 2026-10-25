"""国际化（i18n）。

与原项目的 dataclass 写法相比改为**注册表**：框架只内置一小撮通用文案
（成功/失败/未授权/禁止访问/参数错误…），业务文案由项目自己 `register()` 注入，
不需要改动框架文件。语种也不写死中英——`register()` 传什么语种就支持什么语种。

业务项目 app/i18n.py::

    from webx.i18n import register

    register("zh", {
        "order_created": "订单已创建",
        "order_not_found": "订单不存在",
    })
    register("en", {
        "order_created": "Order created",
        "order_not_found": "Order not found",
    })

取值：``t("order_created", lang)``；缺失语种回落到 ``DEFAULT_LANG``，
仍缺失则原样返回 key（绝不抛异常——文案缺失不该让接口 500）。
"""

from __future__ import annotations

# 默认语种：当请求语种无对应文案时回落到此
DEFAULT_LANG = "zh"

# 文案注册表：lang -> {key: template}
_MESSAGES: dict[str, dict[str, str]] = {
    "zh": {
        "success": "操作成功",
        "error": "操作失败",
        "not_found": "资源不存在",
        "unauthorized": "未授权访问",
        "forbidden": "禁止访问",
        "invalid_params": "参数校验失败",
        "internal_error": "服务器内部错误",
        "token_expired": "登录已过期，请重新登录",
        "token_invalid": "非法的登录凭证",
        "too_many_requests": "请求过于频繁，请稍后再试",
        "db_error": "数据库操作失败",
        # 用户体系
        "user_not_found": "用户不存在",
        "old_password_incorrect": "当前密码不正确",
        "username_taken": "用户名已存在",
        "account_forbidden": "账户已被禁用",
        "account_locked": "账户已被锁定，请稍后再试",
        # 任务
        "task_not_found": "任务不存在",
        "task_status_invalid": "非法的任务状态",
        "task_state_conflict": "任务状态不一致，请勿重复操作",
        "children_not_ready": "子任务尚未全部完成",
        "quota_exceeded": "超出配额限制（{scope}：当前 {current}，上限 {limit}）",
        "invalid_step_name": "步骤名 {step} 不属于任务类型 {task_type}（可选：{allowed}）",
        "step_not_allowed": "不能从 {current} 推进到 {requested}（允许：{allowed}）",
        "max_steps_reached": "步骤数已达上限 {max}",
        "unknown_task_type": "未登记的任务类型：{task_type}",
    },
    "en": {
        "success": "Success",
        "error": "Operation failed",
        "not_found": "Resource not found",
        "unauthorized": "Unauthorized",
        "forbidden": "Forbidden",
        "invalid_params": "Invalid parameters",
        "internal_error": "Internal server error",
        "token_expired": "Login expired, please sign in again",
        "token_invalid": "Invalid token",
        "too_many_requests": "Too many requests, please retry later",
        "db_error": "Database operation failed",
        # 用户体系
        "user_not_found": "User not found",
        "old_password_incorrect": "Current password is incorrect",
        "username_taken": "Username already taken",
        "account_forbidden": "Account disabled",
        "account_locked": "Account locked, please retry later",
        # tasks
        "task_not_found": "Task not found",
        "task_status_invalid": "Invalid task status",
        "task_state_conflict": "Task state changed concurrently, do not repeat the action",
        "children_not_ready": "Child tasks are not all finished yet",
        "quota_exceeded": "Quota exceeded ({scope}: current {current}, limit {limit})",
        "invalid_step_name": "Step {step} does not belong to task type {task_type} (allowed: {allowed})",
        "step_not_allowed": "Cannot advance from {current} to {requested} (allowed: {allowed})",
        "max_steps_reached": "Step count reached the limit {max}",
        "unknown_task_type": "Unregistered task type: {task_type}",
    },
}


def register(lang: str, messages: dict[str, str], override: bool = True) -> None:
    """注册/合并某语种的文案。

    Args:
        lang: 语种代码（如 "zh" / "en" / "ja"）
        messages: key -> 模板串，模板可含 ``{name}`` 占位由 t() 的 kwargs 填充
        override: 同名 key 是否覆盖已有文案（默认覆盖，便于业务改写框架文案）
    """
    bucket = _MESSAGES.setdefault(lang, {})
    for key, value in messages.items():
        if override or key not in bucket:
            bucket[key] = value


def supported_langs() -> list[str]:
    """当前已注册的语种列表。"""
    return list(_MESSAGES.keys())


def get_lang_from_header(headers) -> str:
    """从请求头解析语种偏好。

    优先级：``X-Lang``（前端显式指定）→ ``Accept-Language`` → ``DEFAULT_LANG``。
    解析出的主语言码（如 ``en-US`` → ``en``）若未注册，回落 ``DEFAULT_LANG``。
    """
    x_lang = headers.get("X-Lang") if headers else None
    if x_lang:
        code = x_lang.strip().split("-")[0].lower()
        return code if code in _MESSAGES else DEFAULT_LANG

    accept = (headers.get("Accept-Language", "") if headers else "") or ""
    if accept:
        # 形如 "en-US,en;q=0.9,zh-CN;q=0.8"，取最前一项的主语言码
        primary = accept.split(",")[0].strip().split(";")[0]
        code = primary.split("-")[0].lower()
        if code in _MESSAGES:
            return code

    return DEFAULT_LANG


def t(key: str, lang: str = DEFAULT_LANG, **kwargs) -> str:
    """翻译。

    回落链：``lang`` → ``DEFAULT_LANG`` → key 原样返回。
    格式化失败（占位符对不上）时返回未格式化的模板，绝不抛异常。
    """
    template = _MESSAGES.get(lang, {}).get(key)
    if template is None:
        template = _MESSAGES.get(DEFAULT_LANG, {}).get(key, key)
    if not kwargs:
        return template
    try:
        return template.format(**kwargs)
    except (KeyError, IndexError, ValueError):
        return template
