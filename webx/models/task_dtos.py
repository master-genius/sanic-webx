"""任务接口的请求 / 响应模型（pydantic）。

``params_text`` 对外既收 JSON 对象也收 JSON 字符串——前端直接发对象最自然，库里存的恒是 JSON 文本。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _jsonish(v: Any) -> Any:
    """把 dict / list 规整成 JSON 文本；字符串原样；其余拒绝。"""
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    raise ValueError("必须是 JSON 对象或 JSON 字符串")


# ==================== 用户接口：请求 ====================


class TaskCreateRequest(BaseModel):
    task_type: str = Field(min_length=1, max_length=64)
    name: str = Field(default="", max_length=128)
    params_text: Optional[Any] = None
    files: Optional[list[dict]] = None
    #: true = 创建后立即提交（走完整的提交前置闸）
    submit: bool = False

    @field_validator("params_text", mode="before")
    @classmethod
    def _params(cls, v):
        return _jsonish(v)


class TaskPatchRequest(BaseModel):
    """编辑 / 推进任务。

    - 草稿态：改 ``params_text`` / ``step``，``submit=true``（默认）则同时提交
    - 多阶段、等用户追加参数 / 草稿窗口：必须带 ``step``；``submit=false`` 只存草稿
    """

    params_text: Optional[Any] = None
    step: Optional[str] = Field(default=None, max_length=30)
    submit: bool = True

    @field_validator("params_text", mode="before")
    @classmethod
    def _params(cls, v):
        return _jsonish(v)


class TaskInfoUpdateRequest(BaseModel):
    """与计算无关的辅助信息；不判断任务状态，任意状态均可改。"""

    name: Optional[str] = Field(default=None, max_length=128)


class TaskSubmitRequest(BaseModel):
    #: 提交时整体覆盖参数（非合并）；不带则沿用已有参数
    params_text: Optional[Any] = None

    @field_validator("params_text", mode="before")
    @classmethod
    def _params(cls, v):
        return _jsonish(v)


class TaskRetryRequest(BaseModel):
    params_text: Optional[Any] = None
    step: Optional[str] = Field(default=None, max_length=30)

    @field_validator("params_text", mode="before")
    @classmethod
    def _params(cls, v):
        return _jsonish(v)


class TaskRedoRequest(BaseModel):
    step: str = Field(min_length=1, max_length=30)
    params_text: Optional[Any] = None

    @field_validator("params_text", mode="before")
    @classmethod
    def _params(cls, v):
        return _jsonish(v)


# ==================== 内部接口：请求 ====================


class TaskStatusUpdateRequest(BaseModel):
    status: str
    error_message: str = ""


class StepStatusUpdateRequest(BaseModel):
    steps: Optional[list[str]] = None
    sub_step: Optional[str] = Field(default=None, max_length=30)
    sub_status: Optional[str] = None
    step_message: Optional[str] = None


class TaskFieldUpdateRequest(BaseModel):
    """result_path / log_path 既收 JSON 对象也收字符串；库里存 JSON 文本。"""

    result_path: Optional[Any] = None
    log_path: Optional[Any] = None

    @field_validator("result_path", "log_path", mode="before")
    @classmethod
    def _jsonish_fields(cls, v):
        return _jsonish(v)


class TaskFailRequest(BaseModel):
    error_message: str = "Unknown error"


# ==================== 响应 ====================


class TaskResponse(BaseModel):
    """任务详情。刻意不含租约相关列（worker_id / lease_*）和 user_id / org_id。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    status: str
    task_type: str
    name: str = ""
    parent_id: str = ""
    tag: str = ""
    parent_step: str = ""
    retry: int = 0
    allowed_retries: int = 0
    timeout_seconds: int = 0
    steps: list[str] = Field(default_factory=list)
    sub_step: str = ""
    end_step: str = ""
    sub_status: str = "pending"
    step_message: str = ""
    files_json: str = "[]"
    params_text: str = "{}"
    result_path: str = ""
    log_path: str = ""
    error_message: str = ""
    submitted_at: Optional[datetime] = None
    start_time: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator("steps", mode="before")
    @classmethod
    def _steps(cls, v):
        if isinstance(v, str):
            try:
                v = json.loads(v or "[]")
            except json.JSONDecodeError:
                return []
        return v or []

    @field_validator("status", "sub_status", mode="before")
    @classmethod
    def _enum_value(cls, v):
        return getattr(v, "value", v)


class TaskStatusBrief(BaseModel):
    """轻量轮询用的状态精简视图。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    status: str
    name: str = ""
    steps: list[str] = Field(default_factory=list)
    sub_step: str = ""
    sub_status: str = "pending"
    step_message: str = ""
    error_message: str = ""

    _steps = field_validator("steps", mode="before")(TaskResponse._steps.__func__)
    _enum = field_validator("status", "sub_status", mode="before")(TaskResponse._enum_value.__func__)


class TaskListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    page_size: int
