"""任务结果 / 日志字段的纯逻辑：深度合并、按步骤裁剪、步骤令牌解析。零 ORM。

``result_path`` / ``log_path`` 是 TEXT 列，约定存**嵌套 JSON**，顶层 key 是步骤名：

    {"prepare": {"url": "/public/tasks/T/results/prepare.zip"}, "render": {"url": "..."}}

这样不同步骤的产物互不覆盖，且可以按步骤作废。任务脚本经 ``PATCH .../fields?mode=merge`` 逐步写入。
"""

from __future__ import annotations

import json
from typing import Any, Optional

from webx.lib.task_types import get_stage_config, get_task_type


def _to_dict(raw: str) -> dict:
    """把字段值解析成 dict；空 → {}；非 JSON 对象的纯字符串（如单个 URL）包成 ``{"default": raw}``。"""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {"default": raw}
    return parsed if isinstance(parsed, dict) else {"default": raw}


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for key, val in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(val, dict):
            out[key] = _deep_merge(out[key], val)
        else:
            out[key] = val
    return out


def merge_field_path(existing: str, new_value: str) -> str:
    """合并 result_path / log_path，支持结构化数据的深度合并。

    规则：key 不存在 → 添加；两边都是 dict → 递归合并；否则新值覆盖（new wins）。
    """
    merged = _deep_merge(_to_dict(existing), _to_dict(new_value))
    return json.dumps(merged, ensure_ascii=False) if merged else ""


def collect_urls(value: Any) -> list[str]:
    """收集嵌套结构里所有字符串叶子里**像地址**的值（以 ``/`` 或 ``http`` 开头）。用于找出孤儿产物。"""
    out: list[str] = []
    if isinstance(value, str):
        if value.startswith(("/", "http://", "https://")):
            out.append(value)
    elif isinstance(value, dict):
        for v in value.values():
            out.extend(collect_urls(v))
    elif isinstance(value, list):
        for v in value:
            out.extend(collect_urls(v))
    return out


def _prune_by_steps(task_type: str, raw: str, kept_steps: list[str]) -> tuple[str, list[str]]:
    """丢掉"属于本类型步骤、但不在保留轨迹里"的顶层 key，返回 ``(新值, 被丢弃的地址)``。

    **只动认得出来的步骤 key**：不是该类型步骤名的 key（如共享的汇总产物）原样保留——
    框架不知道它们归谁，误删比残留代价大。单阶段任务没有步骤，原样返回。
    """
    config = get_stage_config(task_type)
    if not raw or not config.is_multi_stage():
        return raw, []
    data = _to_dict(raw)
    if "default" in data and len(data) == 1:
        return raw, []  # 非结构化值（单个地址）：不裁剪
    names = config.step_names
    kept = set(kept_steps)
    orphans: list[str] = []
    out = {}
    for key, val in data.items():
        if key in names and key not in kept:
            orphans.extend(collect_urls(val))
        else:
            out[key] = val
    return (json.dumps(out, ensure_ascii=False) if out else ""), orphans


def prune_result_path(task_type: str, raw: str, kept_steps: list[str]) -> tuple[str, list[str]]:
    """重试 / 重置后裁剪 result_path。类型登记了自己的 ``prune_result_path`` 就用它。

    为什么必须裁剪：留着上一代的下载地址，worker 恢复时会把它当当代产物取回来——
    半新半旧的组合不会报错，只会算出错的结果。孤儿文件由调用方在**库写完之后**异步删除
    （顺序不可反：先删文件后裁库，中间若崩就留下一批指向空地址的条目）。
    """
    hook = get_task_type(task_type).prune_result_path
    if hook is not None:
        return hook(raw, kept_steps)
    return _prune_by_steps(task_type, raw, kept_steps)


def prune_log_path(task_type: str, raw: str, kept_steps: list[str]) -> str:
    """裁剪 log_path：只保留已完成步骤的日志 key。

    被丢弃的恰是"本次要重跑的步骤"的旧日志——典型来源是上一轮取消 / 中断时 worker 已增量推送上来的半截日志。
    不清掉的话，任务翻 PENDING 后到新一轮推出第一份日志之前，前端读到的是上一轮内容。
    """
    return _prune_by_steps(task_type, raw, kept_steps)[0]


def resolve_step_token(task_type: str, steps_done: list[str], token: str) -> tuple[str, Optional[int]]:
    """解析 retry / redo 的步骤令牌 → ``(裸步骤名, 截断索引|None)``。

    默认：令牌就是步骤名，截断点取它在已完成轨迹里**最近一次**出现的位置；不在轨迹里 → ``None``
    （典型：失败时未完成的当前步，截断时保留全部）。类型可登记 ``resolve_step_token`` 自定义（带序号消歧等）。
    """
    hook = get_task_type(task_type).resolve_step_token
    if hook is not None:
        return hook(steps_done, token)
    for i in range(len(steps_done) - 1, -1, -1):
        if steps_done[i] == token:
            return token, i
    return token, None
