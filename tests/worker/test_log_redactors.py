"""日志脱敏的业务接缝：`redactors` 注册表（取代原来写死在 log_writer 里的业务过滤）。

通用机制只保留与协议相关的内置规则（内部 API 路径 / URL / 本机路径）；
「某个外部程序的启动横幅」「业务内部标记行」这类规则通过 `redactors: list[Callable[[str], str]]`
注册，来自 `TaskKindPlugin.redactors`。
"""

import re

from webx_worker.log_writer import (
    ConcurrentLogWriter,
    collapse_blank_runs,
    redact_for_frontend,
)
from webx_worker.plugin import TaskKindPlugin

_TAG = "INTERNAL_NOISE:"


def _drop_tagged(text: str) -> str:
    return "\n".join(ln for ln in text.splitlines() if _TAG not in ln)


def test_redactors_run_before_builtin_rules():
    """业务规则可能依赖原文里的 URL 作锚点——必须先于内置的 URL 遮蔽执行。"""
    seen = {}

    def spy(text: str) -> str:
        seen["text"] = text
        return text

    redact_for_frontend("see https://example.com/x for details", (), [spy])
    assert "https://example.com/x" in seen["text"], "redactor 看到的已经是被遮蔽后的文本"


def test_tagged_lines_are_dropped_regardless_of_level():
    text = "\n".join([
        "2026-01-01 [INFO] mod: ok line",
        f"2026-01-01 [DEBUG] mod: {_TAG} cache hit",
        f"2026-01-01 [WARNING] mod: {_TAG} cache write failed",
        "2026-01-01 [ERROR] mod: real error",
    ])
    out = redact_for_frontend(text, (), [_drop_tagged])
    assert out.splitlines() == ["2026-01-01 [INFO] mod: ok line", "2026-01-01 [ERROR] mod: real error"]


def test_multiple_redactors_apply_in_registration_order():
    out = redact_for_frontend("a", (), [lambda t: t + "b", lambda t: t + "c"])
    assert out == "abc"


def test_a_crashing_redactor_is_skipped_but_builtin_rules_still_run():
    """脱敏失败不得影响日志推送，更不能让内置的 URL/路径遮蔽也一起失效。"""
    def boom(_t):
        raise RuntimeError("redactor bug")

    out = redact_for_frontend("visit https://example.com/a/b now", (), [boom])
    assert out == "visit [redacted] now"


def test_builtin_drop_rule_covers_the_sdk_own_log_prefixes():
    """内置整行剔除规则（`_REDACT_DROP_RE`）必须与 SDK 自己的日志措辞对得上：
    sdk.result_client 的上传/字段更新/下载日志带这些前缀，漏了就会把内部接口信息推给前端。"""
    from webx_worker.log_writer import _REDACT_DROP_RE

    for line in (
        "结果文件上传成功: task_id=t1, file=a.dat, download_url=/public/tasks/t1/results/a.dat",
        "任务字段更新成功: task_id=t1, mode=merge, fields=['result_path']",
        "产物下载成功: task_id=t1, filename=a.dat",
        "产物 404: task_id=t1, filename=a.dat",
        "GET /internal/tasks/t1/files/a.dat -> 200",
    ):
        assert _REDACT_DROP_RE.search(line), line
    assert not _REDACT_DROP_RE.search("Potential Energy = -1.2e+05 kJ/mol")


def test_blank_runs_left_by_dropped_blocks_are_collapsed():
    text = "head\n\n\n\n\ntail"
    assert collapse_blank_runs(text) == "head\n\ntail"
    # 单个空行原样保留；带尾随空格的空行保留原文而不是改写成 ""
    assert collapse_blank_runs("a\n\nb") == "a\n\nb"
    assert collapse_blank_runs("a\n \n\nb") == "a\n \nb"
    # 幂等
    assert collapse_blank_runs(collapse_blank_runs(text)) == collapse_blank_runs(text)


def test_removed_block_does_not_leave_a_blank_run_end_to_end():
    banner = re.compile(r"BANNER-START.*?BANNER-END\n?", re.S)
    out = redact_for_frontend(
        "before\n\nBANNER-START\nx\ny\nBANNER-END\n\nafter", (),
        [lambda t: banner.sub("", t)],
    )
    assert out == "before\n\nafter"


async def test_plugin_redactors_are_wired_into_the_writer_by_reference(tmp_path):
    """build_executor 传的是插件列表的引用：插件事后追加的函数同样生效。"""
    plugin = TaskKindPlugin()
    w = ConcurrentLogWriter(redactors=plugin.redactors)
    await w.write("t1", str(tmp_path / "t1" / "run.log"), f"{_TAG} secret\n")
    await w.write("t1", str(tmp_path / "t1" / "run.log"), "visible\n")
    assert "secret" in (w.get_truncated_content("t1") or "")      # 尚未注册
    plugin.redactors.append(_drop_tagged)
    assert (w.get_truncated_content("t1") or "") == "visible"
    # 本地全量文件不受影响
    assert "secret" in (tmp_path / "t1" / "run.log").read_text(encoding="utf-8")
    await w.close_task("t1")


def test_app_build_executor_hands_plugin_redactors_to_the_writer(tmp_path, monkeypatch):
    from webx_worker.app import build_executor
    from webx_worker.config import worker_settings

    monkeypatch.setattr(worker_settings, "WORK_DIR", str(tmp_path))
    plugin = TaskKindPlugin()
    ex = build_executor({}, "", plugin=plugin)
    assert ex.log_writer.redactors is plugin.redactors
