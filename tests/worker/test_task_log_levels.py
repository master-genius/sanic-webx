"""任务子进程的日志级别 —— **降噪，不是保证**。

⚠️ 「内部细节不进前端」这条保证**不在这里**，在 worker 侧的 redactors 与
``_REDACT_DROP_RE``（交付出口整行剔除，与级别无关，测试见 test_log_redactors.py）。
理由：级别是配置，配置会被改；把保证建立在「默认没人开 DEBUG」上等于没有保证。

这里管两件级别才管得了的事：
  ① **截断窗口** —— 剔除发生在截断之后，这些行若真被打出来会先占满首尾窗口；
  ② **httpx 这类第三方库** —— 它们打不上我们的标记，只能靠级别压住。
"""

import logging

import pytest

from webx_worker import envnames as E
from webx_worker.sdk.logsetup import LOG_LEVEL_ENV, configure_task_logging


@pytest.fixture(autouse=True)
def _restore_logging():
    """配置日志是全局副作用，逐条用例还原，别污染同进程里的其它测试。"""
    root = logging.getLogger()
    saved = [root.level] + [logging.getLogger(n).level
                            for n in ("httpx", "httpcore", "urllib3")]
    yield
    root.setLevel(saved[0])
    for n, lv in zip(("httpx", "httpcore", "urllib3"), saved[1:]):
        logging.getLogger(n).setLevel(lv)


def test_env_name_comes_from_the_shared_constants():
    assert LOG_LEVEL_ENV == E.ENV_TASK_LOG_LEVEL == "WEBX_WORKER_TASK_LOG_LEVEL"


def test_default_level_is_info(monkeypatch):
    monkeypatch.delenv(LOG_LEVEL_ENV, raising=False)
    assert configure_task_logging() == logging.INFO
    assert logging.getLogger().level == logging.INFO


def test_httpx_is_silenced_by_default(monkeypatch):
    """🔴 httpx 在 **INFO** 就会为每个请求打一行 ``HTTP Request: GET <url> "404"``。

    它是第三方库打的、**打不上我们的标记**，所以这里是它唯一的闸。
    """
    monkeypatch.delenv(LOG_LEVEL_ENV, raising=False)
    configure_task_logging()
    for name in ("httpx", "httpcore", "urllib3"):
        lg = logging.getLogger(name)
        assert lg.level == logging.WARNING, f"{name} 未被压到 WARNING"
        assert not lg.isEnabledFor(logging.INFO), f"{name} 的 INFO 行仍会产生"


def test_invalid_level_falls_back_to_info(monkeypatch):
    """日志配置不该成为任务失败的原因——无效值静默回落，不抛。"""
    for bad in ("", "  ", "VERBOSE", "42", "debug; rm -rf /"):
        monkeypatch.setenv(LOG_LEVEL_ENV, bad)
        assert configure_task_logging() == logging.INFO, f"{bad!r} 未回落 INFO"


def test_level_name_is_case_insensitive(monkeypatch):
    monkeypatch.setenv(LOG_LEVEL_ENV, "debug")
    assert configure_task_logging() == logging.DEBUG


def test_debug_switch_restores_everything(monkeypatch):
    """「能记录下来」必须是真的：DEBUG 时应用日志和第三方往返一起回来。"""
    monkeypatch.setenv(LOG_LEVEL_ENV, "DEBUG")
    assert configure_task_logging() == logging.DEBUG
    assert logging.getLogger().level == logging.DEBUG
    assert logging.getLogger("httpx").isEnabledFor(logging.DEBUG)


def test_level_applied_even_when_root_already_configured(monkeypatch):
    """🔴 basicConfig 在 root 已有 handler 时会整个跳过（连 level 都不设）。

    只要有任何一环提前碰过 logging，级别就会静默失效——那意味着 debug 行会突然全部
    涌进用户日志。必须显式设 root level。
    """
    root = logging.getLogger()
    probe = logging.NullHandler()
    root.addHandler(probe)
    try:
        monkeypatch.setenv(LOG_LEVEL_ENV, "WARNING")
        configure_task_logging()
        assert root.level == logging.WARNING
    finally:
        root.removeHandler(probe)


def test_docs_say_the_variable_must_go_through_run_env():
    """⚠️ 配置文件层（.env / 覆盖层）只对主进程可见，写这个变量没用——钉住文档里那句话。"""
    from webx_worker.sdk import logsetup

    doc = logsetup.__doc__
    assert "run_env" in doc and "子进程只认「真环境变量」" in doc
