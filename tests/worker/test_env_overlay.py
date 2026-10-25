"""配置分层（.env 基础层 / 实例覆盖层 / 真实环境变量）回归测试。

钉住的核心不变量：

    加载覆盖层后，**模块级单例** callback_client / http_file_client 必须跟着变。

为什么这条最关键：这两个单例在 import 期就构造完成，早于 main() 的 parse_args()。
若在 __init__ 里把 base_url / internal_token 等拷成实例属性，任何启动期的配置覆盖
对它们都会完全失效（值已冻死）。改为 property 晚绑定后问题消失——但这是个不可见的
约束：任何人若把 property 改回 __init__ 赋值、或新增一个构造期拷贝配置的模块级单例，
就会静默退回旧行为。本测试一旦红，说明那条约束被破坏了。

另外钉住自写 .env 解析器的语义（不再依赖第三方配置库）。
"""

from pathlib import Path

import pytest

from webx_worker import config as C
from webx_worker.callback import callback_client
from webx_worker.config import (
    apply_env_overlay,
    default_overlay_for,
    load_base_env,
    parse_env_text,
    worker_settings,
)
from webx_worker.http_file_client import http_file_client


def test_overlay_reaches_module_level_singletons(tmp_path: Path) -> None:
    """覆盖层生效后，两个 import 期构造的单例必须读到新值。"""
    overlay = tmp_path / "gpu9_env"
    overlay.write_text(
        "WEBX_WORKER_WEB_BASE_URL=http://overlay-host:9999\n"
        "WEBX_WORKER_INTERNAL_TOKEN=overlay-token\n"
        "WEBX_WORKER_STORAGE_URL=http://overlay-storage:8888\n",
        encoding="utf-8",
    )

    assert apply_env_overlay(overlay) == overlay.resolve()

    assert callback_client.base_url == "http://overlay-host:9999"
    assert callback_client.internal_token == "overlay-token"
    assert callback_client.storage_url == "http://overlay-storage:8888"
    assert http_file_client.base_url == "http://overlay-host:9999"
    assert http_file_client.internal_token == "overlay-token"


def test_inherit_override_add(tmp_path: Path) -> None:
    """三种语义：未写的键继承 .env、写了的键覆盖、.env 没有的键新增。"""
    env = tmp_path / ".env"
    env.write_text(
        "WEBX_WORKER_WEB_BASE_URL=http://base:1\nWEBX_WORKER_WORK_DIR=/base/work\n",
        encoding="utf-8",
    )
    overlay = tmp_path / "partial_env"
    # 覆盖 WORK_DIR、新增 LOG_DIR；WEB_BASE_URL 沿用 .env
    overlay.write_text(
        "WEBX_WORKER_WORK_DIR=/overlay/work\nWEBX_WORKER_LOG_DIR=/overlay/logs\n", encoding="utf-8"
    )
    load_base_env(env)
    apply_env_overlay(overlay)

    assert worker_settings.WORK_DIR == "/overlay/work", "覆盖失败"
    assert worker_settings.WEB_BASE_URL == "http://base:1", "继承失败"
    assert worker_settings.LOG_DIR == "/overlay/logs", "新增失败"


def test_real_environment_beats_overlay_and_base(tmp_path: Path, monkeypatch) -> None:
    """优先级：真实环境变量 > 覆盖层 > .env（运维的显式覆盖不能被文件悄悄盖掉）。"""
    (tmp_path / ".env").write_text("WEBX_WORKER_WORK_DIR=/from-base\n", encoding="utf-8")
    (tmp_path / "x_env").write_text("WEBX_WORKER_WORK_DIR=/from-overlay\n", encoding="utf-8")
    load_base_env(tmp_path / ".env")
    apply_env_overlay(tmp_path / "x_env")
    assert worker_settings.WORK_DIR == "/from-overlay"

    monkeypatch.setenv("WEBX_WORKER_WORK_DIR", "/from-process-env")
    C.refresh_settings()
    assert worker_settings.WORK_DIR == "/from-process-env"


def test_refresh_keeps_debug_flag(tmp_path: Path) -> None:
    """DEBUG 来自命令行而非环境：重建配置时不能把它冲回默认值。"""
    worker_settings.DEBUG = True
    try:
        (tmp_path / "e_env").write_text("WEBX_WORKER_LOG_DIR=/x\n", encoding="utf-8")
        apply_env_overlay(tmp_path / "e_env")
        assert worker_settings.DEBUG is True
    finally:
        worker_settings.DEBUG = False


def test_missing_overlay_is_silent(tmp_path: Path) -> None:
    """按约定探测时文件缺失是常态，必须静默跳过（返回 None）而非报错。"""
    assert apply_env_overlay(tmp_path / "does_not_exist_env") is None
    assert load_base_env(tmp_path / "no.env") is None


def test_default_overlay_convention() -> None:
    """约定推导：<channel 配置同目录>/<配置名去扩展名>_env。"""
    assert default_overlay_for(Path("/a/channel_config/gpu0.py")) == Path("/a/channel_config/gpu0_env")


def test_config_attrs_are_read_only() -> None:
    """这些属性故意不提供 setter：误写应显式报错而非静默偏离配置。"""
    for obj, attr in (
        (callback_client, "base_url"),
        (callback_client, "internal_token"),
        (http_file_client, "base_url"),
    ):
        with pytest.raises(AttributeError):
            setattr(obj, attr, "should-not-work")


# ───────────────────────── .env 解析器 ─────────────────────────

def test_parse_env_text_semantics() -> None:
    got = parse_env_text(
        "# 注释\n"
        "A=1\n"
        "export B = two  \n"
        'C="quoted # not comment"\n'
        "D='single'\n"
        "E=plain # inline comment\n"
        "F=url#fragment\n"        # '#' 前无空白 → 不是注释
        "G=\n"
        "A=last-wins\n"
        "no_equal_sign\n"
        "=novalue\n"
    )
    assert got == {
        "A": "last-wins", "B": "two", "C": "quoted # not comment", "D": "single",
        "E": "plain", "F": "url#fragment", "G": "",
    }


def test_typed_getters_fall_back_on_garbage(monkeypatch, caplog) -> None:
    monkeypatch.setenv("WEBX_WORKER_UPLOADER_CONCURRENCY", "not-a-number")
    monkeypatch.setenv("WEBX_WORKER_UPLOADER_ENABLED", "off")
    C.refresh_settings()
    assert worker_settings.UPLOADER_CONCURRENCY == 3          # 默认值
    assert worker_settings.UPLOADER_ENABLED is False
    assert "UPLOADER_CONCURRENCY" in caplog.text


def test_lease_filters_distinguish_unset_from_zero() -> None:
    """空串 = 不限；"0" 是有意义且常用的取值（只服务最低档）。"""
    worker_settings.LEASE_MAX_LEVEL = ""
    worker_settings.LEASE_MAX_COMPUTE_POINTS = "0"
    try:
        assert C.lease_filters() == (None, 0)
        worker_settings.LEASE_MAX_LEVEL = "junk"
        assert C.lease_filters() == (None, 0)
    finally:
        C.refresh_settings()


def test_worker_home_defaults_to_cwd_not_package_dir(monkeypatch, tmp_path) -> None:
    """主目录锚定 cwd / WEBX_WORKER_HOME，绝不是包源码目录（包可能装在 site-packages）。"""
    monkeypatch.delenv("WEBX_WORKER_HOME", raising=False)
    monkeypatch.chdir(tmp_path)
    assert C.worker_home() == tmp_path.resolve()
    monkeypatch.setenv("WEBX_WORKER_HOME", str(tmp_path / "h"))
    assert C.worker_home() == (tmp_path / "h").resolve()
