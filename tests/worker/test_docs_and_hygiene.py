"""文档与代码不漂移 + 包卫生（独立性、无业务词、无旧名）。"""

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PKG = REPO / "webx_worker"
DOC = (REPO / "docs" / "worker.md").read_text(encoding="utf-8")


def _py_files():
    return sorted(PKG.rglob("*.py"))


def test_every_environment_variable_in_the_code_is_documented():
    names = set()
    for p in _py_files():
        if p.name == "fake_server.py":
            continue
        names |= set(re.findall(r"WEBX_WORKER_[A-Z0-9_]+", p.read_text(encoding="utf-8")))
    names.discard("WEBX_WORKER_")
    missing = sorted(n for n in names if n not in DOC)
    assert not missing, f"docs/worker.md 没有记录这些环境变量: {missing}"


def test_documented_env_names_all_exist_in_the_code():
    """反向：文档里写的 WEBX_WORKER_* 都得在代码里真实存在（防止文档写了个不存在的开关）。"""
    code = "\n".join(p.read_text(encoding="utf-8") for p in _py_files())
    doc_names = set(re.findall(r"WEBX_WORKER_[A-Z0-9_]+", DOC))
    # 表格里把成对的名字缩写成 `_LOG_TAIL_LINES` 这种后缀形式，不在此列；只核对完整名
    ghosts = sorted(n for n in doc_names if n not in code and not n.endswith("_"))
    assert not ghosts, f"文档里出现了代码中不存在的环境变量: {ghosts}"


def test_doc_mentions_every_subcommand_and_protocol_gap_section():
    for sub in ("selftest", "doctor", "service one", "service multi", "cleanup"):
        assert sub in DOC, sub
    assert "协议缺口的裁定" in DOC and "已知限制" in DOC


def test_no_forbidden_names_anywhere():
    """硬约束：包、测试、文档里不得出现旧项目名。"""
    needle = "xy" + "mol"                       # 拆开写，免得本文件自己命中自己
    offenders = []
    for base in (PKG, REPO / "tests" / "worker"):
        for p in base.rglob("*"):
            if p.is_file() and p.suffix in {".py", ".md", ".txt", ".toml", ".cfg", ".sh", ".service"}:
                if needle in p.read_text(encoding="utf-8", errors="ignore").lower():
                    offenders.append(str(p.relative_to(REPO)))
    for p in (REPO / "docs").glob("worker*.md"):
        if needle in p.read_text(encoding="utf-8").lower():
            offenders.append(str(p.relative_to(REPO)))
    assert not offenders, offenders


def test_package_imports_only_stdlib_httpx_and_itself():
    """独立性（比 test_layering 更严）：允许的第三方只有 httpx。"""
    import sys

    stdlib = set(sys.stdlib_module_names)
    bad = []
    for p in _py_files():
        for node in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
            mods = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                mods = [node.module]
            for m in mods:
                top = m.split(".")[0]
                if top not in stdlib and top not in {"httpx", "webx_worker"}:
                    bad.append(f"{p.relative_to(REPO)} → {m}")
    assert not bad, bad


def test_no_business_modules_were_carried_over():
    present = {p.stem for p in _py_files()}
    for banned in ("gmx_noise", "executor_o", "channel_template", "ffcache", "molid", "scratchpad_run_md"):
        assert banned not in present, banned
    assert not (PKG / "tasks").exists() and not (PKG / "top").exists()
