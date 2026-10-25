"""后台文件日志：小时命名 + PID 结尾 + 字节/行数双上限滚动 + stdout/stderr tee。"""

import logging
import re
import sys

import pytest

from webx_worker import log_config as LC


def test_file_name_has_hour_then_pid_so_that_sorting_is_chronological(tmp_path):
    h = LC.HourlyRotatingFileHandler(str(tmp_path), pid=4242)
    h.write_text("hello\n")
    h.close()
    names = [p.name for p in tmp_path.iterdir()]
    assert len(names) == 1
    assert re.fullmatch(r"worker_task_\d{4}-\d{2}-\d{2}-\d{2}_4242\.log", names[0]), names


def test_rolls_over_on_line_limit_and_appends_sequence_numbers(tmp_path):
    h = LC.HourlyRotatingFileHandler(str(tmp_path), max_lines=3, pid=1)
    for i in range(7):
        h.write_text(f"line {i}\n")
    h.close()
    files = sorted(p.name for p in tmp_path.iterdir())
    assert len(files) == 3
    assert {f.split("_1.", 1)[1] for f in files} == {"log", "1.log", "2.log"}
    assert sum(len(p.read_text().splitlines()) for p in tmp_path.iterdir()) == 7


def test_rolls_over_on_byte_limit(tmp_path):
    h = LC.HourlyRotatingFileHandler(str(tmp_path), max_bytes=20, pid=1)
    for _ in range(4):
        h.write_text("0123456789\n")          # 11 字节：每个文件最多容 1 条
    h.close()
    assert len(list(tmp_path.iterdir())) == 4


def test_reopen_in_same_process_continues_the_existing_file(tmp_path):
    h1 = LC.HourlyRotatingFileHandler(str(tmp_path), max_lines=100, pid=7)
    h1.write_text("a\n")
    h1.close()
    h2 = LC.HourlyRotatingFileHandler(str(tmp_path), max_lines=100, pid=7)
    h2.write_text("b\n")
    h2.close()
    files = list(tmp_path.iterdir())
    assert len(files) == 1 and files[0].read_text() == "a\nb\n"


def test_different_pids_never_share_a_file(tmp_path):
    for pid in (1, 2):
        h = LC.HourlyRotatingFileHandler(str(tmp_path), pid=pid)
        h.write_text("x\n")
        h.close()
    assert len(list(tmp_path.iterdir())) == 2


@pytest.fixture
def restore_logging():
    root = logging.getLogger()
    saved = (root.level, list(root.handlers), sys.stdout, sys.stderr, sys.excepthook)
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved[1]:
        root.addHandler(h)
    root.setLevel(saved[0])
    sys.stdout, sys.stderr, sys.excepthook = saved[2], saved[3], saved[4]


def test_setup_logging_tees_print_and_logging_into_the_rolling_file(tmp_path, restore_logging):
    eff = LC.setup_logging(log_dir=str(tmp_path / "logs"), debug=True)
    assert eff is not None
    logging.getLogger("t").debug("debug-visible")
    print("plain print")
    sys.stdout.flush()
    text = "".join(p.read_text() for p in (tmp_path / "logs").iterdir())
    assert "debug-visible" in text and "plain print" in text


def test_setup_logging_is_idempotent_and_console_only_without_dir(tmp_path, restore_logging):
    assert LC.setup_logging(log_dir=None) is None
    LC.setup_logging(log_dir=str(tmp_path / "l"))
    LC.setup_logging(log_dir=str(tmp_path / "l"))
    assert not isinstance(getattr(sys.stdout, "_original", None), LC._TeeStream), "tee 套 tee"
    assert len(logging.getLogger().handlers) == 1
