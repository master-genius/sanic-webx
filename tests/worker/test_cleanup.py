"""任务目录清理：只删「已确认的终态」且超龄的目录；仍被上传队列引用的跳过。"""

import json
from datetime import datetime, timedelta, timezone

import pytest

from webx_worker import cleanup as C
from webx_worker.sdk import upload_queue as q


def _task(work, tid, status, confirmed=True, age_days=10.0, extra_file=None):
    d = work / tid
    d.mkdir(parents=True)
    ts = datetime.now(timezone.utc) - timedelta(days=age_days)
    (d / "ok.json").write_text(json.dumps({
        "task_id": tid, "status": status, "server_confirmed": confirmed,
        "completed_at": ts.isoformat(),
    }))
    if extra_file:
        (d / extra_file).write_text("data")
    return d


def test_parse_duration_forms():
    assert C.parse_duration("30d") == timedelta(days=30)
    assert C.parse_duration("3h") == timedelta(hours=3)
    assert C.parse_duration("120m") == timedelta(minutes=120)
    assert C.parse_duration("300s") == timedelta(seconds=300)
    assert C.parse_duration("2") == timedelta(days=2) and C.parse_duration(1.5) == timedelta(days=1.5)
    with pytest.raises(ValueError):
        C.parse_duration("abc")
    with pytest.raises(ValueError):
        C.parse_duration("")


def test_default_policy_purges_old_confirmed_terminal_dirs(tmp_path):
    old_ok = _task(tmp_path, "t_old_ok", "completed", age_days=8)
    old_failed = _task(tmp_path, "t_old_fail", "failed", age_days=3)
    fresh = _task(tmp_path, "t_fresh", "completed", age_days=1)
    r = C.TaskDirCleaner(tmp_path).scan_and_purge()
    assert not old_ok.exists() and not old_failed.exists() and fresh.exists()
    assert r.purged == 2 and sorted(r.purged_task_ids) == ["t_old_fail", "t_old_ok"]
    assert r.skipped["fresh_completed"] == 1


def test_unconfirmed_and_unmarked_and_out_of_scope_are_kept(tmp_path):
    unconfirmed = _task(tmp_path, "t_unconf", "completed", confirmed=False, age_days=30)
    cancelled = _task(tmp_path, "t_canc", "cancelled", age_days=30)            # 不在默认策略里
    running = tmp_path / "t_running"
    running.mkdir()                                                           # 无 ok.json = 执行中
    broken = tmp_path / "t_broken"
    broken.mkdir()
    (broken / "ok.json").write_text("{oops")
    r = C.TaskDirCleaner(tmp_path).scan_and_purge()
    assert all(p.exists() for p in (unconfirmed, cancelled, running, broken))
    assert r.purged == 0
    assert r.skipped["unconfirmed_completed"] == 1 and r.skipped["no_ok_json"] == 1
    assert r.skipped["bad_ok_json"] == 1 and r.skipped["out_of_scope_cancelled"] == 1


def test_directory_referenced_by_upload_queue_is_never_purged(tmp_path):
    """队列只记路径、不复制文件：清理必须避开还没传完的产物，否则产物永久缺失。"""
    d = _task(tmp_path, "t_queued", "completed", age_days=30, extra_file="out.bin")
    other = _task(tmp_path, "t_free", "completed", age_days=30)
    q.ensure_dirs(tmp_path)
    q.enqueue(tmp_path, "t_queued", d / "out.bin", "out.bin", ["s", "u"])
    r = C.TaskDirCleaner(tmp_path).scan_and_purge()
    assert d.exists() and not other.exists()
    assert r.skipped["pending_upload_completed"] == 1


def test_dry_run_deletes_nothing_but_reports(tmp_path):
    d = _task(tmp_path, "t1", "failed", age_days=9)
    r = C.TaskDirCleaner(tmp_path, dry_run=True).scan_and_purge()
    assert d.exists() and r.purged == 1


def test_custom_policy_and_cli_json_output(tmp_path, capsys):
    d = _task(tmp_path, "t1", "cancelled", age_days=2)
    with pytest.raises(SystemExit) as e:
        C.main(["--work-dir", str(tmp_path), "--status", "cancelled", "--timeout", "1d", "--quiet"])
    assert e.value.code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["purged"] == 1 and out["purged_task_ids"] == ["t1"] and not d.exists()


def test_cli_rejects_orphan_timeout(tmp_path):
    with pytest.raises(SystemExit) as e:
        C.main(["--work-dir", str(tmp_path), "--timeout", "1d"])
    assert "--status" in str(e.value)


def test_missing_work_dir_is_harmless(tmp_path):
    assert C.TaskDirCleaner(tmp_path / "nope").scan_and_purge().scanned == 0


def test_cleanup_subcommand_is_reachable_through_python_dash_m(tmp_path):
    import subprocess
    import sys

    from webx_worker._paths import PACKAGE_PARENT

    d = _task(tmp_path, "t1", "failed", age_days=5)
    out = subprocess.run(
        [sys.executable, "-m", "webx_worker", "cleanup", "--work-dir", str(tmp_path), "--quiet"],
        env={"PYTHONPATH": PACKAGE_PARENT, "PATH": "/usr/bin:/bin"},
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout)["purged"] == 1 and not d.exists()
