"""
任务目录清理模块 — 对超时的终态任务做硬删除。

设计前提（依赖 ok.json 协议，详见 executor._write_ok_json）：
  - completed / cancelled / failed 三种本地终态都会被 executor 写入 ok.json
  - server_confirmed=True 表示服务端已知此终态
  - 接收新任务时 executor 会先删除旧 ok.json（见 executor.execute），
    所以 ok.json 始终对应"上一次完整执行"，不会与新一轮执行混淆

清理判定规则（按顺序短路）：
  1. 无 ok.json                          → 跳过（执行中 / 旧版本 task_dir）
  2. ok.json 损坏                        → 跳过（记 warning）
  3. status 不在 policy 中                → 跳过
  4. server_confirmed=False              → 跳过（终态未上报，留作后续诊断/补报）
  5. age (now - completed_at) < timeout  → 跳过
  6. 否则                                → shutil.rmtree（硬删，不软清不标记）

使用方式：
  库模式  : from webx_worker.cleanup import TaskDirCleaner
            cleaner = TaskDirCleaner(work_base_dir="/path/to/tasks")
            result  = cleaner.scan_and_purge()
  CLI 模式: python -m webx_worker cleanup --status failed --timeout 30d
            python -m webx_worker cleanup --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# 默认清理策略：failed 2 天清理，completed 7 天清理。
# 本地工作目录只是「可重建的中间态」—— 产物已确认上传到 storage（server_confirmed），
# 留着仅为排障与跨机恢复，不必长留；磁盘峰值占用比追溯窗口更值钱。
# cancelled 不在默认列表 —— 用户主动取消的任务量少，留给手动 CLI 触发。
DEFAULT_POLICY: dict[str, timedelta] = {
    "failed":    timedelta(days=2),
    "completed": timedelta(days=7),
}


@dataclass
class CleanupResult:
    scanned: int = 0
    purged: int = 0
    purged_task_ids: list[str] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scanned": self.scanned,
            "purged": self.purged,
            "purged_task_ids": self.purged_task_ids,
            "skipped": dict(self.skipped),
            "errors": self.errors,
        }


def parse_duration(spec: str | int | float | timedelta) -> timedelta:
    """解析人类可读的时长字符串/数值。

    支持的形式：
      timedelta        → 原样返回
      int / float      → 按"天"计
      "30" / "30d"     → 30 天
      "3h"             → 3 小时
      "120m"           → 120 分钟
      "300s"           → 300 秒
    """
    if isinstance(spec, timedelta):
        return spec
    if isinstance(spec, (int, float)):
        return timedelta(days=float(spec))
    s = str(spec).strip().lower()
    if not s:
        raise ValueError(f"无效的时长: {spec!r}")
    if s[-1] in ("d", "h", "m", "s"):
        unit = s[-1]
        try:
            value = float(s[:-1])
        except ValueError as e:
            raise ValueError(f"无效的时长: {spec!r}") from e
        return {
            "d": timedelta(days=value),
            "h": timedelta(hours=value),
            "m": timedelta(minutes=value),
            "s": timedelta(seconds=value),
        }[unit]
    # 纯数字按天
    try:
        return timedelta(days=float(s))
    except ValueError as e:
        raise ValueError(f"无效的时长: {spec!r}") from e


class TaskDirCleaner:
    """任务目录清理器（纯本地操作，无网络依赖）。

    用法：
        cleaner = TaskDirCleaner(work_base_dir="/path/to/tasks")
        result  = cleaner.scan_and_purge()                       # 默认策略
        result  = cleaner.scan_and_purge({"failed": "7d"})       # 自定义策略
        result  = cleaner.scan_and_purge(dry_run=True)           # 试运行
    """

    def __init__(self, work_base_dir: str | Path, dry_run: bool = False) -> None:
        self.work_base_dir = Path(work_base_dir)
        self.dry_run = dry_run
        # 每轮 scan_and_purge 重置：队列在扫描期间可能变化，但一轮内取一次快照即可
        self._pending_tasks_cache: set[str] | None = None

    def scan_and_purge(
        self,
        policy: dict[str, timedelta | str | int | float] | None = None,
        dry_run: bool | None = None,
    ) -> CleanupResult:
        """扫描工作目录，按策略清理超时任务。

        Args:
            policy: {status: timeout} 字典，timeout 由 parse_duration 解析。
                    None 时使用 DEFAULT_POLICY。
            dry_run: 覆盖实例的 dry_run 标记；None 表示沿用 self.dry_run。
        """
        effective_policy: dict[str, timedelta] = {
            s: parse_duration(t) for s, t in (policy or DEFAULT_POLICY).items()
        }
        effective_dry_run = self.dry_run if dry_run is None else dry_run
        # 每轮重新取一次上传队列快照（上一轮的结果可能已过期）
        self._pending_tasks_cache = None

        result = CleanupResult()
        if not self.work_base_dir.exists():
            logger.warning(f"[Cleanup] 工作目录不存在: {self.work_base_dir}")
            return result

        now = datetime.now(timezone.utc)
        logger.info(
            f"[Cleanup] 开始扫描 {self.work_base_dir}，"
            f"策略={{ {', '.join(f'{s}: {t}' for s, t in effective_policy.items())} }}"
            f"{' (dry-run)' if effective_dry_run else ''}"
        )

        for task_dir in sorted(self.work_base_dir.iterdir()):
            if not task_dir.is_dir():
                continue
            result.scanned += 1
            try:
                self._evaluate_one(
                    task_dir, effective_policy, now, effective_dry_run, result,
                )
            except Exception as e:
                logger.error(
                    f"[Cleanup] task={task_dir.name} 异常: {e}", exc_info=True,
                )
                result.errors.append(f"{task_dir.name}: {e}")
                result.skipped["error"] += 1

        logger.info(
            f"[Cleanup] 完成 scanned={result.scanned} purged={result.purged} "
            f"skipped={dict(result.skipped)} errors={len(result.errors)}"
        )
        return result

    def _tasks_with_pending_uploads(self) -> set[str]:
        """当前上传队列引用到的 task_id 集合（每轮扫描只读一次，结果缓存）。

        读磁盘目录即可，不需要与 uploader 通信——目录本身就是共享状态，
        这正是选磁盘队列而非内存队列的红利之一。
        读取失败一律返回空集：清理是空间回收，不该因为观测失败而停摆。
        """
        if self._pending_tasks_cache is not None:
            return self._pending_tasks_cache
        tasks: set[str] = set()
        try:
            from .sdk import upload_queue as q
            for d in (q.pending_dir(self.work_base_dir), q.inflight_dir(self.work_base_dir)):
                if d.is_dir():
                    # 文件名形如 {task_id}__{upload_id}.json，不必解析 JSON
                    tasks.update(f.name.split("__", 1)[0] for f in d.glob("*.json"))
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Cleanup] 读取上传队列失败（按无待传处理）: {e}")
        self._pending_tasks_cache = tasks
        return tasks

    def _evaluate_one(
        self,
        task_dir: Path,
        policy: dict[str, timedelta],
        now: datetime,
        dry_run: bool,
        result: CleanupResult,
    ) -> None:
        """评估并清理单个 task_dir，所有跳过原因记入 result.skipped。"""
        task_id = task_dir.name
        ok_path = task_dir / "ok.json"

        # 规则 1: 无 ok.json → 跳过
        if not ok_path.exists():
            result.skipped["no_ok_json"] += 1
            return

        # 规则 2: ok.json 损坏 → 跳过
        try:
            ok_data = json.loads(ok_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            result.skipped["bad_ok_json"] += 1
            logger.warning(f"[Cleanup] task={task_id} ok.json 解析失败: {e}")
            return

        status = ok_data.get("status", "")

        # 规则 3: status 不在 policy 中 → 跳过
        if status not in policy:
            result.skipped[f"out_of_scope_{status or 'unknown'}"] += 1
            return

        # 规则 4: 未确认 → 跳过（终态信息可能尚未上报到服务端）
        if not ok_data.get("server_confirmed", False):
            result.skipped[f"unconfirmed_{status}"] += 1
            logger.warning(
                f"[Cleanup] task={task_id} status={status} confirmed=False，跳过清理"
            )
            return

        # 规则 5: 取时间起点，判超时
        ref_time = _parse_ok_timestamp(ok_data, ok_path)
        age = now - ref_time
        if age < policy[status]:
            result.skipped[f"fresh_{status}"] += 1
            return

        # 规则 5.5: 仍有产物在上传队列里 → 跳过
        # 队列只记路径、不复制文件（几 GB 的产物复制一份不可接受），所以清理必须避开
        # 还没传完的产物；删了它 uploader 只能丢弃条目，产物就永久缺失了。
        if task_id in self._tasks_with_pending_uploads():
            result.skipped[f"pending_upload_{status}"] += 1
            logger.info(f"[Cleanup] task={task_id} 仍有产物待上传，跳过清理")
            return

        # 规则 6: 硬删
        age_repr = _format_age(age)
        if dry_run:
            logger.info(
                f"[Cleanup][DRY-RUN] task={task_id} status={status} "
                f"age={age_repr} → 会清理"
            )
        else:
            shutil.rmtree(task_dir, ignore_errors=True)
            logger.info(
                f"[Cleanup] task={task_id} status={status} age={age_repr} → 已清理"
            )
        result.purged += 1
        result.purged_task_ids.append(task_id)


def _parse_ok_timestamp(ok_data: dict, ok_path: Path) -> datetime:
    """从 ok.json 取参考时间；缺失或解析失败回退到 ok.json 文件 mtime。"""
    ts_str = ok_data.get("completed_at")
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        except ValueError:
            pass
    return datetime.fromtimestamp(ok_path.stat().st_mtime, tz=timezone.utc)


def _format_age(age: timedelta) -> str:
    """人类可读的 age 格式：'30d'/'1d 5h'/'2h 10m'/'30m'。"""
    total_seconds = int(age.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days >= 1:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours >= 1:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    return f"{minutes}m"


# ─── CLI 入口 ────────────────────────────────────────────────────────────────

def _build_policy_from_args(
    statuses: list[str] | None,
    timeouts: list[str] | None,
) -> dict[str, timedelta]:
    """合并 --status 和 --timeout 为 policy 字典。

    匹配规则（按"出错可能小"排序）：
      - 都不传                          → DEFAULT_POLICY
      - 只传 --status                   → 每个 status 从 DEFAULT_POLICY 取默认 timeout
                                          （命中失败则报错，必须显式 --timeout）
      - 只传 --timeout                  → 报错（孤立的 timeout 没有归属）
      - 都传且 len(timeouts)==1         → 单 timeout 广播到所有 status
      - 都传且 len 相等                  → 一一对应
      - 其他                            → 报错
    """
    if not statuses and not timeouts:
        return dict(DEFAULT_POLICY)
    if timeouts and not statuses:
        raise SystemExit("[ERR] --timeout 必须与 --status 配对使用")

    statuses = statuses or []
    timeouts = timeouts or []

    if not timeouts:
        policy: dict[str, timedelta] = {}
        for s in statuses:
            if s not in DEFAULT_POLICY:
                raise SystemExit(
                    f"[ERR] status='{s}' 不在默认策略中，必须显式提供 --timeout"
                )
            policy[s] = DEFAULT_POLICY[s]
        return policy

    if len(timeouts) == 1:
        td = parse_duration(timeouts[0])
        return {s: td for s in statuses}

    if len(timeouts) == len(statuses):
        return {s: parse_duration(t) for s, t in zip(statuses, timeouts)}

    raise SystemExit(
        f"[ERR] --status 数量({len(statuses)}) 与 --timeout 数量({len(timeouts)}) 不匹配"
    )


def _resolve_work_dir(explicit: str | None) -> str:
    """优先使用 --work-dir 参数，缺省时从 worker_settings.WORK_DIR 读取。"""
    if explicit:
        return explicit
    try:
        from .config import worker_settings
        return worker_settings.WORK_DIR
    except Exception as e:
        raise SystemExit(
            f"[ERR] 未指定 --work-dir 且无法从 webx_worker.config 读取: {e}"
        ) from e


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m webx_worker cleanup",
        description=(
            "任务目录清理工具：扫描 worker 工作目录，对超时的终态任务做硬删除。"
            "判定依据是本地 ok.json（status + server_confirmed + completed_at），无网络依赖。"
        ),
    )
    parser.add_argument(
        "--work-dir",
        help="工作目录路径（缺省时从 worker_settings.WORK_DIR 读取）",
    )
    parser.add_argument(
        "--status", action="append",
        help=(
            "要清理的状态，可重复（如 --status failed --status completed）。"
            "省略则使用默认列表（failed, completed）。"
        ),
    )
    parser.add_argument(
        "--timeout", action="append",
        help=(
            "超时阈值，与 --status 一一对应或单值广播；"
            "支持 d/h/m/s 后缀，纯数字按天计（如 30 / 30d / 3h / 120m / 300s）。"
            "省略则使用默认值（failed=2d, completed=7d）。"
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="只报告会清理什么，不实际删除",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="只输出最终 JSON 结果，抑制 INFO 日志",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    work_dir = _resolve_work_dir(args.work_dir)
    policy = _build_policy_from_args(args.status, args.timeout)

    cleaner = TaskDirCleaner(work_base_dir=work_dir, dry_run=args.dry_run)
    result = cleaner.scan_and_purge(policy=policy)

    # 结构化输出，便于 worker 主进程解析或外部脚本对接
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    sys.exit(0 if not result.errors else 1)


if __name__ == "__main__":
    main()
