"""保留期清理：可插拔的策略 + 一轮扫描 + 独立进程的循环。

**为什么是独立进程而不是应用里的后台协程**：存储服务以多 worker 运行，挂在应用里的
后台任务会每个 worker 各跑一份，几个进程同时扫同一批目录、互相删对方正在检查的东西。
独立进程只有一份，天然无竞争。

**默认关闭**（``STORAGE_RETENTION_ENABLED``）。删除不可逆，所以：
  - ``--dry-run`` 只列出将删除的条目，不动文件；
  - 单轮有上限（``STORAGE_RETENTION_MAX_PER_ROUND``），一次配置失误删不空整个存储；
  - ``RetentionPolicy.confirm`` 是删前确认钩子——"该不该留"的权威方（通常是业务主站，
    它有容量、用量、购买时长这些数据）接进来后，必须守住一条：**权威方不可达时绝不删**。
    查不到 ≠ 不存在。

纯 mtime 策略（``MtimeRetention``）只是兜底，没有任何业务数据可参考，别把它当权威。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from webx.lib.storage import ObjectStore

logger = logging.getLogger("webx_storage.retention")


@dataclass
class Candidate:
    """一个可清理单元（如一个任务目录）。"""

    id: str
    path: Path


class RetentionPolicy(ABC):
    """保留期策略。"""

    @abstractmethod
    def candidates(self, store: ObjectStore) -> Iterable[Candidate]:
        """枚举本策略管辖范围内的清理单元。**不得越界**到别的策略/业务方的数据。"""

    @abstractmethod
    def expired(self, c: Candidate, now: float) -> bool:
        """是否已超过保留期（判据由策略自定）。"""

    def confirm(self, c: Candidate) -> bool:
        """删前确认钩子。返回 False = 不删。默认直接放行。

        接入权威方时：权威方不可达 / 超时 / 返回非预期，一律返回 False。
        """
        return True

    @abstractmethod
    def remove(self, store: ObjectStore, c: Candidate) -> bool:
        """执行删除，返回是否真的删掉了。"""


def _latest_mtime(path: Path) -> float:
    """目录自身与直属子项里最新的 mtime（快速路径：绝大多数「还新」的单元在这一步就保留了）。"""
    try:
        newest = path.stat().st_mtime
    except OSError:
        return 0.0
    try:
        for child in path.iterdir():
            newest = max(newest, child.stat().st_mtime)
    except OSError:
        pass
    return newest


def _all_older_than(path: Path, cutoff: float) -> bool:
    """递归确认整棵树里没有比 cutoff 更新的文件。**宁可多走一趟，也不能误删**。"""
    try:
        for f in path.rglob("*"):
            try:
                if f.stat().st_mtime >= cutoff:
                    return False
            except OSError:
                continue
    except OSError:
        return False
    return True


class MtimeRetention(RetentionPolicy):
    """按目录树里**最新的 mtime** 判龄（不是创建时间——还在被追加产物的单元不该被删）。

    只管辖 ``<scope>/`` 下的一级子目录，绝不碰存储里的其它区域。
    """

    def __init__(self, scope: str, days: float):
        self.scope = scope
        self.days = days

    def candidates(self, store: ObjectStore) -> Iterable[Candidate]:
        base = store.root / self.scope
        if not base.is_dir():
            return
        for d in sorted(base.iterdir()):
            if d.is_dir():
                yield Candidate(id=d.name, path=d)

    def expired(self, c: Candidate, now: float) -> bool:
        cutoff = now - self.days * 86400.0
        if _latest_mtime(c.path) >= cutoff:
            return False  # 快速路径：还新
        return _all_older_than(c.path, cutoff)  # 递归确认

    def remove(self, store: ObjectStore, c: Candidate) -> bool:
        return store.delete_prefix(f"{self.scope}/{c.id}")


def sweep(
    store: ObjectStore,
    policy: RetentionPolicy,
    *,
    max_per_round: int = 0,
    dry_run: bool = False,
    now: float | None = None,
) -> tuple[int, int]:
    """扫一轮。返回 ``(检查数, 删除数)``；dry_run 时第二项是"将删除数"。"""
    now = time.time() if now is None else now
    checked = removed = 0
    for c in policy.candidates(store):
        checked += 1
        try:
            if not policy.expired(c, now) or not policy.confirm(c):
                continue
            if dry_run:
                logger.info("[dry-run] 将清理: %s", c.id)
                removed += 1
            elif policy.remove(store, c):
                removed += 1
                logger.info("已清理超期单元: %s", c.id)
        except Exception as e:  # noqa: BLE001 —— 单个失败不能中断整轮
            logger.warning("清理失败 %s: %s", c.id, e)
        if max_per_round and removed >= max_per_round:
            logger.info("本轮达到上限 %d，剩余留待下一轮", max_per_round)
            break
    return checked, removed


async def run_loop(
    store: ObjectStore, policies: list[RetentionPolicy], interval_hours: float, max_per_round: int
) -> None:
    """循环清理，直到收到 SIGINT/SIGTERM。注意：先 sweep 再 sleep，启动即开始删除。"""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # 非 POSIX 平台
            pass

    logger.info("清理器启动：dir=%s 每 %s 小时一轮", store.root, interval_hours)
    while not stop.is_set():
        for policy in policies:
            try:
                checked, removed = sweep(store, policy, max_per_round=max_per_round)
                logger.info("清理完成: 检查 %d 个，删除 %d 个", checked, removed)
            except Exception as e:  # noqa: BLE001 —— 单轮失败不该让清理器退出
                logger.error("清理轮次异常: %s", e, exc_info=True)
        try:
            # wait_for 而不是 sleep：收到信号能立刻退出，不用等满一个周期
            await asyncio.wait_for(stop.wait(), timeout=interval_hours * 3600.0)
        except asyncio.TimeoutError:
            pass
    logger.info("清理器已停止")


def main(argv: list[str] | None = None) -> int:
    from webx.config import settings
    from webx_storage.config import configure
    from webx_storage.policy import build_policies

    p = argparse.ArgumentParser(prog="python -m webx_storage retention", description="存储保留期清理")
    p.add_argument("--once", action="store_true", help="只跑一轮就退出（手动执行 / 排查）")
    p.add_argument("--dry-run", action="store_true", help="只列出将删除的条目，不删（隐含 --once）")
    args = p.parse_args(argv)

    configure()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    policies = [r for r in (pol.retention() for pol in build_policies(settings.STORAGE_POLICIES)) if r]
    if not policies:
        print("没有任何已启用的键策略提供保留期清理，退出")
        return 0
    store = ObjectStore(settings.storage_path, settings.STORAGE_KEY_RULE)

    if args.once or args.dry_run:
        for policy in policies:
            checked, removed = sweep(
                store, policy, max_per_round=settings.STORAGE_RETENTION_MAX_PER_ROUND, dry_run=args.dry_run
            )
            print(f"检查 {checked} 个，{'将删除' if args.dry_run else '删除'} {removed} 个")
        return 0

    if not settings.STORAGE_RETENTION_ENABLED:
        print("STORAGE_RETENTION_ENABLED 未开启，清理器不启动（删除不可逆，必须显式开启）")
        return 0
    if settings.STORAGE_RETENTION_DAYS <= 0:
        print("STORAGE_RETENTION_DAYS <= 0，清理器不启动")
        return 0
    try:
        asyncio.run(
            run_loop(store, policies, settings.STORAGE_RETENTION_INTERVAL_HOURS,
                     settings.STORAGE_RETENTION_MAX_PER_ROUND)
        )
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
