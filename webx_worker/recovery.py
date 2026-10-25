"""
WorkerRecovery — 启动恢复模块

Worker 重启后自动扫描本地工作目录，根据 ok.json + 服务端状态协商处理：
  - not_found 标记 → 检查是否超过 7 天 → 过期清理，否则跳过
  - 有 ok.json + confirmed=true → 已确认终态，跳过
  - 有 ok.json + status=completed + confirmed=false → 重试 complete_task
  - 有 ok.json + status=cancelled + confirmed=false → 标记 confirmed
  - 有 ok.json + status=failed + confirmed=false → 不重试（由服务端补偿器兜底，
      避免过期 fail 信号覆盖重投后正在执行的工作）
  - 有 ok.json + status=not_found → 跳过（下次启动再检查过期）
  - 无 ok.json + 服务端 queued/running 且租户是本机 → **主动归还租约**（任务立刻回池）
  - 无 ok.json + 404 → 标记 not_found
  - 无 ok.json + 网络故障 → 保留目录下次再查
  - 无 ok.json + 服务端 completed → 保留目录等专用清理（避免数据丢失）
  - 无 ok.json + 其他状态 → 按插件的 `soft_clean_policy(task_type)` 清理崩溃目录：
      返回名字集合 → 只保留这些条目（软清理，保留昂贵的中间产物）；
      返回 None（默认）→ rmtree 整目录

业务软清理不再写死在这里：保留哪些产物取决于任务类型，由 `TaskKindPlugin.soft_clean_policy`
这个接缝提供（见 plugin.py）。
"""

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .plugin import TaskKindPlugin

logger = logging.getLogger(__name__)

# not_found 标记超过此天数后清理目录
NOT_FOUND_EXPIRE_DAYS = 7

class WorkerRecovery:

    def __init__(self, callback, work_base_dir: str, plugin: TaskKindPlugin | None = None):
        """
        Args:
            callback: WebCallbackClient 实例
            work_base_dir: 工作目录根路径
            plugin: 提供 soft_clean_policy 的插件（缺省为中性实现：一律 rmtree）
        """
        self.callback = callback
        self.work_base_dir = Path(work_base_dir)
        self.plugin = plugin or TaskKindPlugin()

    async def recover(self):
        """扫描本地工作目录，执行恢复逻辑。"""
        if not self.work_base_dir.exists():
            logger.info("[Recovery] 工作目录不存在，跳过恢复")
            return

        # ⚠️ 跳过点开头的目录：**它们不是任务**。
        # WORK_DIR 下除了 {task_id}/ 还住着基础设施目录，最典型的是 uploader 的
        # 磁盘队列 `.uploads/`（见 sdk/upload_queue）。不排除的话，恢复扫描会把
        # 目录名当成 task_id 去查服务端 —— 查不到有意义的结果，然后被归类成
        # 「网络/5xx，保留目录」，于是**每次启动都刷一条查询失败的告警**，
        # 而它既不是网络问题也永远不会好。
        #
        # 用「点开头」这条通用规则而不是硬编码 `.uploads`：将来再加基础设施目录
        # （临时区、锁目录之类）只要沿用点前缀就自动免疫，不必回来改这里。
        task_dirs = sorted(
            d for d in self.work_base_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        )

        if not task_dirs:
            logger.info("[Recovery] 无本地任务目录，跳过恢复")
            return

        logger.info(f"[Recovery] 开始扫描，发现 {len(task_dirs)} 个任务目录")

        recovered = 0
        for task_dir in task_dirs:
            task_id = task_dir.name
            ok_json_path = task_dir / "ok.json"

            try:
                # 前置过滤：not_found 标记检查
                if ok_json_path.exists():
                    ok_data = json.loads(ok_json_path.read_text(encoding="utf-8"))
                    if ok_data.get("status") == "not_found":
                        not_found_at = ok_data.get("not_found_at")
                        if not_found_at and self._is_not_found_expired(not_found_at):
                            logger.info(
                                f"[Recovery] task={task_id} not_found 超过 "
                                f"{NOT_FOUND_EXPIRE_DAYS} 天，清理目录"
                            )
                            shutil.rmtree(task_dir, ignore_errors=True)
                        else:
                            logger.info(
                                f"[Recovery] task={task_id} 已标记 not_found，跳过"
                            )
                        continue

                # 常规恢复逻辑
                if ok_json_path.exists():
                    if await self._handle_with_ok_json(task_id, task_dir, ok_json_path):
                        recovered += 1
                else:
                    if await self._handle_without_ok_json(task_id, task_dir):
                        recovered += 1
            except Exception as e:
                logger.error(
                    f"[Recovery] 处理 task_id={task_id} 异常: {e}",
                    exc_info=True,
                )

        logger.info(f"[Recovery] 恢复完成，处理了 {recovered} 个任务")

    async def _handle_with_ok_json(self, task_id: str, task_dir: Path, ok_json_path: Path) -> bool:
        """处理有 ok.json 的任务目录。

        Returns: True if action was taken
        """
        ok_data = json.loads(ok_json_path.read_text(encoding="utf-8"))
        server_confirmed = ok_data.get("server_confirmed", False)
        status = ok_data.get("status", "")

        if server_confirmed:
            logger.debug(f"[Recovery] task={task_id} 已确认，跳过")
            return False

        # server_confirmed=false，需要重试回调
        if status == "completed":
            # 仅重试状态迁移，result_path/log_path 由任务脚本通过 PATCH /fields 管理
            logger.info(f"[Recovery] task={task_id} 重试 complete 回调")
            success = await self.callback.complete_task(task_id)

            # 若回调失败，查询服务端实际状态
            if not success:
                status_result = await self.callback.get_task_status(task_id)
                if status_result.is_ok and status_result.task_status == "completed":
                    logger.info(
                        f"[Recovery] task={task_id} 服务端已是 COMPLETED，视为已确认"
                    )
                    success = True
                elif status_result.is_not_found:
                    logger.warning(f"[Recovery] task={task_id} 服务端 404，标记 not_found")
                    self._write_not_found_ok_json(task_dir, task_id, ok_data)
                    return True
                # status_result.is_error → 网络故障，下次启动再试

            if success:
                ok_data["server_confirmed"] = True
                ok_data["server_confirmed_at"] = datetime.now(timezone.utc).isoformat()
                ok_json_path.write_text(
                    json.dumps(ok_data, indent=2), encoding="utf-8"
                )
                return True
            else:
                logger.warning(f"[Recovery] task={task_id} 重试 complete 回调失败，下次启动再试")
                return False
        elif status == "cancelled":
            # 取消状态但未确认，标记确认
            ok_data["server_confirmed"] = True
            ok_json_path.write_text(
                json.dumps(ok_data, indent=2), encoding="utf-8"
            )
            logger.info(f"[Recovery] task={task_id} 取消状态已确认")
            return True
        elif status == "failed":
            # failed 故意不在 recovery 重试 fail_task：
            # 重启后服务端可能已被补偿器基于心跳超时重投，
            # 此时补打过期 fail_task 会以"过期失败信号"覆盖正在执行的工作。
            # 失败补偿由服务端补偿器统一兜底，已足够保证终态一致性。
            # 本地 ok.json 仅用于清理模块识别 + 审计诊断。
            logger.warning(
                f"[Recovery] task={task_id} 本地 failed 未确认，"
                f"不主动重试 fail_task（由服务端补偿器兜底）"
            )
            return False
        else:
            logger.warning(f"[Recovery] task={task_id} ok.json 状态未知: {status}")
            return False

    async def _handle_without_ok_json(self, task_id: str, task_dir: Path) -> bool:
        """处理无 ok.json 的任务目录。

        分流策略（按服务端状态 + 任务类型）：
          - 404 → 标记 not_found，不删除（7 天后过期清理）
          - 网络故障 → 保留目录，下次启动再查
          - 服务端 completed → 保留目录，等专用清理统一处理（避免数据丢失）
          - 服务端 failed / cancelled / running / queued / pending →
              按 plugin.soft_clean_policy(task_type)：
              名字集合: 软清理，保留这些昂贵产物，下次执行复用
              None:     rmtree 整目录，由补偿器重投后从零重跑

        task_type 来源优先级：服务端响应 > 本地 meta.json（容错回退）。
        """
        result = await self.callback.get_task_status(task_id)

        if result.is_not_found:
            logger.info(f"[Recovery] task={task_id} 服务端 404，标记 not_found")
            self._write_not_found_ok_json(task_dir, task_id)
            return True

        if result.is_error:
            logger.warning(f"[Recovery] task={task_id} 查询失败(网络/5xx)，保留目录")
            return False

        # result.is_ok → 正常响应
        task_info = result.data or {}
        server_status = task_info.get("status", "")

        # ── 主动归还租约：worker 重启后 5 秒就能确知「这一轮没跑完」 ──────────
        # 「目录里没有 ok.json」这个判据非常干净：任务跑完必写 ok.json，没写就是没跑完。
        # **答案早就在 worker 手里**，过去只是没有安全的渠道传回去 ——
        # 本模块的 docstring 记着当年的顾虑：「不重试（由服务端补偿器兜底，避免过期
        # fail 信号覆盖重投后正在执行的工作）」。那个顾虑完全正确。
        #
        # fencing 是它的正解：归还带 (worker_id, lease_seq)，若任务已被别人领走则
        # lease_seq 不匹配、直接被拒，**物理上不可能覆盖别人的工作**。
        # 顾虑从「必须靠沉默规避」变成「由机制保证」。
        #
        # 收益：恢复耗时从「等一个完整租约周期」压到「worker 重启即刻」。
        # 被拒是正常结果（多半租约早已到期、任务被接管），不重试、不报错。
        if server_status in ("queued", "running"):
            lease_seq = task_info.get("lease_seq")
            worker_id = task_info.get("worker_id") or ""
            from .config import worker_settings

            if lease_seq is not None and worker_id and worker_id == worker_settings.WORKER_ID:
                from .fencing import register as fence_register, unregister as fence_unregister

                fence_register(task_id, int(lease_seq))
                try:
                    await self.callback.release_lease(task_id)
                finally:
                    fence_unregister(task_id)
        # task_type 优先取服务端权威值，缺失时回退本地 meta.json
        task_type = task_info.get("task_type", "") or self._read_task_type_from_meta(task_dir)
        keep = self.plugin.soft_clean_policy(task_type)

        if server_status == "completed":
            # 已完成任务：保留目录，等专用清理脚本统一处理，避免误删用户数据
            logger.info(
                f"[Recovery] task={task_id} 服务端 completed，保留目录等待专用清理脚本"
            )
            return True

        if server_status in ("failed", "cancelled"):
            # 终态非完成：插件要求保留的产物软清理保留（便于 retry 复用）；其他整目录删
            if keep is not None:
                logger.info(
                    f"[Recovery] task={task_id} 服务端 {server_status}({task_type})，"
                    f"软清理保留 {sorted(keep)}（retry 时复用）"
                )
                self._soft_clean_crashed_dir(task_dir, keep)
            else:
                logger.info(
                    f"[Recovery] task={task_id} 服务端 {server_status}"
                    f"({task_type or '未知'})，清理目录"
                )
                shutil.rmtree(task_dir, ignore_errors=True)
            return True

        if server_status in ("running", "queued", "pending"):
            # 崩溃目录：插件要求保留的产物软清理保留，让下次执行复用；其他任务整目录删
            if keep is not None:
                logger.info(
                    f"[Recovery] task={task_id} 服务端 {server_status}({task_type})，"
                    f"软清理崩溃目录保留 {sorted(keep)}（由补偿器重投）"
                )
                self._soft_clean_crashed_dir(task_dir, keep)
            else:
                logger.info(
                    f"[Recovery] task={task_id} 服务端 {server_status}"
                    f"({task_type or '未知'})，清理崩溃目录（由补偿器重投）"
                )
                shutil.rmtree(task_dir, ignore_errors=True)
            return True

        logger.warning(f"[Recovery] task={task_id} 未知服务端状态: {server_status}")
        return False

    @staticmethod
    def _read_task_type_from_meta(task_dir: Path) -> str:
        """从本地 meta.json 读 task_type，作为服务端字段缺失的回退来源。"""
        meta_path = task_dir / "meta.json"
        if not meta_path.exists():
            return ""
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            return meta.get("task_type", "")
        except (json.JSONDecodeError, OSError):
            return ""

    @staticmethod
    def _soft_clean_crashed_dir(task_dir: Path, keep: frozenset[str]) -> None:
        """崩溃目录的软清理：删脏产物 + 元数据，只保留 `keep` 里列出的名字。

        保留（keep，由插件的 soft_clean_policy 给出）：重算代价高昂的中间产物。

        删除：
          - in/, out/ 等阶段产物与输入                    → 重投覆盖
          - params.json, files.json, meta.json            → 元数据（executor 重投重写）
          - ok.json                                       → 避免下次 recovery 误判
          - 其他临时文件                                    → 一并清理，保持目录干净
        """
        if not task_dir.exists():
            return
        for child in task_dir.iterdir():
            if child.name in keep:
                continue
            try:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
            except OSError as e:
                logger.warning(f"[Recovery] 软清理 {child} 失败: {e}")

    def _write_not_found_ok_json(
        self, task_dir: Path, task_id: str, base_data: dict | None = None
    ) -> None:
        """写入 not_found 状态的 ok.json，记录发现时间供过期清理判断。"""
        ok_data = base_data or {}
        ok_data.update({
            "task_id": task_id,
            "status": "not_found",
            "not_found_at": datetime.now(timezone.utc).isoformat(),
            "server_confirmed": False,
        })
        # 保留多阶段信息（如果有）
        meta_path = task_dir / "meta.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                ok_data.setdefault("steps", meta.get("steps", []))
                ok_data.setdefault("sub_step", meta.get("sub_step", ""))
                ok_data.setdefault("end_step", meta.get("end_step", ""))
            except (json.JSONDecodeError, OSError):
                pass
        try:
            (task_dir / "ok.json").write_text(
                json.dumps(ok_data, indent=2), encoding="utf-8"
            )
        except Exception as e:
            logger.error(f"[Recovery] 写入 not_found ok.json 失败: {task_id}: {e}")

    def _is_not_found_expired(self, not_found_at: str) -> bool:
        """检查 not_found 标记是否超过过期天数。"""
        try:
            marked_time = datetime.fromisoformat(not_found_at)
            now = datetime.now(timezone.utc)
            if marked_time.tzinfo is None:
                marked_time = marked_time.replace(tzinfo=timezone.utc)
            elapsed_days = (now - marked_time).days
            return elapsed_days >= NOT_FOUND_EXPIRE_DAYS
        except (ValueError, TypeError) as e:
            logger.warning(f"[Recovery] 解析 not_found_at 失败: {not_found_at}, {e}")
            return False