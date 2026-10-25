"""GenericTaskExecutor — 通用任务执行器。

与具体业务逻辑完全解耦，负责：
  - 创建任务工作目录
  - 从任务服务 Internal API 下载输入文件
  - 解析参数（params_text 直接是 JSON）
  - 写入 params.json 和 meta.json（含多阶段进度字段）
  - 启动/停止心跳（双向心跳，感知取消）
  - 构建并执行命令（通过配置的 conda / uv / 直接解释器）
  - ok.json 持久化协议（先写后回调）
  - 取消检测（queued 阶段 + 子进程运行期间）

与任务类型相关的少数规则（日志 key、输入文件筛选、环境变量注入）经 `TaskKindPlugin`
接缝注入，见 plugin.py。

⚠️ 本文件是旧版 executor.py / executor_o.py（二者仅因 Python 版本二选一）合并后的**唯一**
一份。`asyncio.wait(timeout=)` 在 3.10 / 3.11+ 通用，不需要两份实现；
两份并存曾三次漂移（令牌不注入、日志 key 口径、心跳注册顺序），且每次都只在某个
Python 版本上发作。
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from . import envnames as E
from ._paths import with_package_on_pythonpath
from .config import worker_settings
from .fencing import lease_seq_of as fence_lease_seq_of
from .log_writer import LineAssembler
from .plugin import TaskKindPlugin

logger = logging.getLogger(__name__)

# 子进程被 SIGKILL 后，等 drain 读完残余输出的兜底时长（秒）。模块常量，测试可压短。
_DRAIN_GRACE_SEC = 2.0


class GenericTaskExecutor:
    """通用任务执行器 — 与具体业务完全解耦。

    依赖注入（由 app.py 构建时传入）：
        callback       : webx_worker.callback.WebCallbackClient 实例
        log_writer     : webx_worker.log_writer.ConcurrentLogWriter 实例
        heartbeat      : webx_worker.heartbeat.HeartbeatManager 实例
        http_client    : webx_worker.http_file_client.HttpFileClient 实例
        work_base_dir  : 临时工作目录根路径
        channels_config: CHANNELS 配置字典
        conda_root     : conda 根路径
        plugin         : TaskKindPlugin（缺省为中性实现）
        script_root    : 相对脚本路径的解析根（缺省 = 当前目录）
    """

    def __init__(
        self,
        callback,
        log_writer,
        heartbeat,
        http_client,
        work_base_dir: str,
        channels_config: dict,
        conda_root: str,
        ipc_sock_path: str | None = None,
        plugin: TaskKindPlugin | None = None,
        script_root: str | Path | None = None,
    ) -> None:
        self.callback = callback
        self.log_writer = log_writer
        self.heartbeat = heartbeat
        self.http_client = http_client
        self.work_base_dir = Path(work_base_dir)
        self.work_base_dir.mkdir(parents=True, exist_ok=True)
        self.channels = channels_config
        self.conda_root = conda_root
        self.plugin = plugin or TaskKindPlugin()
        self.script_root = Path(script_root) if script_root else Path.cwd()
        # IPC socket 路径：子进程通过 WEBX_WORKER_IPC_SOCK 环境变量得知
        # 由 app.py 启动 IPCServer 时传入；None 表示未启用 IPC（子进程会降级写文件）
        self.ipc_sock_path = ipc_sock_path
        # 增量日志推送（心跳节拍驱动）的去冗余状态，按 task_id 隔离：
        #   _streamed_marks: 上次推送时的内容修订号（写入次数，含 \r 活动行刷新），
        #                    未变化则跳过本次上传。用修订号而非行数：进度行原地刷新时
        #                    行数不变、内容在变，按行数判断会让长程任务期间日志假死。
        #   _streamed_fields: 已写过 log_path 字段的任务（URL 稳定，字段仅首次写入）
        self._streamed_marks: dict[str, int] = {}
        self._streamed_fields: set[str] = set()

    async def execute(self, task_id: str, payload: dict) -> None:
        """完整执行一个任务的生命周期。"""
        task_dir = self.work_base_dir / task_id
        in_dir = task_dir / "in"
        out_dir = task_dir / "out"
        in_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)

        # 接收新任务（含 retry/redo/补偿重投）：删除旧 ok.json，避免上一轮的
        # 终态标记误导 recovery（让其错过崩溃目录）与清理模块（按过期时间戳误清）。
        # 即便后续 worker 崩溃，recovery 会走"无 ok.json"分支按服务端状态正确兜底。
        old_ok = task_dir / "ok.json"
        if old_ok.exists():
            try:
                old_ok.unlink()
            except OSError as e:
                logger.warning(f"清理旧 ok.json 失败 task_id={task_id}: {e}")

        task_type = payload.get("task_type", "unknown")
        steps = payload.get("steps") or []
        sub_step = payload.get("sub_step") or ""
        end_step = payload.get("end_step") or ""

        # 上传日志的 key / 文件名一次算定（口径由插件决定），
        # 流式增量推送与末尾上传共用，保证两条路径口径一致、URL 稳定。
        # 注意与下方 run_subprocess 的本地落盘名 log_filename（run_{sub_step}.log）区分：
        # 前者是上传到云端的 results 文件名 + log_path key，后者只是 worker 本地 stdout 文件。
        log_key, log_upload_name = self.plugin.log_target(task_type, sub_step, steps)

        logger.info(f"[{task_type}] 开始处理 task_id={task_id}"
                     + (f" steps={steps} sub_step={sub_step} end_step={end_step}" if end_step else ""))

        try:
            # 0. 执行前检查服务端任务状态（区分 404 / cancelled / 网络故障）
            result = await self.callback.get_task_status(task_id)
            if result.is_not_found:
                logger.warning(f"任务不存在(task 404)，跳过执行: task_id={task_id}")
                self._write_ok_json(task_dir, task_id, "not_found", server_confirmed=False,
                                    steps=steps, sub_step=sub_step, end_step=end_step,
                                    not_found_at=datetime.now(timezone.utc).isoformat())
                return
            if result.is_ok and result.task_status == "cancelled":
                logger.warning(f"任务已被取消，跳过执行: task_id={task_id}")
                self._write_ok_json(task_dir, task_id, "cancelled", server_confirmed=True,
                                    steps=steps, sub_step=sub_step, end_step=end_step)
                return
            # result.is_error → 网络故障，不阻塞，继续执行

            # 1. 启动心跳 —— **必须在下载之前**。
            #
            # 认领后的租约是 短租约 + grace，而此刻 start_time 还是 NULL，
            # grace 取下限 ⇒ 预算有限。下载放在心跳之前的话，
            # 一个大输入任务会在**一台完全健康的 worker 正在启动它**的时候租约到期、
            # 被回收给别人，然后两份并发跑。心跳提前 = 下载期间租约持续被续。
            #
            # ⚠️ 配套前提：正常路径上 RUNNING 已由 lease 分发器在 execute 之前上报
            # （lease.py 的「上报 running —— 不可省」那段），但那次上报可能失败重试、
            # 也可能整段被跳过（补偿重投），此刻服务端仍答 queued。而心跳循环的
            # 取消判据是「status != running」，故 heartbeat 侧有「起跑前的 queued 视为
            # 存活」的判定（见 heartbeat._heartbeat_loop）——少了那一半，心跳前移会让
            # 这类任务在第一跳就自杀。
            await self.heartbeat.register(
                task_id,
                on_beat=partial(self._stream_log, log_key=log_key, log_filename=log_upload_name),
            )
            self.heartbeat.seed_lease(task_id, self.callback.pop_ack_lease_remaining_ms(task_id))

            # 2. 下载输入文件
            await self._download_inputs(task_id, payload, in_dir, task_type)

            # 3. 解析参数 -> params.json
            params = await self._resolve_params(payload)
            (task_dir / "params.json").write_text(
                json.dumps(params, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            # 3.5 写入文件清单 -> files.json（含元信息，供任务脚本读取）
            raw_files = payload.get("files_json") or payload.get("files")
            if raw_files:
                if isinstance(raw_files, str):
                    files_data = json.loads(raw_files)
                elif isinstance(raw_files, list):
                    files_data = raw_files
                else:
                    files_data = []
                if files_data:
                    (task_dir / "files.json").write_text(
                        json.dumps(files_data, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )

            # 4. 写入元数据（含多阶段进度字段，供 Recovery 和任务脚本读取）
            (task_dir / "meta.json").write_text(
                json.dumps(
                    {
                        "task_id": task_id,
                        # 任务名：服务端 payload 透传，仅供任务脚本【只读】使用。
                        "name": payload.get("name", "") or "",
                        "task_type": task_type,
                        "steps": steps,
                        "sub_step": sub_step,
                        "end_step": end_step,
                        "timeout_seconds": payload.get("timeout_seconds", 172800),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            # 5. 构建并执行命令（追加 --task-id 参数，供 Python 脚本读取）
            cmd = self._build_cmd(task_type, task_id)
            timeout = payload.get("timeout_seconds", 172800)

            # channel 配置的 run_env：机器级环境变量（GPU/线程绑定等），注入子进程
            run_env = self._resolve_run_env(task_type)
            # 插件追加的环境变量（在 fencing 令牌之前注入，令牌最后写入、不可被覆盖）
            run_env.update(
                {str(k): str(v) for k, v in
                 (self.plugin.extend_env(task_type, task_id, dict(run_env)) or {}).items()}
            )
            # fencing 令牌：任务子进程经 sdk.result_client 直接调内部端点，
            # 拿不到父进程的内存，只能靠环境变量。**漏了这步，子进程的所有上报都会被
            # fencing 拒掉** —— 而步骤进度 / 字段更新 / 产物交付这些主力上报恰恰全在子进程里。
            # 无租约时 env_for 返回空 dict，不注入任何东西（此时服务端会明确拒绝子进程的写入）。
            from .fencing import env_for
            run_env.update(env_for(task_id))

            log_filename = f"run_{sub_step}.log" if sub_step else "run.log"
            returncode = await self.run_subprocess(
                cmd, task_id, task_dir, log_filename=log_filename,
                timeout=timeout, env=run_env, heartbeat=self.heartbeat,
            )

            # 检查子进程是否因服务端信号而终止（cancelled / not_found）
            if returncode == -1:
                srv_status = self.heartbeat.get_server_status(task_id)
                if srv_status == "not_found":
                    logger.info(f"[{task_type}] 任务 404(not_found) task_id={task_id}")
                    self._write_ok_json(task_dir, task_id, "not_found", server_confirmed=False,
                                        steps=steps, sub_step=sub_step, end_step=end_step,
                                        not_found_at=datetime.now(timezone.utc).isoformat())
                else:
                    logger.info(f"[{task_type}] 任务被取消 task_id={task_id}")
                    self._write_ok_json(task_dir, task_id, "cancelled", server_confirmed=True,
                                        steps=steps, sub_step=sub_step, end_step=end_step)
                await self._collect_log(task_id)
                return

            if returncode != 0:
                raise RuntimeError(f"任务脚本退出码: {returncode}")

            # 6. 上传步骤日志到云端（在 _collect_log 关闭 writer 之前）
            await self._upload_step_log(task_id, log_key, log_upload_name)

            # 7. 收集本地路径（仅用于 ok.json 诊断记录，不写入 DB）
            output_files = list(out_dir.glob("*"))
            local_result_path = str(out_dir) if output_files else ""
            local_log_path = await self._collect_log(task_id) or ""

            # 8. ok.json 持久化协议：记录本地路径供恢复诊断
            self._write_ok_json(task_dir, task_id, "completed", server_confirmed=False,
                                steps=steps, sub_step=sub_step, end_step=end_step,
                                result_path=local_result_path, log_path=local_log_path or "")

            # 9. 回调：仅触发状态迁移，不传业务数据
            try:
                success = await self.callback.complete_task(task_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                # 产物与 ok.json(completed) 都已就绪，只是通知没送达：绝不能落进下面的“任务失败”分支
                # （那会把已完成的计算作废、覆盖成 failed 并归还租约，整个任务重跑）。保持未确认，交给 recovery / 补偿器
                logger.error(f"[{task_type}] 完成回调异常，保持 completed 未确认 task_id={task_id}: {e}")
                success = False

            if success:
                self._write_ok_json(task_dir, task_id, "completed", server_confirmed=True,
                                    steps=steps, sub_step=sub_step, end_step=end_step,
                                    result_path=local_result_path, log_path=local_log_path or "")

            logger.info(f"[{task_type}] 任务完成 task_id={task_id}")

        except asyncio.CancelledError:
            # ⚠️ **刻意不写 ok.json**，这条分支只有「进程正在退出」能触达：
            # Ctrl-C 时 asyncio.run 的 _cancel_all_tasks，以及停机时
            # lease.cancel_running_tasks 的同步取消。两者都意味着**这一轮被打断了、
            # 什么都没确认**，而原先写的是 `server_confirmed=True` 的终态标记 ——
            # 那是在撒谎，代价很实在：recovery 的第一句就是
            # `if server_confirmed: return False`，于是重启扫描**整个跳过这个目录**：
            #   · 「服务端还说 queued/running 且租户是我 → 主动归还租约」不做了
            #     ⇒ 归还退化成等补偿器的租约到期（而非重启后几秒）；
            #   · 崩溃目录的软清理也不做了 ⇒ 半成品留着进下一轮。
            #
            # 不写，recovery 就走 `_handle_without_ok_json` —— 按服务端状态分流、
            # 必要时带 fencing 归还租约。正是被打断的这一轮该走的路。
            #
            # 措辞也一并改掉：「被取消」在本框架覆盖三种成因（用户取消 / 自熔
            # lease_expired / 进程退出），排障时读到会误判成用户操作。
            logger.warning(f"[{task_type}] 任务被中断（进程退出中）task_id={task_id}")
            raise
        except Exception as e:
            logger.error(
                f"[{task_type}] 任务失败 task_id={task_id}: {e}",
                exc_info=True,
            )
            # 将进程级错误和日志尾部摘要合并到 error_message
            error_msg = await self._build_error_message(task_id, str(e))
            # 上传步骤日志（在 _collect_log 关闭 writer 之前）
            await self._upload_step_log(task_id, log_key, log_upload_name)
            # 保存截断日志到本地（诊断用）
            await self._collect_log(task_id)
            # 先写 ok.json 作为本地标识（含 error_message 供清理/审计），再调用 fail_task。
            # 注意：fail_task 调用失败时不在 recovery 重试，由补偿器心跳超时兜底，
            # 避免过期失败信号覆盖重投后正在执行的工作（详见 recovery._handle_with_ok_json）。
            self._write_ok_json(task_dir, task_id, "failed", server_confirmed=False,
                                steps=steps, sub_step=sub_step, end_step=end_step,
                                error_message=error_msg)
            if await self.callback.fail_task(task_id, error_message=error_msg):
                self._mark_ok_confirmed(task_dir)
        finally:
            await self.heartbeat.unregister(task_id)
            # 清理增量推送状态，避免长生命周期 executor 跨任务累积
            self._streamed_marks.pop(task_id, None)
            self._streamed_fields.discard(task_id)

    async def _stream_log(self, task_id: str, log_key: str, log_filename: str) -> None:
        """心跳节拍驱动的增量日志上传（执行期间周期性推送，让云端进度实时可见）。

        log_key / log_filename 由 execute() 经 `plugin.log_target` 算好传入。
        与末尾的 _upload_step_log 同源、同口径，体积有界（头 N + 尾 M 行），
        区别在于本方法在子进程运行中被心跳反复调用。
        去冗余：内容修订号未变则跳过；文件名稳定（同名覆盖）→ URL 稳定 → log_path
        字段仅首次写入。本方法绝不抛异常（由 heartbeat._fire_on_beat 兜底），
        日志推送失败不得影响心跳与取消。
        """
        mark = self.log_writer.get_write_count(task_id)
        if mark == 0 or mark == self._streamed_marks.get(task_id):
            return  # 尚无输出 / 自上次推送内容未变 → 跳过

        content = self.log_writer.get_truncated_content(task_id)
        if not content:
            return

        # 流式增量上传用 35s 超时（短于终态产物的 60s）：避免单次卡住长时间占住
        # heartbeat 的"在途"槽、拖延后续合并补发
        download_url = await self.callback.upload_result_file(
            task_id, log_filename, content.encode("utf-8"), timeout=35.0
        )
        if not download_url:
            return  # 上传失败，不更新行数水位，下次心跳重试
        self._streamed_marks[task_id] = mark

        # URL 稳定：仅首次写 log_path 字段，后续覆盖同名文件内容即可，省一次字段写
        if task_id not in self._streamed_fields:
            log_entry = json.dumps({log_key: download_url}, ensure_ascii=False)
            # **写成功才置位**：这个集合的含义是「这一格已经有 URL 了」。
            # 写失败也置位等于自己断了后路——本任务后续心跳不再尝试，只剩步骤末尾
            # _upload_step_log 那一发；成功才置位则下一跳自动重来，白捡一层自愈。
            if await self.callback.update_task_fields(
                task_id, log_path=log_entry, mode="merge"
            ):
                self._streamed_fields.add(task_id)
        logger.debug(
            f"增量日志已推送: task_id={task_id}, step={log_key}, "
            f"lines={self.log_writer.get_total_lines(task_id)}"
        )

    def _write_ok_json(
        self, task_dir: Path, task_id: str, status: str,
        server_confirmed: bool,
        steps: list = None, sub_step: str = "", end_step: str = "",
        result_path: str = "", log_path: str = "",
        not_found_at: str | None = None,
        error_message: str = "",
    ) -> None:
        """写入 ok.json 持久化协议文件（本地完成标记，见 docs/worker.md）。

        completed_at 字段对所有 status 表示"事件记录时刻"（failed/cancelled/not_found
        共用此字段名，保持向后兼容）。failed 状态额外持久化 error_message 用于审计与诊断，
        但 recovery 不会用它重试 fail_task —— 失败补偿由服务端补偿器兜底（避免过期信号
        覆盖重投后正在执行的工作）。
        """
        ok_data = {
            "task_id": task_id,
            "status": status,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "server_confirmed": server_confirmed,
            "steps": steps or [],
            "sub_step": sub_step,
            "end_step": end_step,
            "result_path": result_path,
            "log_path": log_path,
        }
        # 租约令牌随终态标记落盘：进程崩溃后重启，注册表是空的，recovery 重试 complete 时
        # 没有它就是无令牌写，按强制 fencing 必被拒（见 recovery 的临时登记）
        seq = fence_lease_seq_of(task_id)
        if seq is not None:
            ok_data["worker_id"] = worker_settings.WORKER_ID
            ok_data["lease_seq"] = seq
        if server_confirmed:
            ok_data["server_confirmed_at"] = datetime.now(timezone.utc).isoformat()
        if not_found_at:
            ok_data["not_found_at"] = not_found_at
        if error_message:
            ok_data["error_message"] = error_message
        try:
            (task_dir / "ok.json").write_text(
                json.dumps(ok_data, indent=2), encoding="utf-8"
            )
        except Exception as e:
            logger.error(f"写入 ok.json 失败 task_id={task_id}: {e}")

    def _mark_ok_confirmed(self, task_dir: Path) -> None:
        """将 ok.json 中的 server_confirmed 增量更新为 True。

        相比 _write_ok_json 整文件重写更轻量、不易丢字段，
        适用于"已经写过 ok.json + 回调成功"的二阶段流程。
        """
        ok_path = task_dir / "ok.json"
        if not ok_path.exists():
            return
        try:
            data = json.loads(ok_path.read_text(encoding="utf-8"))
            data["server_confirmed"] = True
            data["server_confirmed_at"] = datetime.now(timezone.utc).isoformat()
            ok_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"标记 ok.json confirmed 失败 path={ok_path}: {e}")

    async def _resolve_params(self, payload: dict) -> dict:
        """解析 params_text：直接 JSON 数据。"""
        raw = payload.get("params_text", "{}")

        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(f"params_text 解析失败: {raw!r}")
                return {}
        return {}

    async def _download_inputs(
        self, task_id: str, payload: dict, in_dir: Path, task_type: str = ""
    ) -> None:
        """从任务服务下载输入文件到 in/ 目录。

        哪些条目需要下载由 `plugin.select_inputs` 决定（缺省：名字非空的全部）；
        条目里「只携带数据、不对应实际文件」的规格类条目应在插件里被滤掉。
        """
        raw = payload.get("files_json") or payload.get("files")
        if not raw:
            logger.debug("无输入文件需要下载")
            return

        if isinstance(raw, str):
            files_list = json.loads(raw)
        elif isinstance(raw, list):
            files_list = raw
        else:
            logger.warning(f"未知的 files 格式: {type(raw)}")
            return

        if not isinstance(files_list, list):
            logger.warning(f"files_json 应为数组格式，实际为: {type(files_list)}")
            return

        for f in self.plugin.select_inputs(task_type, files_list):
            filename = f.get("name", "")
            local_path = in_dir / filename
            logger.debug(f"下载 {filename} -> {local_path}")
            success = await self.http_client.download_file(
                task_id, filename, str(local_path)
            )
            if not success:
                raise RuntimeError(f"下载文件失败: {filename}")

    def _resolve_run_env(self, task_type: str) -> dict:
        """取出 channel 配置的 run_env（机器级运行时环境变量）。

        run_env 用于把 GPU/CPU 绑定等"机器属性"与代码、任务参数解耦：
        同一目录下用不同配置文件 + 各自 run_env，即可跑多个绑定不同设备的 worker。
        值统一强制为 str（环境变量要求），缺省返回空字典。
        """
        run_env = self.channels.get(task_type, {}).get("run_env") or {}
        return {str(k): str(v) for k, v in run_env.items()}

    def _build_cmd(self, task_type: str, task_id: str = "") -> list[str]:
        """构建执行命令。"""
        from .channel_loader import build_cmd_prefix

        if task_type not in self.channels:
            raise RuntimeError(f"未配置任务类型 {task_type} 的执行环境")

        config = self.channels[task_type]
        script = config["script"]

        # 相对路径按 script_root 解析（绝对路径原样保留：Path / 绝对路径 = 绝对路径）
        script_path = self.script_root / script

        if not script_path.exists():
            raise FileNotFoundError(f"任务脚本不存在: {script_path}")

        if script.endswith(".sh"):
            return ["bash", str(script_path)]
        elif script.endswith(".js"):
            return ["node", str(script_path)]
        else:
            prefix = build_cmd_prefix(config, self.conda_root)
            # conda run 直接 exec 脚本文件，需要 +x 权限；
            # 显式加 python 解释器避免权限问题
            if prefix and prefix[0] == "conda":
                cmd = prefix + ["python", str(script_path)]
            else:
                cmd = prefix + [str(script_path)]
            if task_id:
                cmd += ["--task-id", task_id]
            return cmd

    async def run_subprocess(
        self,
        cmd: list[str],
        task_id: str,
        work_dir: Path,
        log_filename: str = "run.log",
        env: dict | None = None,
        timeout: int = 7200,
        heartbeat=None,
    ) -> int:
        """异步运行子进程，将 stdout/stderr 写入截断日志。

        Returns:
            子进程退出码。-1 表示因取消信号而终止。
        """
        log_path = work_dir / log_filename
        merged_env = {**os.environ, **(env or {})}
        # 子进程要能 import 本包的 SDK（它可能由另一个解释器启动，sys.path 里没有本包）
        merged_env = with_package_on_pythonpath(merged_env)

        # 注入认证和连接信息到子进程环境变量，供任务脚本调用内部 API
        merged_env[E.ENV_INTERNAL_TOKEN] = self.callback.internal_token
        merged_env[E.ENV_WEB_BASE_URL] = self.callback.base_url
        # 存储服务【对外】URL（落库 download_url 展示用）。未配置则为空串（没有回退到任务服务）。
        merged_env[E.ENV_STORAGE_URL] = self.callback.storage_url
        # 存储服务【内网】URL（worker 实际上传/下载请求走此地址）。callback 已收口「未配置回退对外 URL」。
        merged_env[E.ENV_STORAGE_INTERNAL_URL] = self.callback.storage_internal_url
        # 存储服务专属 Token：仅当单独配置时才注入（未配置时子进程自己回退到主 Token）
        storage_token = worker_settings.STORAGE_INTERNAL_TOKEN.strip()
        if storage_token:
            merged_env[E.ENV_STORAGE_INTERNAL_TOKEN] = storage_token
        # 注入工作目录根路径（配置文件层只对主进程可见，子进程继承不到）
        merged_env[E.ENV_WORK_DIR] = str(self.work_base_dir)
        # 异步上传（uploader）开关与体积门槛：子进程据此决定"同步传"还是"投队列"。
        merged_env[E.ENV_UPLOADER_ENABLED] = (
            "true" if worker_settings.UPLOADER_ENABLED else "false"
        )
        merged_env[E.ENV_UPLOAD_ASYNC_THRESHOLD_MB] = str(
            worker_settings.UPLOAD_ASYNC_THRESHOLD_MB
        )
        merged_env[E.ENV_UPLOAD_REQUIRED_GLOBS] = worker_settings.UPLOAD_REQUIRED_GLOBS
        # 注入 IPC socket 路径：子进程的 ipc_client 会读这个变量发汇报失败事件
        if self.ipc_sock_path:
            merged_env[E.ENV_IPC_SOCK] = self.ipc_sock_path

        logger.info(f"启动子进程 task_id={task_id} cmd={' '.join(cmd)}")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(work_dir),
            env=merged_env,
            # 子进程成为新会话 / 新进程组的组长（等价于旧版的 preexec_fn=os.setsid）。
            # 改用 start_new_session 的原因：preexec_fn 在多线程进程里不安全
            # （CPython 文档明确警告可能死锁），而本进程会被嵌进带线程的宿主/测试里；
            # start_new_session 由 C 层直接调用 setsid，语义相同、没有这个风险。
            start_new_session=True,
        )

        # 取消信号：心跳探到 404/cancelled 时 set() 该 Event。下面把它与"读 stdout 的
        # drain 协程"并发竞态——被 set 即立即终止子进程，**与子进程是否输出 stdout 无关**
        # （根治"靠 stdout 节奏轮询状态、静默子进程迟迟杀不掉"的不稳定）。
        cancel_event = heartbeat.get_cancel_event(task_id) if heartbeat else None

        async def _drain_stdout() -> None:
            """读 stdout 写日志直到 EOF。**只管日志，不做取消判定**（取消走 Event）。

            ⚠️ 不用 `async for line in proc.stdout`：StreamReader.readline 默认 64KiB 行长
            上限，而一些程序（如 `mdrun -v`）用 \\r 原地刷新进度、长时间不输出 \\n，长程任务
            会累积出 >64KiB 的"无换行行" → ValueError 打崩读取、任务误判失败。改为定长 chunk
            读取（无行长限制），交给 LineAssembler 按终端语义装配成行。
            \\r 不再当换行：一整段进度刷新只占日志里的一行（活动行覆盖），既不挤占截断
            缓冲的尾部窗口，也大幅减少上报数据量。
            加固原则：**日志处理永不影响任务判定**——读取/解码/写盘任何异常都吞掉（仅告警），
            且持续读空管道，避免子进程因 stdout 写满而阻塞死锁。
            """
            assembler = LineAssembler()
            while True:
                try:
                    chunk = await proc.stdout.read(8192)
                except Exception as e:
                    logger.warning(f"[{task_id}] 读取子进程输出异常，停止日志转发（不影响任务）：{e}")
                    break
                if not chunk:
                    break
                try:
                    # 变量名避开模块级 functools.partial
                    for ln, is_partial in assembler.feed(chunk):
                        await self.log_writer.write(
                            task_id, str(log_path), ln + "\n", partial=is_partial
                        )
                except Exception as e:
                    logger.warning(f"[{task_id}] 日志写入异常，丢弃本段日志（不影响任务）：{e}")
                    # 只重置装配器；写入器里可能仍开着活动行槽位，下一条行会覆盖它而非追加，
                    # 代价是极端异常下少一条日志行——符合"日志处理永不影响任务判定"。
                    assembler.reset()
            # EOF：把残留的活动行定版（best-effort）
            try:
                for ln, is_partial in assembler.close():
                    await self.log_writer.write(
                        task_id, str(log_path), ln + "\n", partial=is_partial
                    )
            except Exception:
                pass

        drain_task = asyncio.create_task(_drain_stdout())
        cancel_waiter = asyncio.create_task(cancel_event.wait()) if cancel_event else None
        async def _wait_exit() -> None:
            # 不能用 proc.wait()：asyncio 的 Process.wait() 要等 stdout 管道也关闭才返回——后台子孙继承了管道时
            # 它同样永远不返回。returncode 在进程一退出就由 asyncio 置位，轮询它才是真正的“组长已退出”
            while proc.returncode is None:
                await asyncio.sleep(0.1)

        proc_waiter = asyncio.create_task(_wait_exit())

        def _kill_group() -> None:
            # start_new_session=True ⇒ 子进程是新会话组长，pgid == pid，直接用 proc.pid。
            # 不要再 getpgid(proc.pid)：组长一旦被 asyncio 回收，getpgid 就抛 ProcessLookupError，
            # 而组里遗留的子孙进程这时恰恰还活着——取消 / 超时 / 自熔都杀不掉它们
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

        try:
            # 三路竞态：cancel_waiter 完成 = 收到取消信号；proc_waiter 完成 = 子进程（组长）退出；
            # 二者皆未完成且 timeout 到 = 计算超时。均不依赖子进程是否在输出。
            # ⚠️ 以进程退出而不是 stdout EOF 作为“结束”的信号：脚本主进程退出后，只要有后台子孙继承了
            # stdout 管道，EOF 就永远不来——Lane 会被占到 timeout_seconds（默认 48 小时），心跳还在续租。
            waiters = {proc_waiter} | ({cancel_waiter} if cancel_waiter else set())
            done, _ = await asyncio.wait(
                waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )

            cancelled = cancel_waiter is not None and cancel_waiter in done
            timed_out = not cancelled and proc_waiter not in done

            if cancelled or timed_out:
                # 主动终止子进程组（Linux 信号；子进程是新会话的独立进程组的组长）
                _kill_group()
                # 让 drain 读完残余输出 + flush 末段日志（子进程已杀，EOF 将至；兜底防卡）
                if not drain_task.done():
                    try:
                        await asyncio.wait_for(drain_task, timeout=_DRAIN_GRACE_SEC)
                    except (asyncio.TimeoutError, Exception):
                        pass
                if cancelled:
                    logger.warning(
                        f"[{task_id}] 收到取消信号（status="
                        f"{heartbeat.get_server_status(task_id)}），已终止子进程"
                    )
                    return -1
                logger.error(f"子进程超时 task_id={task_id}")
                raise RuntimeError(f"计算超时（>{timeout}s）")

            # 组长已退出：给 drain 一个短宽限读完残余输出，然后无条件清理组里遗留的子孙进程
            # （正常退出也可能留下后台进程；它们不该活过这个任务）
            if not drain_task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(drain_task), timeout=_DRAIN_GRACE_SEC)
                except (asyncio.TimeoutError, Exception):
                    pass
            _kill_group()
            if not drain_task.done():
                try:
                    await asyncio.wait_for(drain_task, timeout=_DRAIN_GRACE_SEC)   # 子孙已杀，EOF 将至
                except (asyncio.TimeoutError, Exception):
                    pass
        finally:
            # ⚠️ 先**同步**发 SIGKILL（不 await），再做任何可能被打断的清理/回收。
            #
            # 教训来自 worker-cancel-task-fix：此前 finally 里先 `await gather(...)` 再
            # `proc.kill()`。若外层 task 被**二次取消**，那个 await 会再抛一次
            # CancelledError，把后面的 kill 整段跳过 —— 子进程（可能正占着 GPU 的长算子）
            # 成为孤儿继续跑，直到 worker 进程退出。把杀进程组放到一切 await 之前：
            # 信号同步送达，之后 await 被打断最多留个僵尸，不会泄漏算力。
            # 用 killpg（进程组）而非 proc.kill()（仅组长），与主分支一致，避免子孙进程逃逸。
            if proc.returncode is None:
                _kill_group()
            # 取消并 await 残留竞态任务（吞 CancelledError，避免遗留 pending task）；再回收进程
            if cancel_waiter is not None and not cancel_waiter.done():
                cancel_waiter.cancel()
            if not drain_task.done():
                drain_task.cancel()
            if not proc_waiter.done():
                proc_waiter.cancel()
            await asyncio.gather(
                *[t for t in (drain_task, cancel_waiter, proc_waiter) if t is not None],
                return_exceptions=True,
            )
            if proc.returncode is None:
                await proc.wait()

        if proc.returncode != 0:
            log_content = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
            logger.error(f"子进程退出码 {proc.returncode}，最后输出:\n{log_content[-500:]}")

        # -1 在本函数里专指“被取消”（上面的 return -1）。子进程自己被 SIGHUP 杀死时 returncode 也是 -1，
        # 若原样返回会被当成用户取消：不上报失败、ok.json 写 cancelled，任务一直 running 到租约到期。映射成 128+1
        return 129 if proc.returncode == -1 else proc.returncode

    async def _upload_step_log(self, task_id: str, log_key: str, log_filename: str) -> None:
        """上传步骤日志到存储服务并更新 log_path（JSON 格式，按步骤索引）。

        log_key / log_filename 由 execute() 经 `plugin.log_target` 算好传入。
        log_path 格式：{"<步骤key>": ".../<步骤key>.log", ...}；
        单阶段任务：{"log": ".../run.log"}
        """
        content = self.log_writer.get_truncated_content(task_id)
        if not content:
            # 静默 return 曾让「日志从未上传」与「上传失败」在排障时无法区分：
            # 前端只显示「暂无日志」，DB 的 log_path 空着，worker 日志一行没有。
            logger.warning(f"步骤日志为空，未上传: task_id={task_id}, step={log_key}")
            return

        try:
            download_url = await self.callback.upload_result_file(
                task_id, log_filename, content.encode("utf-8")
            )
            if not download_url:
                # 上传侧已按状态码记 error，这里补一句「后果」：
                # 没有 URL 就不会写 log_path，前端从此看不到这一步的日志。
                logger.warning(
                    f"步骤日志上传未返回 URL，log_path 不会写入: "
                    f"task_id={task_id}, step={log_key}"
                )
                return

            log_entry = json.dumps({log_key: download_url}, ensure_ascii=False)
            await self.callback.update_task_fields(
                task_id, log_path=log_entry, mode="merge"
            )
            logger.debug(f"步骤日志已上传: task_id={task_id}, step={log_key}")
        except Exception as e:
            logger.warning(f"步骤日志上传失败 task_id={task_id}: {e}")

    async def _collect_log(self, task_id: str) -> str:
        """收集截断日志并保存到本地（仅诊断用，不写入 DB）。

        Returns:
            保存成功的日志文件路径，失败或无内容时返回空字符串。
        """
        content = self.log_writer.get_truncated_content(task_id)
        if not content:
            await self.log_writer.close_task(task_id)
            return ""

        log_filename = "run.truncated.log"
        log_path = self.work_base_dir / task_id / log_filename

        try:
            log_path.write_text(content, encoding="utf-8")
            logger.debug(f"保存截断日志 -> {log_path}")
            return str(log_path)
        except Exception as e:
            logger.warning(f"保存截断日志失败 task_id={task_id}: {e}")
            return ""
        finally:
            await self.log_writer.close_task(task_id)

    async def _build_error_message(self, task_id: str, error: str,
                                   tail_lines: int = 50) -> str:
        """构建进程级错误消息（含日志尾部摘要）。

        将错误原因 + 进程输出尾部合并为一条可读的 error_message，
        便于前端直接展示，无需访问 worker 本地文件系统。

        Args:
            task_id:     任务 ID
            error:       原始错误信息
            tail_lines:  截取日志尾部的行数
        """
        content = self.log_writer.get_truncated_content(task_id)
        if not content:
            return error

        lines = content.rstrip().split("\n")
        tail = "\n".join(lines[-tail_lines:]) if len(lines) > tail_lines else content.rstrip()
        return f"{error}\n\n--- 进程输出尾部 ---\n{tail}"
