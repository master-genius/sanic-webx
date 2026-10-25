"""
IPC Server — AF_UNIX socket server，接收子进程通过 ipc_client.send_report 发来的
"汇报失败事件"，转交 PendingReporter 入队。

通道：
  - asyncio.start_unix_server，路径由 worker 主入口决定（默认 {WORK_DIR}/ipc.sock）
  - 路径通过环境变量 WEBX_WORKER_IPC_SOCK 传给子进程
  - 子进程 lazy connect 长连接，sendall JSON 行
  - 服务端 readline 解析 → enqueue

容错：
  - 每个子进程连接独立 _handle_client，单个连接异常不影响其他
  - JSON 解析失败仅 WARNING，丢弃该行继续读
  - 客户端断开 → readline 返回空 → 正常退出 _handle_client
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .pending_reporter import PendingReporter

logger = logging.getLogger(__name__)


class IPCServer:
    """AF_UNIX server，接 worker 内部的 PendingReporter"""

    def __init__(self, sock_path: Path, reporter: "PendingReporter"):
        self.sock_path = sock_path
        self.reporter = reporter
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        """启动 IPC server，绑定 AF_UNIX socket。"""
        self.sock_path.parent.mkdir(parents=True, exist_ok=True)
        # 清理残留 socket 文件
        try:
            self.sock_path.unlink()
        except FileNotFoundError:
            pass

        self._server = await asyncio.start_unix_server(
            self._handle_client, path=str(self.sock_path),
        )
        try:
            os.chmod(self.sock_path, 0o600)
        except OSError:
            pass
        logger.info(f"[IPC] Server 监听: {self.sock_path}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        """处理单个子进程连接，逐行解析 JSON 入队"""
        peer = writer.get_extra_info("peername") or "<unix>"
        logger.debug(f"[IPC] 接受连接: {peer}")
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break  # 客户端关闭
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(f"[IPC] 非法 JSON 行（已丢弃）: {e}, raw={line[:200]!r}")
                    continue
                if not isinstance(entry, dict) or not entry.get("task_id"):
                    logger.warning(f"[IPC] 非法 entry 结构: {entry}")
                    continue
                try:
                    self.reporter.enqueue_nowait(entry)
                except Exception as e:
                    logger.error(f"[IPC] 入队失败: {e}")
        except (asyncio.CancelledError, ConnectionResetError):
            raise
        except Exception as e:
            logger.error(f"[IPC] 客户端处理异常: {e}", exc_info=True)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def close(self) -> None:
        """关闭 server 并清理 socket 文件。

        wait_closed 加 3s timeout：避免子进程长连接未断开导致父进程退出阻塞。
        生产环境下 lane_worker await subprocess.wait()，子进程通常已先退出，连接已断。
        """
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning("[IPC] wait_closed 超时（3s），强制关闭")
            except Exception as e:
                logger.warning(f"[IPC] wait_closed 异常: {e}")
            self._server = None
        try:
            self.sock_path.unlink()
        except FileNotFoundError:
            pass
        logger.info("[IPC] Server 已关闭")
