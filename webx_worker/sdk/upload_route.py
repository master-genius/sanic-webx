"""产物交付的**唯一分流咽喉** —— 决定一个产物是同步上传还是交给 uploader。

三个入口，名字就是契约，调用点不需要再想判据：

    deliver_required(...)   下游步骤依赖 → **恒同步，不看大小**
    deliver_only(...)       纯交付给用户 → 按大小：小的同步、大的交 uploader
    deliver(...)            由**可配置规则**决定走哪条（见下）

【为什么必须分两个函数而不是一个 size 判断】
纯按大小分流会把「下游步骤要吃的」产物推给 uploader。它们一旦还没传完，跨机接力
就拿不到上一步的产物——而且是偶发的（取决于产物大小），最难排查的那种。分流判据必须是
**合取**：`下游不消费 AND ≥阈值`。

把它拆成两个具名函数，是为了让"选错"在代码审查时一眼可见：调用点写的是
``deliver_required(x)`` 还是 ``deliver_only(y)``，语义写在名字上，
而不是藏在一个布尔参数或一个 size 比较里。

【可配置规则】`deliver()` 为不想逐处写死 required/only 的任务脚本准备：
环境变量 ``WEBX_WORKER_UPLOAD_REQUIRED_GLOBS``（逗号分隔的 fnmatch 模式，按【远端文件名】匹配，
由 worker 配置项 UPLOAD_REQUIRED_GLOBS 注入）命中的产物视为 required，其余视为 only。
框架本身**不内置任何扩展名判据**——哪些文件是「下游依赖」只有业务知道。

【返回值】两种，调用方一律直接落进 result 字段：
    真 URL   同步上传成功
    ``""``   进了队列（uploader 传完会 merge 真 URL 覆盖它），或彻底没戏

**进队列与彻底失败对调用方是同一个返回值**，这是刻意的：不写占位值就没有"占位值
要先推上云才生效"的次序问题（详见 upload_queue 的说明），前端也不必认识任何新前缀。
代价是前端在传完前分不清"正在传"和"没有"——都显示暂无，传完刷新即有。
"""

from __future__ import annotations

import fnmatch
import logging
import os
from pathlib import Path
from typing import List

from webx_worker import envnames as E
from webx_worker.sdk import upload_queue as q
from webx_worker.sdk.result_client import upload_result

logger = logging.getLogger(__name__)

_ENV_ENABLED = E.ENV_UPLOADER_ENABLED
_ENV_THRESHOLD_MB = E.ENV_UPLOAD_ASYNC_THRESHOLD_MB
_ENV_WORK_DIR = E.ENV_WORK_DIR
_ENV_STORAGE_INTERNAL = E.ENV_STORAGE_INTERNAL_URL
_ENV_STORAGE = E.ENV_STORAGE_URL
_ENV_REQUIRED_GLOBS = E.ENV_UPLOAD_REQUIRED_GLOBS

_DEFAULT_THRESHOLD_MB = 15

# 「未配 storage」只告警一次：分流判据每个产物都要过一遍，逐次刷屏没有信息量。
_warned_no_storage = False


def _storage_configured() -> bool:
    """是否配置了存储服务地址 —— uploader 能不能工作的前提。

    ⚠️ 为什么这是硬前提而不是优化项：协议规定产物只上传到存储服务，任务服务上没有对应
    端点（docs/worker-protocol.md §5）。没配存储地址时投进队列的产物永远传不上去，
    只会按队列「永不放弃」的语义无限重试。此时正确的行为是**根本不投**，退回同步上传
    （同步路径会明确报错，而不是悄悄堆积）。

    父进程侧（config.storage_configured）读 worker 配置做同一判断 —— 两处口径必须一致。
    """
    return bool(os.environ.get(_ENV_STORAGE_INTERNAL, "").strip()
                or os.environ.get(_ENV_STORAGE, "").strip())


def _async_enabled() -> bool:
    """uploader 总开关。**未注入即视为关闭** —— 不由 worker 拉起的子进程、
    直接跑脚本的调试场景，都应退回"一律同步上传"，绝不静默投进一个没人消费的队列。

    未配存储地址时同样视为关闭，理由见 `_storage_configured`。"""
    global _warned_no_storage
    v = os.environ.get(_ENV_ENABLED, "").strip().lower()
    if v not in ("1", "true", "yes", "on"):
        return False
    if not os.environ.get(_ENV_WORK_DIR, "").strip():
        return False
    if not _storage_configured():
        if not _warned_no_storage:
            _warned_no_storage = True
            logger.warning(
                f"uploader 已开启但未配置存储服务（{_ENV_STORAGE_INTERNAL} / {_ENV_STORAGE}），"
                "大产物改走同步上传——投进队列只会无限重试"
            )
        return False
    return True


def _threshold_bytes() -> int:
    try:
        mb = int(os.environ.get(_ENV_THRESHOLD_MB, "") or _DEFAULT_THRESHOLD_MB)
    except ValueError:
        mb = _DEFAULT_THRESHOLD_MB
    return max(0, mb) * 1024 * 1024


def required_globs() -> list[str]:
    """解析可配置的「恒同步」规则（逗号分隔的 fnmatch 模式）。"""
    raw = os.environ.get(_ENV_REQUIRED_GLOBS, "")
    return [g.strip() for g in raw.split(",") if g.strip()]


def is_required(remote_name: str) -> bool:
    """按配置的规则判断一个产物是否「下游依赖」（命中任一模式即是）。"""
    return any(fnmatch.fnmatchcase(remote_name, g) for g in required_globs())


def _enqueue(task_id: str, src: Path, remote_name: str, key_path: List[str]) -> bool:
    """投队列。成功 True / 失败 False。"""
    work_dir = os.environ.get(_ENV_WORK_DIR, "").strip()
    if not work_dir:
        return False
    return q.enqueue(work_dir, task_id, src, remote_name, key_path)


def _sync_upload(task_id: str, src: Path, remote_name: str) -> str:
    """同步上传，成功返回 download_url，失败返回空串（异常不外泄）。"""
    try:
        resp = upload_result(task_id, str(src), filename=remote_name)
        return resp.get("download_url") or ""
    except Exception as e:  # noqa: BLE001 —— 上传失败绝不能掀翻步骤
        logger.error(f"上传失败 {remote_name}: {e}")
        return ""


def _deliver(
    task_id: str,
    local_path: str | Path,
    remote_name: str,
    key_path: List[str],
    *,
    required: bool,
) -> str:
    src = Path(local_path)
    if not src.is_file():
        return ""

    try:
        size = src.stat().st_size
    except OSError:
        return ""

    go_async = (not required) and _async_enabled() and size >= _threshold_bytes()

    if not go_async:
        url = _sync_upload(task_id, src, remote_name)
        if url:
            return url
        # 同步失败降级进队列：文件太大、重试超时这类问题交给 uploader 慢慢磨，
        # 比"写空串、产物永久丢失"严格改善。required 产物同样降级——
        # 落空串对下游本来就是"暂时没有"，进队列只是多了一条自愈的路。
        if _async_enabled() and _enqueue(task_id, src, remote_name, key_path):
            logger.warning(f"同步上传失败，已转交 uploader 补传: {remote_name}")
        return ""

    if _enqueue(task_id, src, remote_name, key_path):
        return ""       # 字段留空，uploader 传完 merge 真 URL 进来
    # 投队列失败 → 退回同步传，别让产物无声消失
    logger.warning(f"投递上传队列失败，退回同步上传: {remote_name}")
    return _sync_upload(task_id, src, remote_name)


def deliver_required(
    task_id: str, local_path: str | Path, remote_name: str, key_path: List[str]
) -> str:
    """**下游步骤依赖的产物** —— 恒同步上传，不看大小。

    它们是链式接力与跨机恢复的命脉，异步化会让下一步拿到还没传完的 URL。
    多大都等，等的是正确性。
    """
    return _deliver(task_id, local_path, remote_name, key_path, required=True)


def deliver_only(
    task_id: str, local_path: str | Path, remote_name: str, key_path: List[str]
) -> str:
    """**纯交付给用户的产物** —— 按大小分流：小的同步传掉，大的交 uploader。

    用于 worker 侧零消费点的产物。小文件也走同步是为了省一趟队列往返——
    几 MB 同步传只要几秒，进队列反而多一次扫描延迟和一次权威检查往返。
    """
    return _deliver(task_id, local_path, remote_name, key_path, required=False)


def deliver(
    task_id: str, local_path: str | Path, remote_name: str, key_path: List[str]
) -> str:
    """由**可配置规则**决定走 required 还是 only（见模块 docstring 的「可配置规则」）。

    命中 ``WEBX_WORKER_UPLOAD_REQUIRED_GLOBS`` 的远端文件名恒同步；其余按大小分流。
    """
    return _deliver(
        task_id, local_path, remote_name, key_path, required=is_required(remote_name)
    )
