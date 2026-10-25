"""独立上传进程 —— 把大产物的上传从计算的关键路径上摘出去。

    python -m webx_worker uploader   # 由 worker 父进程 spawn，见 uploader_supervisor

【解决的问题】上传发生在任务子进程里，Lane 要等它传完才释放。一个数 GB 的产物传几十
分钟，这期间 GPU/CPU 全程闲着。本进程接手这类上传后，子进程算完即退出，Lane 立刻释放。

【零回边】本进程**不通知任何人**：任务子进程早退出了，worker 父进程不消费产物 URL，
下游步骤只吃同步上传的那几个文件。唯一出边是存储服务（传）与任务服务（交付地址）。
没有回边就没有时序耦合，也就不存在"通知丢了怎么办"。

【怎么知道该传什么】不需要谁通知——扫 ``{WORK_DIR}/.uploads/pending/`` 目录。
写文件成功就是送达，见 sdk/upload_queue 的说明。

【怎么知道不用传了】任务被取消/删除/重置/redo，都**不通知本进程**：它不在那条链上，
而且 worker 随时可能重启。服务端是唯一权威，故每次上传前问一次
`GET /internal/tasks/<id>`，判据只有一条：

    任务存在（非 404）且状态不是 failed / cancelled → 就是需要传的

（协议保证该接口不返回已软删任务，所以软删同样表现为 404。）
既然让本进程传，那么只要任务还在、没失败没取消，这份委托就算数。大文件每传若干分片
复查一次——几 GB 传一半才发现白传，省下的带宽很可观。

【交付】传完后 `POST /internal/tasks/<id>/deliveries`（协议 §4.9）：**刻意不做 fencing**——
本进程是长驻独立进程，手里永远没有 per-task 的租约令牌。判据只有两条：404 → 丢弃条目，
其余 → 幂等 merge。
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from pathlib import Path
from typing import Any, Optional

import httpx

from . import envnames as E
from .config import storage_configured, worker_settings
from .link_health import observed_async_client
from .log_config import setup_logging
from .sdk import upload_queue as q

logger = logging.getLogger(__name__)

# 分片大小。storage 的单片上限默认 64MB（UPLOAD_CHUNK_MAX_BYTES），
# 取 8MB 是在"往返次数"与"单次读盘阻塞"之间取中：读盘走 to_thread，8MB 一次约几十毫秒。
CHUNK_SIZE = 8 * 1024 * 1024

# 每传多少分片复查一次云端（8 × 8MB = 64MB）。
# 太密则每次都多一个 HTTP 往返；太疏则取消后仍会白传很久。
RECHECK_EVERY_CHUNKS = 8

# 单次 HTTP 的超时。与 result_client 同一套理由：四项分开，connect 不继承长超时；
# 这几项全是**空闲超时**（per-operation），不限制整体传输时长。
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, write=60.0, read=300.0, pool=10.0)


# ==================== 配置解析 ====================


def _storage_base() -> str:
    """worker 实际请求存储服务的地址：内网 → 对外，与 sdk.result_client 同序。

    没有回退到任务服务的路径（协议 §5：产物不经过任务服务）。"""
    return (
        worker_settings.STORAGE_INTERNAL_URL.strip()
        or worker_settings.STORAGE_URL.strip()
    ).rstrip("/")


def _storage_token() -> str:
    """存储服务专属 Token，未配则回退主 Token（与 sdk.result_client._storage_token 同语义）。"""
    return worker_settings.STORAGE_INTERNAL_TOKEN.strip() or worker_settings.INTERNAL_TOKEN


def _public_prefix() -> str:
    """落库 URL 用的【对外】地址。空则保持相对路径（同机部署由前端按 origin 解析）。"""
    return worker_settings.STORAGE_URL.strip().rstrip("/")


def _normalize_download_url(rel: str) -> str:
    """存储端回传相对路径 → 落库用的绝对/相对 URL。与 sdk.result_client.upload_result 同口径。"""
    if not rel or rel.startswith(("http://", "https://")):
        return rel
    pub = _public_prefix()
    return f"{pub}/{rel.lstrip('/')}" if pub else rel


# ==================== 云端权威判据 ====================
#
# 判据只看「任务还在不在、状态对不对」——队列条目本身就是委托，不做字段级比对。

# 任务已进这些状态 → 产物不再需要，删掉队列里那条 json 记录。
# 取消是显式的用户意图；失败则是这一代计算作废，用户要么重跑（重跑会重新投递）
# 要么不要了——两种情况下继续传都是白烧带宽，还会在 storage 留下无人引用的文件。
_DEAD_TASK_STATUSES = frozenset({"cancelled", "failed"})

# 业务拒绝 → 丢弃。重试也不会变好：404=任务没了，400=请求本身不合法（如畸形 task_id）。
_BUSINESS_REJECT_CODES = frozenset({400, 404})

# ⚠️ 鉴权/权限 → **保留**，不丢弃。
# 401/403 说明 Token 或路由配错了，是**部署问题不是数据问题**：修好即自愈。
# 若按"4xx 一律丢弃"处理，一次 Token 填错就会在几轮扫描内清空整个待传队列、
# 产物全部永久丢失，而日志上只有几行"已丢弃"。PendingReporter 的
# _AUTH_RETAIN_CODES 出于同一理由做同样的取舍，两处保持一致。
_AUTH_RETAIN_CODES = frozenset({401, 403})


async def _still_wanted(client: httpx.AsyncClient, entry: q.Entry) -> bool:
    """问任务服务：这条上传现在还需要吗？**上传前必问，一次都不能省。**

    本进程收不到任何通知（worker 还随时可能重启），服务端是唯一权威。

    判据就两条——**队列条目本身就是委托**，既然让我传，那么只要任务还在、
    状态不是失败/取消，就是需要传的：
      · 任务不存在（404）→ 不传
      · 状态 failed / cancelled → 不传
      · 其余（含 success：多阶段任务终态后产物照样要交付）→ 传

    不比对 result_path 里的字段值：那需要投递时先落一个占位值、并由任务脚本推上云，
    而推上云发生在本步全部产物传完之后——中间几十秒里本进程会问到"还没有我的占位值"
    从而把没发布的条目当过期的删掉。不比对就没有这个次序问题。

    问不到时（网络故障 / 5xx / 401 / 403）返回 True 保留条目：上传是幂等覆盖，
    多传一次只费带宽；误丢是产物永久缺失。鉴权错更要保留——那是部署问题，
    修好即自愈，按"4xx 一律丢弃"会让一次 Token 填错清空整个队列。
    """
    url = f"{worker_settings.WEB_BASE_URL.rstrip('/')}/internal/tasks/{entry.task_id}"
    try:
        resp = await client.get(
            url, headers={E.HEADER_INTERNAL_TOKEN: worker_settings.INTERNAL_TOKEN}
        )
    except Exception as e:  # noqa: BLE001 —— 问不到就当作"还需要"，见 docstring
        logger.warning(f"[uploader] 权威检查失败（按仍需要处理）: {entry.remote_name}, {e}")
        return True

    code = resp.status_code
    if code in _BUSINESS_REJECT_CODES:
        logger.info(
            f"[uploader] 云端拒绝({code})，删记录: task={entry.task_id} {entry.remote_name}"
        )
        return False
    if code in _AUTH_RETAIN_CODES:
        # 只记 warning 不丢弃：配置问题，人工修好后这一队列会自己恢复
        logger.warning(
            f"[uploader] 权威检查鉴权失败({code})，保留待人工处理: {entry.remote_name}"
        )
        return True
    if code != 200:
        logger.warning(
            f"[uploader] 权威检查返回 {code}（按仍需要处理）: {entry.remote_name}"
        )
        return True

    try:
        body = resp.json() or {}
        # 协议 §4.10 没写清响应是否包在 data 里 —— 两种都兼容
        data = body.get("data") if isinstance(body.get("data"), dict) else body
    except (ValueError, TypeError, AttributeError) as e:
        logger.warning(f"[uploader] 任务信息解析失败（按仍需要处理）: {e}")
        return True

    status = str(data.get("status") or "").strip().lower()
    if status in _DEAD_TASK_STATUSES:
        logger.info(
            f"[uploader] 任务已 {status}，删记录: task={entry.task_id} {entry.remote_name}"
        )
        return False
    return True


# ==================== 重试退避 ====================
# 队列语义是「永不放弃」（丢弃=产物永久缺失），但**永不放弃不等于每 3 秒重试一次**。
# 恒定失败的条目（storage 侧 400、本地文件损坏、名字非法…）在无退避时会：
#   · 整传路径每轮重发整个文件 —— 几个 GB 的产物能把上行带宽长期占满；
#   · 每轮一条 warning —— 真正的异常被淹没。
# 故按 attempts 指数退避（队列已经在 release() 里记了 attempts 与本次时间 queued_at，
# 此前从没有人读它）。封顶 5 分钟：再久就失去「配置修好后自愈」的及时性。
_RETRY_BACKOFF_BASE_SEC = 5.0
_RETRY_BACKOFF_MAX_SEC = 300.0
# 连续失败到这个次数就升一级日志：不再是「偶发抖动」，需要人看一眼
_RETRY_ALERT_EVERY = 10


def _retry_delay(attempts: int) -> float:
    """第 attempts 次失败后应等待的秒数（5,10,20,…,封顶 300）。"""
    if attempts <= 0:
        return 0.0
    return min(_RETRY_BACKOFF_BASE_SEC * (2 ** (attempts - 1)), _RETRY_BACKOFF_MAX_SEC)


def _is_due(entry: q.Entry, now: float) -> bool:
    """该条目本轮是否该重试。首次（attempts=0）恒 True。

    `queued_at` 由 release() 刷成"上次失败的时刻"，所以判据直接可用，
    不需要在队列里另存一个 next_try 字段（少一个要跨版本兼容的字段）。
    """
    if entry.attempts <= 0:
        return True
    return (now - entry.queued_at) >= _retry_delay(entry.attempts)


# ==================== 上传 ====================


async def _upload_single(client: httpx.AsyncClient, entry: q.Entry) -> Optional[str]:
    """整传（小文件 / 同步失败降级来的）。返回相对 download_url，失败 None。

    `POST /internal/tasks/<task_id>/results`（multipart，创建成功 201）。"""
    url = f"{_storage_base()}/internal/tasks/{entry.task_id}/results"
    try:
        content = await asyncio.to_thread(Path(entry.local_path).read_bytes)
        resp = await client.post(
            url,
            headers={E.HEADER_INTERNAL_TOKEN: _storage_token()},
            files={"file": (entry.remote_name, content)},
            data={"filename": entry.remote_name},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[uploader] 整传失败: {entry.remote_name}, {e}")
        return None
    if resp.status_code not in (200, 201):
        logger.warning(f"[uploader] 整传被拒 {resp.status_code}: {entry.remote_name}")
        return None
    return (resp.json() or {}).get("download_url") or ""


async def _session_offset(client: httpx.AsyncClient, session_id: str) -> Optional[int]:
    """查续传会话已收字节；会话不存在（过期/被清）返回 None，调用方重新创建会话。

    重同步依据是响应头 ``Upload-Offset``（docs/storage-service.md）；头缺失时才退而看 JSON。"""
    url = f"{_storage_base()}/internal/uploads/{session_id}"
    try:
        resp = await client.get(url, headers={E.HEADER_INTERNAL_TOKEN: _storage_token()})
    except Exception:  # noqa: BLE001
        return None
    if resp.status_code != 200:
        return None
    try:
        hdr = resp.headers.get("Upload-Offset")
        if hdr is not None:
            return int(hdr)
        return int((resp.json() or {}).get("offset", 0))
    except (ValueError, TypeError):
        return None


async def _upload_resumable(
    client: httpx.AsyncClient, entry: q.Entry, work_dir: str
) -> Optional[str]:
    """分片续传（大文件）。返回相对 download_url，失败 None。

    三件事是整个异步化能成立的前提：
      · **协程不被钉死** —— 每片走 to_thread 读盘，片与片之间天然让出事件循环。
        直接把同步文件对象交给 httpx.AsyncClient 会在读盘时阻塞整个 loop，
        并发就成了摆设，比串行还糟（看起来在并发）。
      · **断点续传** —— 崩溃/重启后凭 session_id 取 offset 接着传，不从头再来几 GB。
      · **中途可放弃** —— 每 RECHECK_EVERY_CHUNKS 片复查一次云端，取消即 abort 会话。
    """
    token = _storage_token()
    headers = {E.HEADER_INTERNAL_TOKEN: token}
    src = Path(entry.local_path)
    total = entry.size or src.stat().st_size

    # 复用已有会话（上次崩在半路）；取不到 offset 说明会话已过期 → 重新创建会话
    offset = await _session_offset(client, entry.session_id) if entry.session_id else None
    if offset is None:
        try:
            resp = await client.post(
                f"{_storage_base()}/internal/tasks/{entry.task_id}/results/uploads",
                headers=headers,
                json={"filename": entry.remote_name, "total": total},
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[uploader] 创建续传会话失败: {entry.remote_name}, {e}")
            return None
        if resp.status_code not in (200, 201):
            logger.warning(f"[uploader] 创建续传会话被拒 {resp.status_code}: {entry.remote_name}")
            return None
        meta = resp.json() or {}
        entry.session_id = str(meta.get("upload_id") or "")
        offset = int(meta.get("offset") or 0)
        if not entry.session_id:
            return None
        q.update(work_dir, entry)   # 会话 ID 立刻落盘，崩溃后才接得上
    else:
        logger.info(
            f"[uploader] 续传接力: {entry.remote_name} 从 {offset}/{total} 继续"
        )

    chunk_no = 0
    with open(src, "rb") as fh:
        await asyncio.to_thread(fh.seek, offset)
        while offset < total:
            data = await asyncio.to_thread(fh.read, CHUNK_SIZE)
            if not data:
                break
            try:
                resp = await client.patch(
                    f"{_storage_base()}/internal/uploads/{entry.session_id}",
                    headers={**headers,
                             "Upload-Offset": str(offset),
                             "Content-Type": "application/octet-stream"},
                    content=data,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[uploader] 分片发送失败 @{offset}: {entry.remote_name}, {e}")
                return None

            if resp.status_code == 409:
                # 服务端 offset 与我们的不一致（重复发送/上次半截）→ 按服务端响应头
                # Upload-Offset 给出的真实值重同步
                server_off = int(resp.headers.get("Upload-Offset") or 0)
                logger.info(f"[uploader] offset 重同步 {offset} → {server_off}: {entry.remote_name}")
                offset = server_off
                await asyncio.to_thread(fh.seek, offset)
                continue
            if resp.status_code not in (200, 204):
                logger.warning(
                    f"[uploader] 分片被拒 {resp.status_code} @{offset}: {entry.remote_name}"
                )
                return None

            offset += len(data)
            chunk_no += 1

            # 中途复查：几 GB 传到一半才发现任务已取消，白烧的带宽很可观
            if chunk_no % RECHECK_EVERY_CHUNKS == 0 and not await _still_wanted(client, entry):
                await _abort_session(client, entry.session_id)
                entry.session_id = ""
                return None

    try:
        resp = await client.post(
            f"{_storage_base()}/internal/uploads/{entry.session_id}/complete",
            headers=headers,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[uploader] 续传 complete 失败: {entry.remote_name}, {e}")
        return None
    if resp.status_code not in (200, 201):
        logger.warning(f"[uploader] 续传 complete 被拒 {resp.status_code}: {entry.remote_name}")
        return None
    entry.session_id = ""
    return (resp.json() or {}).get("download_url") or ""


async def _abort_session(client: httpx.AsyncClient, session_id: str) -> None:
    """放弃续传会话（幂等）。失败不重要——storage 侧有超龄会话清理兜底。"""
    if not session_id:
        return
    try:
        await client.delete(
            f"{_storage_base()}/internal/uploads/{session_id}",
            headers={E.HEADER_INTERNAL_TOKEN: _storage_token()},
        )
    except Exception:  # noqa: BLE001
        pass


# ==================== 回写 ====================


def _nest(key_path: list, value: str) -> dict:
    """["step2","out_url"] + url → {"step2": {"out_url": url}}。

    协议 §4.9：服务端按嵌套 key 幂等 merge，任意深度都只覆盖到叶子，不会碰同级的其他字段。
    """
    out: Any = value
    for k in reversed(key_path):
        out = {k: out}
    return out


async def _writeback(client: httpx.AsyncClient, entry: q.Entry) -> str:
    """把真 URL 交付给任务（merge 进 result_path）。

    返回三态（**不是 bool**：丢弃与交付都让条目出队，但含义相反，合成一个值会让日志撒谎）：
      · ``"done"``  —— 写进去了
      · ``"drop"``  —— 无处可写（任务已不存在），出队但**没有交付**
      · ``"retry"`` —— 暂时性失败，退回队列下轮再来
    """
    # 走**产物交付**专用端点（POST /deliveries），而不是 PATCH /fields。
    #
    # 两者的授权口径不同，这正是分开的理由：
    #   · PATCH /fields 是「推进任务状态」的一部分，必须严格 fencing（当前租户才能写）；
    #   · 本进程做的是「把一个已上传产物的地址，幂等 merge 进它自己那一轮的带序号 key」，
    #     它跟"我现在还是不是租户"无关 —— 而且本进程是长驻独立进程，
    #     手里永远没有 per-task 的租约令牌，严格比对的结果只能是全军覆没。
    url = (f"{worker_settings.WEB_BASE_URL.rstrip('/')}"
           f"/internal/tasks/{entry.task_id}/deliveries")
    # result_path 取对象（协议 §4.9 的示例写法 `{"result_path": {...}}`）。
    # 注意 PATCH /fields 的值类型协议没写清（见 docs/worker.md 协议缺口）。
    payload: dict[str, Any] = {"result_path": _nest(
        entry.key_path, _normalize_download_url(entry.uploaded_url),
    )}
    try:
        resp = await client.post(
            url, headers={E.HEADER_INTERNAL_TOKEN: worker_settings.INTERNAL_TOKEN},
            json=payload,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[uploader] 交付失败: {entry.remote_name}, {e}")
        return "retry"
    if resp.status_code == 200:
        return "done"
    if resp.status_code == 404:
        # ⚠️ 404 有两种截然不同的成因，**丢弃前必须分清**：
        #
        #   ① 任务真的没了（在「传完」与「交付」之间被删）→ URL 无处可写，该丢；
        #   ② **反代/服务端没有 /deliveries 这条路由**（升级顺序、转发缺失）→ 丢了就等于
        #      产物地址永久缺失，而它本来只要等路由就位就能送达。
        #
        # 判据是响应体形状：任务不存在时服务端回 `{"error": ...}`（api-conventions 第 8 条）；
        # 路由不存在时通常是框架的通用 404 页，没有 error 键。
        # 认不出来一律保留 —— 误留只是多试几轮，误丢是产物永久缺失。
        if _is_task_not_found(resp):
            logger.info(f"[uploader] 回写时任务已不存在，丢弃: {entry.task_id}")
            return "drop"
        logger.warning(
            f"[uploader] 交付 404 但不是「任务不存在」，保留待重试: {entry.remote_name} "
            f"—— 多半是任务服务/反代缺 /deliveries 路由，就位后自愈"
        )
        return "retry"
    logger.warning(f"[uploader] 交付被拒 {resp.status_code}: {entry.remote_name}")
    return "retry"


def _is_task_not_found(resp: httpx.Response) -> bool:
    """404 是否为「任务不存在」（而非「路由不存在」）。认不出来一律 False（保守保留）。"""
    try:
        body = resp.json()
    except Exception:  # noqa: BLE001 —— 非 JSON 一定不是业务 404
        return False
    return isinstance(body, dict) and "error" in body


# ==================== 单条处理 ====================


async def _process(client: httpx.AsyncClient, entry: q.Entry, work_dir: str) -> None:
    """处理一条：权威检查 → 上传 → 回写。任一环失败即退回队列，下轮再来。"""
    claimed = q.claim(work_dir, entry)
    if claimed is None:
        return          # 已被取走（并发保护，rename 天然互斥）
    entry = claimed

    try:
        if not Path(entry.local_path).is_file():
            # 产物已被清理。设计上清理侧会跳过队列引用的文件，
            # 这里是竞态兜底——补传一个不存在的文件没有意义。
            logger.warning(f"[uploader] 本地文件已不在，丢弃: {entry.local_path}")
            q.drop(work_dir, entry)
            return

        if not await _still_wanted(client, entry):
            await _abort_session(client, entry.session_id)
            q.drop(work_dir, entry)
            return

        # 两阶段：已经传成功过就只做回写，绝不重传几个 GB
        if not entry.uploaded_url:
            if entry.size > CHUNK_SIZE:
                rel = await _upload_resumable(client, entry, work_dir)
            else:
                rel = await _upload_single(client, entry)
            if not rel:
                q.release(work_dir, entry)
                return
            entry.uploaded_url = rel
            q.update(work_dir, entry)   # 先落盘再回写：崩在这里也不会重传
            logger.info(
                f"[uploader] 上传完成: task={entry.task_id} {entry.remote_name} "
                f"({entry.size} 字节)"
            )

        outcome = await _writeback(client, entry)
        if outcome == "retry":
            q.release(work_dir, entry)
        else:
            q.drop(work_dir, entry)
            if outcome == "done":
                logger.info(f"[uploader] 已交付: {entry.remote_name} → {entry.key_path}")
    except Exception as e:  # noqa: BLE001 —— 单条出错绝不能掀翻整个 uploader
        logger.error(f"[uploader] 处理条目异常: {entry.remote_name}, {e}", exc_info=True)
        q.release(work_dir, entry)


# ==================== 主循环 ====================


async def run(work_dir: str) -> None:
    """扫描—分发主循环。永不主动退出，靠 SIGTERM 停。"""
    q.ensure_dirs(work_dir)

    # 自检：正常路径由 supervisor 拦住（见 uploader_supervisor.start），这里是手工
    # `python -m webx_worker uploader` 单独拉起时的兜底。没有存储服务地址时每一条上传都
    # 无处可去，而失败即退回队列、每轮重试 —— 那是纯粹的空转与刷屏。
    if not storage_configured():
        logger.error(
            f"[uploader] 未配置存储服务（{E.ENV_STORAGE_INTERNAL_URL} / {E.ENV_STORAGE_URL}），"
            "拒绝启动 —— 产物只能上传到存储服务，任务服务上没有对应端点。配好后再启动。"
        )
        return

    q.requeue_inflight(work_dir)

    interval = max(0.5, worker_settings.UPLOADER_SCAN_INTERVAL)
    concurrency = max(1, worker_settings.UPLOADER_CONCURRENCY)
    sem = asyncio.Semaphore(concurrency)
    stopping = asyncio.Event()
    running: dict[str, asyncio.Task] = {}

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stopping.set)
        except NotImplementedError:      # 非 Unix，退化为无优雅停止
            pass

    logger.info(
        f"[uploader] 启动: dir={q.queue_root(work_dir)} 并发={concurrency} "
        f"扫描间隔={interval}s storage={_storage_base()}"
    )

    # 用观测客户端而非裸 AsyncClient：本进程要打任务服务（权威检查 / 交付）与存储服务（上传）
    # 两条内部链路，任一条持续 401/路由错都必须发得出声。link_health 的告警文本带完整
    # URL，运维能分辨是哪一端——标签不够精确，也远好过让一条链路哑掉
    # （tests/worker/test_link_health.py 有全包守卫，正是防这个）。
    async with observed_async_client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        while not stopping.is_set():
            now = time.time()
            for entry in q.iter_pending(work_dir):
                if stopping.is_set():
                    break
                # 在途守卫：3 秒一轮扫描，别给同一条目重复起协程
                if entry.filename in running:
                    continue
                # 退避守卫：失败过的条目要等够 _retry_delay 才再试（见常量段说明）
                if not _is_due(entry, now):
                    continue
                if entry.attempts and entry.attempts % _RETRY_ALERT_EVERY == 0:
                    logger.error(
                        "[uploader] 条目已连续失败 %d 次，仍在重试（间隔 %.0fs）: "
                        "task=%s %s", entry.attempts, _retry_delay(entry.attempts),
                        entry.task_id, entry.remote_name,
                    )

                async def _guarded(e=entry):
                    async with sem:
                        await _process(client, e, work_dir)

                t = asyncio.create_task(_guarded(), name=f"upload:{entry.filename}")
                running[entry.filename] = t
                t.add_done_callback(lambda _t, k=entry.filename: running.pop(k, None))

            try:
                await asyncio.wait_for(stopping.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

        # 优雅停止：给在途的一个宽限期把当前分片写完并落 offset，超时就走。
        # **不等它们传完**——传不完下次启动接着传，这本来就是队列的语义。
        if running:
            logger.info(f"[uploader] 收到停止信号，等待 {len(running)} 条在途落盘…")
            await asyncio.wait(list(running.values()), timeout=10)
    logger.info("[uploader] 已退出")


def _guard_orphan() -> None:
    """父进程消失即自杀，避免 worker 被 SIGKILL 后留下孤儿上传进程。"""
    try:
        import ctypes
        # PR_SET_PDEATHSIG = 1：父进程死时内核给本进程发 SIGTERM
        ctypes.CDLL("libc.so.6").prctl(1, signal.SIGTERM, 0, 0, 0)
    except Exception:  # noqa: BLE001 —— 非 Linux / 无 libc，退化为下面的轮询自检
        pass


async def _watch_parent(stop_after_orphan: float = 5.0) -> None:
    """PDEATHSIG 的兜底：父进程被收养到 init 下即退出。"""
    while True:
        await asyncio.sleep(stop_after_orphan)
        if os.getppid() == 1:
            logger.warning("[uploader] 父进程已消失，退出")
            os.kill(os.getpid(), signal.SIGTERM)
            return


def main(argv=None) -> None:
    _guard_orphan()
    setup_logging(
        log_dir=worker_settings.LOG_DIR or None,
        debug=worker_settings.DEBUG,
        max_bytes=worker_settings.LOG_MAX_BYTES,
        max_lines=worker_settings.LOG_MAX_LINES,
    )
    work_dir = worker_settings.WORK_DIR

    async def _main():
        watcher = asyncio.create_task(_watch_parent(), name="uploader-parent-watch")
        try:
            await run(work_dir)
        finally:
            watcher.cancel()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
