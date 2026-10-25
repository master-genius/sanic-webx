"""
结果客户端 — 供任务脚本在子进程中调用任务服务 / 存储服务的内部 API。

连接信息从环境变量读取（由 worker 的 executor 在拉起子进程时注入，见 envnames.py）：
  - WEBX_WORKER_INTERNAL_TOKEN / WEBX_WORKER_WEB_BASE_URL     任务服务
  - WEBX_WORKER_STORAGE_URL / _STORAGE_INTERNAL_URL / _STORAGE_INTERNAL_TOKEN   存储服务
  - WEBX_WORKER_ID / WEBX_WORKER_LEASE_SEQ                    fencing 令牌

核心功能：
  - upload_result: 上传结果文件到存储服务，返回下载 URL
  - update_task_fields: 更新任务的 result_path / log_path（PATCH /fields，支持 merge）
  - update_step_status: 更新步骤进度（PATCH /steps）
  - download_artifact / download_artifact_stream / list_artifacts: 从存储服务取回产物

设计原则：
  - 纯同步 HTTP 客户端（子进程脚本可能是非 async 的）
  - 轻量级，零框架依赖（仅标准库 + httpx 同步接口）
"""

import logging
import os
import time
from pathlib import Path
from typing import Optional

import httpx

from webx_worker import envnames as E
from webx_worker.sdk.ipc_client import send_report

logger = logging.getLogger(__name__)

_ENV_TOKEN = E.ENV_INTERNAL_TOKEN
# fencing 令牌：由 executor 注入本子进程的环境（父进程的内存在这里拿不到）。
_ENV_WORKER_ID = E.ENV_WORKER_ID
_ENV_LEASE_SEQ = E.ENV_LEASE_SEQ


def _fencing_headers() -> dict:
    """本子进程所属任务的 fencing header。

    ⚠️ 缺了它，子进程对任务服务的**全部**写入都会被服务端 fencing 拒掉（403）——
    而步骤进度 / 字段更新这些主力上报都在子进程里。
    缺省为空 = 本任务无租约（不在 worker 管理下手动跑脚本调试时就是这样），
    此时不带 header，服务端会明确拒绝写入，而不是悄悄放行。
    """
    wid = os.environ.get(_ENV_WORKER_ID, "").strip()
    seq = os.environ.get(_ENV_LEASE_SEQ, "").strip()
    if not wid or not seq:
        return {}
    return {E.HEADER_WORKER_ID: wid, E.HEADER_LEASE_SEQ: seq}


# storage 专属鉴权 Token（可选）。未配置则回退主 Token ——
# 与父进程 callback.storage_token / uploader 同一条回退链
# （STORAGE_INTERNAL_TOKEN → INTERNAL_TOKEN），两侧因此一致。
_ENV_STORAGE_TOKEN = E.ENV_STORAGE_INTERNAL_TOKEN
_ENV_BASE_URL = E.ENV_WEB_BASE_URL
# 存储服务地址双轨（严格分离）：
#   _ENV_STORAGE_URL          对外/公网 URL —— 恒用于「落库 download_url」（面向浏览器展示）
#   _ENV_STORAGE_INTERNAL_URL 内网 URL     —— worker 实际上传/下载的 HTTP 请求走此地址（未配置回退对外）
_ENV_STORAGE_URL = E.ENV_STORAGE_URL
_ENV_STORAGE_INTERNAL_URL = E.ENV_STORAGE_INTERNAL_URL

# ===== 重试策略按「消息语义」分层 =====
# 铁律：**可丢弃的消息不该阻塞计算，必须成功的消息不该只试一次。**
#
#  ① 状态迁移（sub_status、result_path）——承重、必须送达：
#     失败仍会转 IPC 交父进程持久重试，故前台可以多试几次。
_REPORT_MAX_ATTEMPTS = 5
_REPORT_BACKOFF_BASE_SEC = 0.2  # 0.2, 0.4, 0.8, 1.6, ...
#
#  ② 进度消息（step_message）——纯提示、过期即无价值：
#     绝不能为一条提示让计算干等。短超时 + 2 次即弃（不入 IPC 队列——一条过期的
#     进度被父进程异步补发，只会覆盖掉更新的进度，反而有害）。
#     最坏阻塞 2×8s + 0.2s ≈ 16s。
_PROGRESS_MAX_ATTEMPTS = 2
_PROGRESS_TIMEOUT_SEC = 8.0
#
#  ③ 结果上传（upload_result）——产物是任务的全部价值，丢了就得整步重跑：
#     必须重试，且要盖住 storage 的一次重启。但重试期间**占着 Lane**（上传在任务子进程里，
#     算力此时全闲着），所以「次数与退避」可以放宽，「单次超时」必须按项精调。
_UPLOAD_MAX_ATTEMPTS = 5
_UPLOAD_BACKOFF_BASE_SEC = 2.0  # 2s, 4s, 8s, 16s —— 累计 30s 的重试窗口
#
# 【为什么是 30s】要盖住的是 storage 的一次重启：进程退出 + 服务起来 + 扫会话目录，
# 量级在秒到十几秒。30s 有富余；再长就是白等，而这段等待**占着 Lane**。
#
# 【超时必须四项分开，不能传标量】httpx 的 timeout=300.0 被定义为
# "300s timeout on **all operations**"——connect 也会变成 300s。跨公网/NAT 部署下
# 防火墙 DROP 掉 SYN 时报的正是 ConnectTimeout，于是一次连不上就抱着 Lane 干等 5 分钟，
# 5 次就是 25 分钟，什么也没干成。
#
# ⚠️ 这四项**全是空闲超时（per-operation），不是请求总时长**——httpx 没有「总超时」
# 这个概念。所以 write=60 不是「上传最多 60 秒」，而是「单次 socket 写最多卡 60 秒」：
# 几 GB 的产物只要传输一直有进展，跑多久都不会超时。
#   · connect=10  握手不需要更久；连不上就快速认输去重试
#   · write=60    发送端空闲上限，与大文件总时长无关
#   · read=300    请求体发完后**等 storage 应答**这一段（fsync + 原子改名可能很慢）
#   · pool=10     取连接不该慢；这里只有一个请求，池压力为零
_UPLOAD_TIMEOUT = httpx.Timeout(connect=10.0, write=60.0, read=300.0, pool=10.0)

# 业务规则拒绝（不重试，直接走异常）——重试也不会变好（如 sub_step 守门不匹配、任务已迁出）
_BUSINESS_REJECT_CODES = {400, 404}

# 可重试的传输层异常：httpx.TransportError 是 ConnectError / NetworkError / TimeoutException
#   （含 ConnectTimeout / WriteTimeout / PoolTimeout）/ ProtocolError / ProxyError 的共同基类。
# ⚠️ 手写元组容易漏掉 ConnectTimeout——跨公网/NAT 部署下防火墙 DROP 掉 SYN 包时报的正是它。
#    漏掉的后果不是崩溃，而是异常直接逃出重试循环、**跳过 IPC 兜底** → 状态更新永久丢失。
#    用基类一网打尽，且不会误吞业务异常（HTTPStatusError 不在 TransportError 下）。
_RETRIABLE_EXC = (httpx.TransportError, OSError)


def _send_with_retry(
    method: str,
    url: str,
    payload: dict,
    token: str,
    *,
    timeout: float = 30.0,
    max_attempts: int = _REPORT_MAX_ATTEMPTS,
) -> tuple[Optional[httpx.Response], Optional[BaseException]]:
    """前台同步重试请求（method 按协议：PATCH /steps、PATCH /fields 等）。

    Args:
        timeout: 单次请求超时（秒）
        max_attempts: 最大尝试次数（含首次）。进度类消息传 _PROGRESS_MAX_ATTEMPTS。

    Returns:
        (resp, None) 成功；resp.status_code 可能是 200 也可能是 4xx
        (None, last_exc) 网络层异常 N 次全失败
    """
    headers = {
        E.HEADER_INTERNAL_TOKEN: token,
        "Content-Type": "application/json",
        **_fencing_headers(),
    }
    last_exc: Optional[BaseException] = None
    for attempt in range(max_attempts):
        try:
            resp = httpx.request(method, url, headers=headers, json=payload, timeout=timeout)
            # 200 / 业务 4xx 直接返回，让上层判断
            if resp.status_code == 200 or resp.status_code in _BUSINESS_REJECT_CODES:
                return resp, None
            # 5xx / 其他 → 重试
            last_exc = httpx.HTTPError(
                f"HTTP {resp.status_code}: {resp.text[:200]}"
            )
        except _RETRIABLE_EXC as e:
            last_exc = e
        if attempt < max_attempts - 1:
            time.sleep(_REPORT_BACKOFF_BASE_SEC * (2 ** attempt))
    return None, last_exc


def _get_config() -> tuple[str, str]:
    """从环境变量读取任务服务的连接信息。"""
    base_url = os.environ.get(_ENV_BASE_URL, "")
    token = os.environ.get(_ENV_TOKEN, "")

    if not base_url or not token:
        raise EnvironmentError(
            f"缺少必要的环境变量: {_ENV_BASE_URL} 和 {_ENV_TOKEN}。"
            "这些变量由 Worker executor 在启动子进程时注入。"
        )

    return base_url, token


def _storage_targeted() -> bool:
    """是否配置了存储服务地址（内网或对外任一）。"""
    return bool(
        os.environ.get(_ENV_STORAGE_INTERNAL_URL, "").strip()
        or os.environ.get(_ENV_STORAGE_URL, "").strip()
    )


def _storage_token(fallback: str) -> str:
    """请求 storage 时用的 Token：**专属优先、回退主 Token**。

    与父进程（callback.storage_token / uploader）的回退链一致：
    配了独立密钥就全都用它，不配就全都回退，不会出现"一半能用"。
    """
    return os.environ.get(_ENV_STORAGE_TOKEN, "").strip() or fallback


def _get_storage_access_url() -> str:
    """worker 实际请求存储服务（上传/下载）的基础 URL：内网 → 对外。

    ⚠️ 协议规定产物只上传到存储服务（docs/worker-protocol.md §5），任务服务上没有
    对应端点，所以**没有回退到任务服务的路径**：两者都没配直接报错，比悄悄 404 好排查。
    仅用于 worker→storage 的实际 HTTP 请求；落库给前端的 download_url 恒用【对外】
    WEBX_WORKER_STORAGE_URL（见 upload_result），二者不可混用。
    """
    url = (
        os.environ.get(_ENV_STORAGE_INTERNAL_URL, "").strip()
        or os.environ.get(_ENV_STORAGE_URL, "").strip()
    )
    if not url:
        raise EnvironmentError(
            f"未配置存储服务地址: {_ENV_STORAGE_INTERNAL_URL} / {_ENV_STORAGE_URL}。"
            "产物只能上传到存储服务，没有回退路径。"
        )
    return url


def resolve_storage_url(download_url: str) -> str:
    """把「结果产物」的下载路径解析为可直接请求的绝对 URL（指向存储服务）。

    result_path 里存的 download_url 形如 ``/public/tasks/.../results/xxx``（相对路径，
    由上传端点回传）。取回产物是 worker 实际请求，故拼【内网】访问地址
    （WEBX_WORKER_STORAGE_INTERNAL_URL → 对外），否则 httpx 请求相对路径会直接抛
    UnsupportedProtocol。若 download_url 已是绝对 URL，原样透传。

    Args:
        download_url: 相对路径或绝对 URL。

    Returns:
        绝对 URL；已是 http(s) 绝对地址则原样返回；空值原样返回。
    """
    if not download_url:
        return download_url
    if download_url.startswith(("http://", "https://")):
        return download_url
    # 不复用抛异常的 _get_storage_access_url：取回路径不应因配置缺失而崩
    access = (
        os.environ.get(_ENV_STORAGE_INTERNAL_URL, "").strip()
        or os.environ.get(_ENV_STORAGE_URL, "").strip()
    )
    return f"{access.rstrip('/')}/{download_url.lstrip('/')}" if access else download_url


def upload_result(
    task_id: str,
    file_path: str | Path,
    filename: Optional[str] = None,
) -> dict:
    """上传结果文件到存储服务，返回含下载 URL 的响应。

    `POST {存储服务}/internal/tasks/<task_id>/results`（multipart，创建成功 201，
    见 docs/storage-service.md）。实际请求走【内网】访问地址，落库 download_url 则用
    【对外】地址（见下）。超大文件应走 uploader 的分片续传（见 upload_route）。
    """
    base_url = _get_storage_access_url()
    _, main_token = _get_config()

    local_path = Path(file_path)
    if not local_path.exists():
        raise FileNotFoundError(f"文件不存在: {local_path}")

    target_name = filename or local_path.name

    # rstrip 防配置末尾斜杠拼出 //（严格反代会 404/400）
    url = f"{base_url.rstrip('/')}/internal/tasks/{task_id}/results"
    token = _storage_token(main_token)

    # 带重试上传：产物是任务的全部价值，一次网络抖动就丢掉，意味着 url 置空、本地副本又会被
    # 后续 redo/清理清掉 → **永久丢失**，用户只能整步重跑。
    # ⚠️ 文件句柄必须【每次重开】：httpx 会把流读到底，复用已消费的句柄会上传 0 字节。
    resp: Optional[httpx.Response] = None
    last_exc: Optional[BaseException] = None
    for attempt in range(_UPLOAD_MAX_ATTEMPTS):
        try:
            with open(local_path, "rb") as f:
                resp = httpx.post(
                    url,
                    headers={E.HEADER_INTERNAL_TOKEN: token},
                    files={"file": (target_name, f)},
                    data={"filename": target_name} if filename else {},
                    timeout=_UPLOAD_TIMEOUT,
                )
            # 创建成功是 201（存储服务规范路径）；兼容 200
            if resp.status_code in (200, 201):
                break
            # 业务拒绝（4xx：鉴权失败 / 任务不存在 / 超限）→ 重试无意义，立即抛
            if 400 <= resp.status_code < 500:
                break
            # 5xx（存储服务重启 / 反代 502）→ 重试
            last_exc = httpx.HTTPError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        except _RETRIABLE_EXC as e:
            last_exc, resp = e, None
        if attempt < _UPLOAD_MAX_ATTEMPTS - 1:
            wait = _UPLOAD_BACKOFF_BASE_SEC * (2 ** attempt)
            logger.warning(
                f"结果文件上传失败，{wait:.0f}s 后重试 "
                f"({attempt + 1}/{_UPLOAD_MAX_ATTEMPTS}): file={target_name}, error={last_exc}"
            )
            time.sleep(wait)

    if resp is None:
        logger.error(
            f"结果文件上传失败（{_UPLOAD_MAX_ATTEMPTS} 次重试均失败）: "
            f"task_id={task_id}, file={target_name}, error={last_exc}"
        )
        raise httpx.HTTPError(f"上传失败（网络层，{_UPLOAD_MAX_ATTEMPTS} 次重试）: {last_exc}")

    if resp.status_code not in (200, 201):
        logger.error(
            f"结果文件上传失败: task_id={task_id}, file={target_name}, "
            f"status={resp.status_code}, body={resp.text[:200]}"
        )
        raise httpx.HTTPError(
            f"上传失败 (HTTP {resp.status_code}): {resp.text[:200]}"
        )

    result = resp.json()

    # 落库/前端用的 download_url 归一化：存储端回传的是相对路径（/public/tasks/.../results/xxx）。
    # ★ 恒用【对外】WEBX_WORKER_STORAGE_URL 拼接（面向浏览器的公网地址），与上面请求用的
    #   内网地址严格解耦——即便 worker 经内网上传，落库给前端的仍是浏览器够得着的对外地址。
    #   未配置对外地址则保持相对路径，由前端按页面 origin 解析。
    public_url = os.environ.get(_ENV_STORAGE_URL, "").strip()
    rel_url = result.get("download_url")
    if (
        rel_url
        and not rel_url.startswith(("http://", "https://"))
        and public_url
    ):
        result["download_url"] = f"{public_url.rstrip('/')}/{rel_url.lstrip('/')}"

    logger.info(
        f"结果文件上传成功: task_id={task_id}, file={target_name}, "
        f"download_url={result.get('download_url')}"
    )
    return result


def update_task_fields(
    task_id: str,
    result_path: Optional[str] = None,
    log_path: Optional[str] = None,
    mode: str = "overwrite",
) -> dict:
    """更新任务字段（result_path / log_path）→ PATCH /internal/tasks/{id}/fields[?mode=merge]

    Args:
        task_id: 任务 ID
        result_path: 新的 result_path 值（None=不更新）
        log_path: 新的 log_path 值（None=不更新）
        mode: "overwrite"（直接覆盖）或 "merge"（按 key 深度合并）

    Returns:
        服务端响应 dict（5 次重试后仍失败 → 通过 IPC 入父进程队列，本地返回占位）
    """
    base_url, token = _get_config()

    url = f"{base_url}/internal/tasks/{task_id}/fields"
    if mode == "merge":
        url += "?mode=merge"

    payload = {}
    if result_path is not None:
        payload["result_path"] = result_path
    if log_path is not None:
        payload["log_path"] = log_path

    if not payload:
        logger.warning(f"无字段需更新: task_id={task_id}")
        return {"task_id": task_id, "message": "No fields to update"}

    resp, last_exc = _send_with_retry("PATCH", url, payload, token)
    if resp is not None and resp.status_code == 200:
        result = resp.json()
        logger.info(
            f"任务字段更新成功: task_id={task_id}, mode={mode}, fields={list(payload.keys())}"
        )
        return result

    # 200 之外的情况：业务拒绝 (4xx) 或 5xx 重试耗尽
    if resp is not None and resp.status_code in _BUSINESS_REJECT_CODES:
        logger.error(
            f"任务字段更新被业务拒绝（不入队）: task_id={task_id}, "
            f"status={resp.status_code}, body={resp.text[:200]}"
        )
        raise httpx.HTTPError(
            f"更新失败 (HTTP {resp.status_code}): {resp.text[:200]}"
        )

    # 5xx / 网络错误 → 发 IPC 入父进程队列，业务继续推进
    logger.warning(
        f"任务字段更新 5 次重试失败，转交父进程异步重试: task_id={task_id}, "
        f"last_exc={last_exc}"
    )
    # 把 mode 编进 payload 给 PendingReporter 用
    ipc_payload = dict(payload)
    ipc_payload["_mode"] = mode
    send_report("update_task_fields", ipc_payload, task_id=task_id)
    return {"task_id": task_id, "message": "Queued for async retry"}


def update_step_status(
    task_id: str,
    steps: list[str] | None = None,
    sub_step: str | None = None,
    sub_status: str | None = None,
    step_message: str | None = None,
    *,
    max_attempts: int = _REPORT_MAX_ATTEMPTS,
    timeout: float = 30.0,
    enqueue_on_failure: bool = True,
) -> dict:
    """更新步骤进度（steps/sub_step/sub_status/step_message）→ PATCH /internal/tasks/{id}/steps

    Args:
        task_id: 任务 ID
        steps: 已完成步骤列表（覆盖写入）
        sub_step: 当前步骤名
        sub_status: 步骤状态 (pending/running/success/failed)
        step_message: 当前步骤动态消息（服务端要求同时传 sub_step 且匹配）
        max_attempts: 前台同步重试次数。**承重的状态迁移**（sub_status）用默认值；
            纯进度消息由 ``progress.report_step_message`` 传 ``_PROGRESS_MAX_ATTEMPTS``。
        timeout: 单次请求超时（秒）
        enqueue_on_failure: 前台失败后是否转 IPC 交父进程持久重试。**状态迁移必须 True**
            （丢了会让任务卡在 running）；**纯进度消息必须 False**——一条过期的进度被父进程
            异步补发，只会覆盖掉此刻更新的进度，不是"补救"而是"倒退"。

    Returns:
        响应字典
    """
    base_url, token = _get_config()

    url = f"{base_url}/internal/tasks/{task_id}/steps"

    payload = {}
    if steps is not None:
        payload["steps"] = steps
    if sub_step is not None:
        payload["sub_step"] = sub_step
    if sub_status is not None:
        payload["sub_status"] = sub_status
    if step_message is not None:
        payload["step_message"] = step_message

    resp, last_exc = _send_with_retry(
        "PATCH", url, payload, token, timeout=timeout, max_attempts=max_attempts
    )
    if resp is not None and resp.status_code == 200:
        result = resp.json()
        logger.info(f"步骤进度更新成功: task_id={task_id}, payload={payload}")
        return result

    # 业务拒绝（如 sub_status 迁移规则不符 / step_message 守门）→ 直接抛，不入队
    if resp is not None and resp.status_code in _BUSINESS_REJECT_CODES:
        logger.error(
            f"步骤进度更新被业务拒绝（不入队）: task_id={task_id}, "
            f"status={resp.status_code}, body={resp.text[:200]}"
        )
        raise httpx.HTTPError(
            f"更新失败 (HTTP {resp.status_code}): {resp.text[:200]}"
        )

    # 5xx / 网络错误 → 承重消息转 IPC 入父进程队列，业务继续
    if not enqueue_on_failure:
        # 纯进度消息：失败即弃（见 enqueue_on_failure 参数说明）
        logger.debug(
            f"步骤进度消息上报失败（已丢弃，不入队）: task_id={task_id}, last_exc={last_exc}"
        )
        return {"task_id": task_id, "message": "Progress message dropped"}

    logger.warning(
        f"步骤状态更新 {max_attempts} 次重试失败，转交父进程异步重试: task_id={task_id}, "
        f"last_exc={last_exc}"
    )
    send_report("update_step_status", payload, task_id=task_id)
    return {"task_id": task_id, "message": "Queued for async retry"}


def download_artifact(
    task_id: str,
    filename: str,
    save_path: str | Path,
) -> bool:
    """从存储服务下载结果文件（小文件；整份读进内存，大文件请用 download_artifact_stream）。

    使用公开下载路径，无需认证。

    Args:
        task_id: 任务 ID
        filename: 结果文件名
        save_path: 本地保存路径

    Returns:
        是否下载成功（404 返回 False，不抛异常）
    """
    access_url = _get_storage_access_url()

    local_path = Path(save_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # 公开下载路径（无需 Internal Token），走【内网】访问地址从存储服务拉取
    # rstrip 防配置末尾斜杠拼出 //（与 upload/resolve 归一化口径一致）
    url = f"{access_url.rstrip('/')}/public/tasks/{task_id}/results/{filename}"

    try:
        resp = httpx.get(url, timeout=300.0)

        if resp.status_code == 200:
            local_path.write_bytes(resp.content)
            logger.debug(f"产物下载成功: task_id={task_id}, filename={filename}")
            return True
        elif resp.status_code == 404:
            logger.warning(f"产物 404: task_id={task_id}, filename={filename}")
            return False
        else:
            logger.error(
                f"产物下载失败: task_id={task_id}, filename={filename}, "
                f"status={resp.status_code}"
            )
            return False
    except Exception as e:
        logger.error(f"产物下载异常: task_id={task_id}, filename={filename}, error={e}")
        return False


# ===== 大文件流式下载 =====
# 与上面的 download_artifact 并存、互不影响：后者 `resp.content` 整个读进内存、总超时 300s、
# 无 Range，只适合 KB~MB 级小产物；数 GB 的文件必须走这里。
# read 是「相邻两个数据块之间」的空闲超时，不是总时长——大文件下载总时长不设限。
_STREAM_TIMEOUT = httpx.Timeout(connect=10.0, write=30.0, read=120.0, pool=10.0)
_STREAM_MAX_ATTEMPTS = 5
_STREAM_BACKOFF_BASE_SEC = 2.0


def list_artifacts(task_id: str, *, client: Optional[httpx.Client] = None) -> Optional[list[str]]:
    """列出任务 results 目录下的文件名（公开端点，无需认证）。

    Returns:
        文件名列表（目录不存在时存储服务返回空列表）；
        **请求失败返回 None** —— 「不知道」与「确实为空」要分开，调用方据此决定是否回源。
    """
    url = f"{_get_storage_access_url().rstrip('/')}/public/tasks/{task_id}/results"
    own = client is None
    c = client or httpx.Client(timeout=30.0)
    try:
        resp = c.get(url)
        if resp.status_code != 200:
            logger.warning(f"列目录失败: task_id={task_id}, status={resp.status_code}")
            return None
        files = resp.json().get("files")
        return [str(f) for f in files] if isinstance(files, list) else None
    except Exception as e:  # 网络/解析异常一律当「不知道」，不让探测拖垮任务
        logger.warning(f"列目录异常: task_id={task_id}, error={e}")
        return None
    finally:
        if own:
            c.close()


def _parse_content_range(value: str) -> tuple[Optional[int], Optional[int]]:
    """``bytes a-b/total`` 或 ``bytes */total`` → (起点或 None, 总长或 None)。"""
    try:
        unit, _, rest = value.partition(" ")
        if unit.strip().lower() != "bytes":
            return None, None
        span, _, total = rest.partition("/")
        start = None if span.strip() == "*" else int(span.split("-", 1)[0])
        return start, (None if total.strip() == "*" else int(total))
    except (ValueError, AttributeError):
        return None, None


def download_artifact_stream(
    task_id: str,
    filename: str,
    save_path: str | Path,
    *,
    client: Optional[httpx.Client] = None,
    max_attempts: int = _STREAM_MAX_ATTEMPTS,
    backoff_base: float = _STREAM_BACKOFF_BASE_SEC,
) -> bool:
    """流式下载大文件：分块落盘、Range 断点续传、完成后原子改名。

    - 先写 ``{save_path}.part``；重试/再次调用时从 ``.part`` 现有长度续传（``Range: bytes=N-``）。
    - 服务端忽略 Range 回 200 → 丢弃 ``.part`` 从头来；206 的起点与本地长度不符 → 同样从头来；
      416 且总长恰等于本地长度 → 视为已完整，直接收尾；416 其余情形 → 丢弃重来。
    - 落盘长度与服务端声明的总长不符（连接被截断）算失败并重试。
    - 404 / 其他 4xx 不重试，返回 False；5xx 与传输层异常按指数退避重试。
    - 全部失败返回 False，**保留 ``.part``** 供下次续传；成功返回 True 且 ``.part`` 已改名。

    Args:
        client: 可注入的 httpx.Client（测试用）；缺省自建，超时见 ``_STREAM_TIMEOUT``。
    """
    url = f"{_get_storage_access_url().rstrip('/')}/public/tasks/{task_id}/results/{filename}"
    final = Path(save_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_name(final.name + ".part")

    own = client is None
    c = client or httpx.Client(timeout=_STREAM_TIMEOUT)
    try:
        for attempt in range(max_attempts):
            offset = part.stat().st_size if part.exists() else 0
            # identity：续传偏移按「传输字节」算，若被 gzip 等内容编码，偏移会与落盘字节错位
            headers = {"Accept-Encoding": "identity"}
            if offset > 0:
                headers["Range"] = f"bytes={offset}-"
            try:
                with c.stream("GET", url, headers=headers) as resp:
                    code = resp.status_code
                    if code == 404:
                        logger.warning(f"产物 404: task_id={task_id}, filename={filename}")
                        part.unlink(missing_ok=True)
                        return False
                    if code == 416:
                        _, total = _parse_content_range(resp.headers.get("content-range", ""))
                        if total is not None and total == offset:
                            part.replace(final)
                            return True
                        part.unlink(missing_ok=True)  # 本地残片与远端对不上：丢弃重来
                        continue
                    if code in (200, 206):
                        if code == 206:
                            start, total = _parse_content_range(resp.headers.get("content-range", ""))
                            if start != offset:
                                part.unlink(missing_ok=True)
                                continue
                            mode = "ab"
                        else:  # 200：服务端没按 Range 给，整份重来
                            cl = resp.headers.get("content-length")
                            total = int(cl) if cl and cl.isdigit() else None
                            mode = "wb"
                        with open(part, mode) as f:
                            # iter_raw 不解码、不重新分块：每收到一块就落盘，断线时丢得最少
                            for chunk in resp.iter_raw():
                                f.write(chunk)
                        got = part.stat().st_size
                        if total is not None and got != total:
                            raise httpx.TransportError(f"下载被截断: {got}/{total}")
                        part.replace(final)
                        return True
                    if 400 <= code < 500:
                        logger.error(f"产物下载被拒: {filename}, status={code}")
                        return False
                    raise httpx.HTTPError(f"HTTP {code}")  # 5xx → 重试
            except _RETRIABLE_EXC + (httpx.HTTPError,) as e:
                logger.warning(
                    f"产物流式下载失败 ({attempt + 1}/{max_attempts}): "
                    f"task_id={task_id}, filename={filename}, error={e}"
                )
            if attempt < max_attempts - 1 and backoff_base > 0:
                time.sleep(backoff_base * (2 ** attempt))
        return False
    finally:
        if own:
            c.close()
