"""父子进程共用的环境变量名 —— **只依赖标准库**，且是这些名字的唯一来源。

worker 父进程（executor）把运行时信息注入任务子进程的环境；子进程侧 SDK
（`webx_worker.sdk.*`）从环境读回来。两侧各写一份字面量必然漂移，漂了的后果是
「令牌/地址没传到子进程」——表现为子进程所有上报静默 403，极难联想。
故统一收口在这里，两侧都 import 它。

命名约定：全部以 `WEBX_WORKER_` 为前缀。

分三组：

  ① 配置层（父进程自己读，见 `config.WorkerSettings`）：
     WEBX_WORKER_WEB_BASE_URL 等。父进程把其中子进程需要的几项原样注入子进程环境。
  ② fencing 令牌（每个任务一份，父进程注入，子进程只读）：
     WEBX_WORKER_ID / WEBX_WORKER_LEASE_SEQ / WEBX_WORKER_TASK_ID
  ③ 通道（IPC socket、工作目录、上传开关）。
"""

PREFIX = "WEBX_WORKER_"

# ── ② fencing 令牌 = (worker_id, lease_seq)，随每个写请求发送 ─────────────────
# ⚠️ 子进程的所有回调都靠它通过服务端 fencing。漏注入的现象是「上报全被 403」。
ENV_WORKER_ID = "WEBX_WORKER_ID"
ENV_LEASE_SEQ = "WEBX_WORKER_LEASE_SEQ"
# 便利项：当前任务 ID（脚本也可以只用 --task-id 参数）
ENV_TASK_ID = "WEBX_WORKER_TASK_ID"

# ── ① 子进程需要的连接信息 ───────────────────────────────────────────────────
ENV_INTERNAL_TOKEN = "WEBX_WORKER_INTERNAL_TOKEN"
ENV_WEB_BASE_URL = "WEBX_WORKER_WEB_BASE_URL"
# 存储服务地址双轨（严格分离）：
#   STORAGE_URL           【对外】—— 恒用于「落库的 download_url」（面向浏览器）
#   STORAGE_INTERNAL_URL  【内网】—— worker 实际上传/下载请求走此地址（未配置回退对外）
ENV_STORAGE_URL = "WEBX_WORKER_STORAGE_URL"
ENV_STORAGE_INTERNAL_URL = "WEBX_WORKER_STORAGE_INTERNAL_URL"
ENV_STORAGE_INTERNAL_TOKEN = "WEBX_WORKER_STORAGE_INTERNAL_TOKEN"

# ── ③ 通道 ─────────────────────────────────────────────────────────────────
ENV_WORK_DIR = "WEBX_WORKER_WORK_DIR"
ENV_IPC_SOCK = "WEBX_WORKER_IPC_SOCK"
ENV_UPLOADER_ENABLED = "WEBX_WORKER_UPLOADER_ENABLED"
ENV_UPLOAD_ASYNC_THRESHOLD_MB = "WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB"
# 子进程分流规则：逗号分隔的 fnmatch 模式，命中的产物恒走同步上传（见 sdk.upload_route）
ENV_UPLOAD_REQUIRED_GLOBS = "WEBX_WORKER_UPLOAD_REQUIRED_GLOBS"
# 子进程日志级别（见 sdk.logsetup）
ENV_TASK_LOG_LEVEL = "WEBX_WORKER_TASK_LOG_LEVEL"

# fencing header 名（HTTP 层，与 docs/worker-protocol.md §2 一致）
HEADER_WORKER_ID = "X-Worker-Id"
HEADER_LEASE_SEQ = "X-Lease-Seq"
HEADER_INTERNAL_TOKEN = "X-Internal-Token"
