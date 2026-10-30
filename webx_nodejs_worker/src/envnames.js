'use strict'

/**
 * 父子进程共用的环境变量名 —— 只依赖 Node 标准库，且是这些名字的唯一来源。
 * 与 Python 版 webx_worker/envnames.py 逐字一致（两个实现可互换、任务脚本可混用）。
 *
 * 分三组：
 *   ① 配置层（父进程自己读，见 config.js）
 *   ② fencing 令牌（每个任务一份，父进程注入，子进程只读）
 *   ③ 通道（IPC socket、工作目录、上传开关）
 */

module.exports = Object.freeze({
  PREFIX: 'WEBX_WORKER_',

  // ② fencing 令牌 = (worker_id, lease_seq)。漏注入的现象是「上报全被 403」
  ENV_WORKER_ID: 'WEBX_WORKER_ID',
  ENV_LEASE_SEQ: 'WEBX_WORKER_LEASE_SEQ',
  ENV_TASK_ID: 'WEBX_WORKER_TASK_ID',

  // ① 子进程需要的连接信息
  ENV_INTERNAL_TOKEN: 'WEBX_WORKER_INTERNAL_TOKEN',
  ENV_WEB_BASE_URL: 'WEBX_WORKER_WEB_BASE_URL',
  // 存储服务地址双轨：STORAGE_URL【对外】恒用于落库的 download_url；
  // STORAGE_INTERNAL_URL【内网】是 worker 实际请求走的地址（未配置回退对外）
  ENV_STORAGE_URL: 'WEBX_WORKER_STORAGE_URL',
  ENV_STORAGE_INTERNAL_URL: 'WEBX_WORKER_STORAGE_INTERNAL_URL',
  ENV_STORAGE_INTERNAL_TOKEN: 'WEBX_WORKER_STORAGE_INTERNAL_TOKEN',

  // ③ 通道
  ENV_WORK_DIR: 'WEBX_WORKER_WORK_DIR',
  ENV_IPC_SOCK: 'WEBX_WORKER_IPC_SOCK',
  ENV_UPLOADER_ENABLED: 'WEBX_WORKER_UPLOADER_ENABLED',
  ENV_UPLOAD_ASYNC_THRESHOLD_MB: 'WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB',
  ENV_UPLOAD_REQUIRED_GLOBS: 'WEBX_WORKER_UPLOAD_REQUIRED_GLOBS',
  ENV_TASK_LOG_LEVEL: 'WEBX_WORKER_TASK_LOG_LEVEL',

  // fencing header 名（与 docs/worker-protocol.md §2 一致）
  HEADER_WORKER_ID: 'X-Worker-Id',
  HEADER_LEASE_SEQ: 'X-Lease-Seq',
  HEADER_INTERNAL_TOKEN: 'X-Internal-Token',
})
