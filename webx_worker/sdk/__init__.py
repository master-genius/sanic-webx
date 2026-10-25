"""任务子进程侧 SDK —— 任务脚本里 `import webx_worker.sdk.*` 用的那一半。

⚠️ 只依赖标准库 + httpx（子进程可能跑在与 worker 不同的解释器 / 环境里）。
连接信息与 fencing 令牌全部从环境变量读（由 worker 的 executor 注入，见 envnames.py）。

模块一览：
  result_client   上传产物 / 更新字段 / 更新步骤进度 / 取回产物（带分层重试 + IPC 兜底）
  progress        步骤动态消息（最佳努力，永不抛出）
  ipc_client      失败事件经 AF_UNIX 交回 worker 父进程做持久重放（不通则降级写文件）
  upload_route    产物交付的分流咽喉：同步上传 or 投递给 uploader
  upload_queue    磁盘目录即消息的异步上传队列（投递侧与消费侧共用）
  logsetup        子进程日志配置
"""
