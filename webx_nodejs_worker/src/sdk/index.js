'use strict'

/**
 * webx_nodejs_worker 任务子进程侧 SDK —— 任务脚本里 require 的那一半（对应 Python 的 webx_worker.sdk）。
 *
 * 用法（executor 已把 NODE_PATH 设为包目录的父目录，包目录名固定为 webx_nodejs_worker）：
 *
 *   const sdk = require('webx_nodejs_worker/src/sdk')
 *   // 或按需取子模块：
 *   const { deliverRequired } = require('webx_nodejs_worker/src/sdk/uploadRoute')
 *
 *   const taskId = process.argv[process.argv.indexOf('--task-id') + 1]
 *   sdk.configureTaskLogging()
 *   await sdk.reportStepMessage(taskId, subStep, 'Preparing')           // 最佳努力，永不抛出
 *   const url = await sdk.deliverRequired(taskId, 'out/a.txt', 'a.txt', ['outputs', 'a_url'])
 *   await sdk.updateTaskFields(taskId, { resultPath: JSON.stringify({ outputs: { a_url: url } }), mode: 'merge' })
 *
 * 要点：
 *   · 所有网络 / 交付函数都是 async，务必 await（尤其是退出前）；
 *   · 只依赖 envnames.js / httpClient.js / log.js 与 Node 标准库，不读 config.js 的 settings 单例：
 *     连接信息与 fencing 令牌全部来自 executor 注入的环境变量（见 envnames.js）；
 *   · 最小示例见包目录下的 examples/demo_task.js。
 *
 * 模块一览：
 *   resultClient  上传产物 / 更新字段 / 更新步骤进度 / 取回产物（分层重试 + IPC 兜底）
 *   progress      步骤动态消息（最佳努力，永不抛出）
 *   ipcClient     失败事件经 AF_UNIX 交回 worker 父进程做持久重放（不通则降级写 <taskDir>/pending_reports.jsonl）
 *   uploadRoute   产物交付的分流咽喉：同步上传 or 投递给 uploader
 *   uploadQueue   磁盘目录即消息的异步上传队列（投递侧与 uploader 消费侧共用）；以命名空间导出，不展开
 *   logsetup      子进程日志配置
 */

const resultClient = require('./resultClient')
const progress = require('./progress')
const ipcClient = require('./ipcClient')
const uploadRoute = require('./uploadRoute')
const uploadQueue = require('./uploadQueue')
const logsetup = require('./logsetup')

module.exports = {
  // resultClient
  uploadResult: resultClient.uploadResult,
  updateTaskFields: resultClient.updateTaskFields,
  updateStepStatus: resultClient.updateStepStatus,
  downloadArtifact: resultClient.downloadArtifact,
  downloadArtifactStream: resultClient.downloadArtifactStream,
  listArtifacts: resultClient.listArtifacts,
  resolveStorageUrl: resultClient.resolveStorageUrl,
  checkResultFilename: resultClient.checkResultFilename,
  HttpError: resultClient.HttpError,
  // progress
  reportStepMessage: progress.reportStepMessage,
  // ipcClient
  setTaskContext: ipcClient.setTaskContext,
  updateTaskContext: ipcClient.updateTaskContext,
  sendReport: ipcClient.sendReport,
  // uploadRoute
  deliverRequired: uploadRoute.deliverRequired,
  deliverOnly: uploadRoute.deliverOnly,
  deliver: uploadRoute.deliver,
  // logsetup
  configureTaskLogging: logsetup.configureTaskLogging,
  // 子模块命名空间
  resultClient, progress, ipcClient, uploadRoute, uploadQueue, logsetup,
}
