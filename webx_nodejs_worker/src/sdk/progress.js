'use strict'

/**
 * 进度回报薄封装 —— 供任务脚本在子进程中调用。对应 Python sdk/progress.py。
 *
 * 只做一件事：把 (taskId, subStep, message) 写入云端 task.step_message。
 * 所有异常静默吞掉（网络故障 / 服务端 400 守门拒绝 / 任务已迁出 RUNNING / 租约已丢）都不影响主流程。
 */

const { getLogger } = require('../log')
const { updateStepStatus, PROGRESS_MAX_ATTEMPTS, PROGRESS_TIMEOUT_SEC } = require('./resultClient')

const logger = getLogger('sdk.progress')

/**
 * 上报当前步骤的动态进度消息（最佳努力，永不 reject）。
 *
 * @param {string|null} taskId   缺失则直接返回，不构成致命错误
 * @param {string|null} subStep  必须与服务端 task.sub_step 一致。【空串是合法值】：单阶段任务的 sub_step
 *                               恒为空串，服务端守门比的是「相等」；真正的「没有」用 null/undefined，那才早退
 * @param {string} message       显示给前端的进度文本。文案契约：一律英文短句；不出现具体软件名
 *
 * 重试语义：短超时 + 2 次即弃，且不入 IPC 队列 —— 进度消息过期即无价值，异步补发只会覆盖掉更新的进度。
 * 最坏阻塞约 2×8s + 0.2s。
 */
async function reportStepMessage(taskId, subStep, message) {
  if (!taskId || subStep == null) return
  try {
    await updateStepStatus(taskId, {
      subStep,
      stepMessage: message,
      maxAttempts: PROGRESS_MAX_ATTEMPTS,
      timeout: PROGRESS_TIMEOUT_SEC,
      enqueueOnFailure: false,
    })
  } catch (e) {
    logger.debug(`step_message 上报失败（已忽略）: task_id=${taskId}, sub_step=${subStep}, error=${e && e.message}`)
  }
}

module.exports = { reportStepMessage }
