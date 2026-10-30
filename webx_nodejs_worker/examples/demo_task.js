'use strict'
/**
 * 演示任务脚本 —— 一个任务子进程能做的全部事情，不含任何业务。
 *
 * executor 以任务目录为 cwd 拉起本脚本并追加 `--task-id <ID>`：
 *   <WORK_DIR>/<task_id>/  params.json  meta.json  in/  out/
 * params 可选字段：sleep（数字，先睡这么多秒，供取消 / 超时测试）、fail（真则退出码 3）。
 */
const fs = require('node:fs')
const path = require('node:path')
const sdk = require('webx_nodejs_worker/src/sdk') // executor 已把 NODE_PATH 设为包目录的父目录

const readJson = (f, dflt) => { try { return JSON.parse(fs.readFileSync(f, 'utf8')) } catch (_) { return dflt } }

async function main() {
  sdk.configureTaskLogging()
  const i = process.argv.indexOf('--task-id')
  const taskId = (i >= 0 && process.argv[i + 1]) || process.env.WEBX_WORKER_TASK_ID || ''
  const params = readJson('params.json', {})
  const subStep = readJson('meta.json', {}).sub_step || '' // 单阶段任务恒为空串（空串是合法值，不是「没有」）
  console.log(`task ${taskId} params=${JSON.stringify(params)} greeting=${process.env.DEMO_GREETING}`)

  await sdk.reportStepMessage(taskId, subStep, 'Preparing') // 最佳努力，永不抛出
  if (typeof params.sleep === 'number') await new Promise((r) => setTimeout(r, params.sleep * 1000))
  if (params.fail) process.exit(3)

  fs.mkdirSync('out', { recursive: true })
  const result = path.join('out', 'result.txt')
  fs.writeFileSync(result, `result of ${taskId}\n`)

  // deliverRequired = 下游依赖，恒同步；deliverOnly = 纯交付，大文件交 uploader。进队列 / 失败都返回 ''
  const url = await sdk.deliverOnly(taskId, result, 'result.txt', ['outputs', 'report_url'])
  if (url) {
    await sdk.updateTaskFields(taskId, { resultPath: JSON.stringify({ outputs: { report_url: url } }), mode: 'merge' })
  }
  await sdk.reportStepMessage(taskId, subStep, 'Done')
  process.exit(0)
}

main().catch((err) => { console.error(err); process.exit(1) })
