'use strict'

/**
 * IPC 客户端 —— 任务脚本把「前台重试仍失败」的上报事件交回 worker 父进程做持久重放。对应 Python sdk/ipc_client.py。
 *
 * 语义：sendReport() fire-and-forget，永不抛出；业务调用前台已重试过，走到这里才发 IPC。
 *   · 通道：AF_UNIX（路径来自 WEBX_WORKER_IPC_SOCK），lazy connect 长连接，一行一个 JSON；
 *   · 发不出（没配 / 连不上 / 写失败）→ 降级追加到 <WEBX_WORKER_WORK_DIR>/<task_id>/pending_reports.jsonl，
 *     并在本进程内不再尝试 IPC；父进程只在启动恢复时扫这个文件；
 *   · fencing 令牌随条目快照（entry.fencing）：事件可能很久之后甚至跨 worker 重启才被重放，
 *     那时只有条目自己记得「当时是哪一次租约」。
 *
 * 与 Python 版的差异：
 *   · sendReport 返回 Promise（Node 的 write 不像 sendall 那样返回即入内核）；内部调用方会 await 它，
 *     这样 `await updateStepStatus(); process.exit()` 不会丢事件。它永不 reject；
 *   · socket 只在写出期间 ref，空闲时 unref，不拖住脚本退出；写入串行化（对应 Python 的锁）；
 *   · 条目线格式沿用 snake_case：task_id / action / payload / context / ts / fencing{worker_id, lease_seq}。
 */

const fs = require('node:fs')
const net = require('node:net')
const path = require('node:path')
const E = require('../envnames')
const { getLogger } = require('../log')
const { envStr, nowSec } = require('./util')

const logger = getLogger('sdk.ipcClient')

const CONNECT_TIMEOUT_SEC = 2.0
const SEND_TIMEOUT_SEC = 2.0

// ─── task 上下文 ─────────────────────────────────────────────────────────────

const CTX = { taskId: '', status: 'running', subStatus: '', subStep: '', endStep: '', stepsDone: [] }

/** 任务脚本启动时调用，初始化任务上下文（sendReport 自动附加到 entry.context） */
function setTaskContext(taskId, status = 'running', subStatus = '', subStep = '', endStep = '', stepsDone = null) {
  Object.assign(CTX, {
    taskId, status, subStatus, subStep, endStep, stepsDone: stepsDone ? [...stepsDone] : [],
  })
}

/** 步骤推进时增量更新上下文：updateTaskContext({subStep: 'x', stepsDone: [...]})；未知键忽略 */
function updateTaskContext(fields = {}) {
  for (const [k, v] of Object.entries(fields)) {
    if (!(k in CTX)) continue
    CTX[k] = k === 'stepsDone' && v != null ? [...v] : v
  }
}

const snapshotContext = () => ({
  status: CTX.status,
  sub_status: CTX.subStatus,
  sub_step: CTX.subStep,
  end_step: CTX.endStep,
  steps_done: [...CTX.stepsDone],
})

// ─── IPC socket（lazy connect 长连接）──────────────────────────────────────

let sock = null
let fileFallbackActive = false // 一旦降级写文件，本次子进程内不再重试 IPC
let chain = Promise.resolve() // 写入串行化

function dropSocket() {
  const s = sock
  sock = null
  if (s) { try { s.destroy() } catch (_) { /* 已关闭 */ } }
}

/** 建立 AF_UNIX 连接。失败返回 null */
function connectIpc() {
  const sockPath = envStr(E.ENV_IPC_SOCK)
  if (!sockPath) return Promise.resolve(null)
  return new Promise((resolve) => {
    let settled = false
    const s = net.createConnection({ path: sockPath })
    const finish = (ok, err) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      if (ok) {
        // 建连后的异常 / 对端关闭 → 视同发送失败：本进程内转文件降级
        s.on('error', () => { if (sock === s) { sock = null; fileFallbackActive = true } })
        s.on('close', () => { if (sock === s) { sock = null; fileFallbackActive = true } })
        s.on('data', () => {}) // 父进程不回数据；消费掉以防缓冲
        resolve(s)
      } else {
        logger.warning(`[IPC] 连接父进程失败: ${err && err.message ? err.message : err}，降级写文件`)
        s.destroy()
        resolve(null)
      }
    }
    const timer = setTimeout(() => finish(false, new Error('connect timeout')), CONNECT_TIMEOUT_SEC * 1000)
    s.once('connect', () => finish(true))
    s.on('error', (e) => finish(false, e)) // 常驻：建连后的异常交给 finish 之后追加的处理器，这里变空操作
  })
}

/** 写一行并等它真正交给内核；超时 / 出错返回 false */
function writeLine(s, line) {
  return new Promise((resolve) => {
    let done = false
    const fin = (ok) => {
      if (done) return
      done = true
      clearTimeout(timer)
      resolve(ok)
    }
    const timer = setTimeout(() => fin(false), SEND_TIMEOUT_SEC * 1000)
    try {
      s.write(line, (err) => fin(!err))
    } catch (_) {
      fin(false)
    }
  })
}

async function sendViaSocket(entry) {
  if (fileFallbackActive) return false
  if (!sock) {
    sock = await connectIpc()
    if (!sock) { fileFallbackActive = true; return false }
    sock.unref()
  }
  const s = sock
  s.ref() // 写出期间保活，写完立刻 unref
  const ok = await writeLine(s, Buffer.from(JSON.stringify(entry) + '\n', 'utf8'))
  if (ok) {
    if (sock === s) s.unref()
    return true
  }
  logger.warning('[IPC] 发送失败，标记降级')
  if (sock === s) dropSocket()
  fileFallbackActive = true
  return false
}

// ─── 文件降级（仅在 IPC 不可用时使用）────────────────────────────────────

/** 父进程信道不可用时，追加到任务目录 pending_reports.jsonl 等启动恢复 */
function persistToFile(entry) {
  const workDir = envStr(E.ENV_WORK_DIR)
  if (!workDir) {
    logger.error(`[IPC] 无 ${E.ENV_WORK_DIR} 环境变量，事件丢失: ${JSON.stringify(entry)}`)
    return
  }
  const taskId = entry.task_id || ''
  if (!taskId) {
    logger.error(`[IPC] 事件无 task_id，丢失: ${JSON.stringify(entry)}`)
    return
  }
  const taskDir = path.join(workDir, taskId)
  try {
    fs.mkdirSync(taskDir, { recursive: true })
    fs.appendFileSync(path.join(taskDir, 'pending_reports.jsonl'), JSON.stringify(entry) + '\n', 'utf8')
  } catch (e) {
    logger.error(`[IPC] 文件降级写入失败 task_id=${taskId}: ${e.message}`)
  }
}

// ─── 对外 API ────────────────────────────────────────────────────────────

/**
 * 本子进程所属任务的 fencing 令牌（由 executor 注入 env）。取不到返回 null。
 * 与 resultClient 读同一对环境变量，只是这里要的是可 JSON 序列化的值（要随条目落盘）。
 * 令牌畸形 = 没有令牌（与服务端 fencing 的解析同口径：永不抛，取不到就是无令牌）。
 */
function fencingSnapshot() {
  const workerId = envStr(E.ENV_WORKER_ID)
  const rawSeq = envStr(E.ENV_LEASE_SEQ)
  if (!workerId || !/^-?\d+$/.test(rawSeq)) return null
  return { worker_id: workerId, lease_seq: parseInt(rawSeq, 10) }
}

/**
 * 子进程把汇报失败事件发给父进程。fire-and-forget：永不抛出、不 reject。
 * @param {string} action   'update_step_status' | 'update_task_fields'
 * @param {object} payload  HTTP body 内容
 * @param {{taskId?: string}} [opts]  缺省取 setTaskContext 设置的 taskId
 */
function sendReport(action, payload, { taskId } = {}) {
  const run = async () => {
    try {
      const tid = taskId == null ? CTX.taskId : taskId
      if (!tid) {
        logger.error(`[IPC] sendReport 未提供 task_id，事件丢失: action=${action}`)
        return
      }
      const entry = { task_id: tid, action, payload, context: snapshotContext(), ts: nowSec() }
      // 令牌快照随条目落盘：服务端 header / body 两条通道都认，重放侧走 body
      const fencing = fencingSnapshot()
      if (fencing) entry.fencing = fencing
      if (await sendViaSocket(entry)) return
      persistToFile(entry)
    } catch (e) {
      logger.error(`[IPC] sendReport 异常: ${e && e.message}`)
    }
  }
  chain = chain.then(run, run)
  return chain
}

/** 测试用：重置进程级状态（降级闩锁、缓存的 socket、任务上下文） */
function _resetForTest() {
  dropSocket()
  fileFallbackActive = false
  chain = Promise.resolve()
  setTaskContext('')
}

module.exports = { setTaskContext, updateTaskContext, sendReport, fencingSnapshot, _resetForTest }
