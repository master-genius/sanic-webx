'use strict'

/**
 * PendingReporter —— 待汇报事件的后台轮询消费者。
 *
 * 设计语义：调用点已经做了前台重试，仍失败的事件进入本模块的内存队列，由 consumer 异步单次重试，
 * 永不放弃：队列长度即系统状态。
 *   · 新事件优先、立刻试一次，失败入 retryPool；POLL_INTERVAL_SEC 内无新事件则扫一遍 retryPool（每条只试 1 次）
 *   · 启动：restoreFromSnapshot() 灌回上次退出时的快照 + 各任务目录里子进程 IPC 降级写的 pending_reports.jsonl
 *   · 退出：snapshotToFile() 把队列 + retryPool 落盘（fail_task 条目不持久化，避免重启后覆盖补偿器重投后的工作）
 *
 * 条目（entry，线路格式与 Python 版 / 子进程 SDK 一致，字段保持 snake_case）：
 *   {task_id, action, payload, context, ts, fencing?: {worker_id, lease_seq}}
 *   action ∈ update_step_status / update_task_fields / update_status / complete_task / fail_task
 *
 * 状态码分类（_tryReport）：
 *   200 → 成功出队；404/400 → 业务拒绝，丢弃；
 *   403 且响应体 reason ∈ {not_lessee, gone, already_final} → fencing 终态拒绝（判据是响应体 reason 而非状态码），丢弃；
 *   401 / 其它 403 → 确定性拒绝但不可丢（令牌填错，修好即自愈），保留；5xx / 网络错 → 保留。
 *   fail_task 重放前先 GET 任务，server 状态已非 running（被补偿器重投或别处接走）则跳过，避免覆盖正在执行的工作。
 *
 * 与 Python 版的差异：
 *   · 无 asyncio.cancel：停机用 requestStop()（置标志、唤醒循环、abort 在途请求），runLoop() 的 Promise 随后返回；
 *     应用须 await runLoop 的 Promise 之后再 snapshotToFile()。被 abort 的在途条目回到 retryPool，随快照落盘（宁可多送一次也不丢）。
 *   · runLoop 内单条处理抛出的意外异常只记日志并把条目放回 retryPool，不再让 consumer 整个退出。
 *   · 修复 K7：restoreFromSnapshot 即使没有快照文件，也会扫各任务目录的降级文件。
 *   · 修复 K8：重放不再就地修改条目（_mode 取自拷贝），merge 模式可多次重放保持不变。
 *   · 快照用「写临时文件 + rename」，避免半截文件。
 *   · 队列不用 aio.Queue：它的 get() 无法在超时后撤销，会吞掉下一个事件；这里用数组 + 单一唤醒函数。
 */

const fs = require('node:fs')
const path = require('node:path')
const E = require('./envnames')
const fencing = require('./fencing')
const { getLogger } = require('./log')

const logger = getLogger('pending_reporter')

const POLL_INTERVAL_SEC = 20 // retryPool 轮询间隔
const SNAPSHOT_FILENAME = 'pending_reports.snapshot.jsonl'
const TASK_PENDING_FILENAME = 'pending_reports.jsonl' // 子进程 IPC 不通时的降级文件

// 业务规则拒绝（不重试）
const BUSINESS_REJECT_CODES = [400, 404]
// 确定性拒绝但不可丢：保留在池里等人工处理后自愈（401 = 令牌填错；403 不带终态 reason 时如 missing_fencing_token 也保守保留）
const AUTH_RETAIN_CODES = [401, 403]

const ACTION_UPDATE_STEP_STATUS = 'update_step_status'
const ACTION_UPDATE_TASK_FIELDS = 'update_task_fields'
const ACTION_UPDATE_STATUS = 'update_status'
const ACTION_COMPLETE_TASK = 'complete_task'
const ACTION_FAIL_TASK = 'fail_task'

/**
 * 响应是否为 fencing 终态拒绝，是则返回 reason，否则 null。
 * 判据是响应体的 reason 而非 403 状态码：按状态码归类会让将来任何别的 403 都被当成 fencing 丢掉——
 * 那正是 AUTH_RETAIN_CODES 存在的理由（一次配置错误清空整个队列）。解析不出来一律 null（保守保留）。
 */
function fenceReason(res) {
  if (res.status !== 403) return null
  let body
  try { body = res.json() } catch (_) { return null }
  return fencing.isTerminalReject(body)
}

/** 读 jsonl 文本为条目数组；坏行告警跳过 */
function parseJsonl(text, onBad) {
  const out = []
  for (const raw of String(text).split(/\r?\n/)) {
    const line = raw.trim()
    if (!line) continue
    try {
      const entry = JSON.parse(line)
      if (entry && typeof entry === 'object' && !Array.isArray(entry)) out.push(entry)
      else onBad(new Error('不是 JSON 对象'))
    } catch (e) { onBad(e) }
  }
  return out
}

class PendingReporter {
  /**
   * @param callback    WebCallbackClient（需有 baseUrl / request() / getTaskStatus()）
   * @param workBaseDir 工作目录（快照与任务级降级文件所在）
   * @param opts        {pollIntervalSec} 仅测试压缩时间用
   */
  constructor(callback, workBaseDir, { pollIntervalSec = POLL_INTERVAL_SEC } = {}) {
    // 也接受单个 options 对象 {callback, workBaseDir, pollIntervalSec}
    if (workBaseDir === undefined && callback && callback.workBaseDir !== undefined) {
      ({ callback, workBaseDir, pollIntervalSec = POLL_INTERVAL_SEC } = callback)
    }
    this.callback = callback
    this.workBaseDir = path.resolve(String(workBaseDir))
    this.pollIntervalSec = pollIntervalSec
    this._queue = []
    this._retryPool = []
    this._stopped = false
    this._wakeFn = null
    this._abort = new AbortController()
  }

  // ─── 入队 ─────────────────────────────────────────────────────

  /** 父进程内部调用（callback 失败时直接入队，不走 IPC） */
  async enqueue(entry) { this.enqueueNowait(entry) }

  /** IPC server 回调用（同步） */
  enqueueNowait(entry) {
    this._queue.push(entry)
    this._wake()
  }

  /** 队列 + 重试池的待送达总数（运维指标） */
  get pendingCount() { return this._queue.length + this._retryPool.length }

  _wake() {
    const fn = this._wakeFn
    if (fn) fn()
  }

  /** 等待 seconds 秒或被唤醒（定时器 unref，不拖住进程退出） */
  _wait(seconds) {
    return new Promise((resolve) => {
      const timer = setTimeout(done, Math.max(0, seconds) * 1000)
      timer.unref()
      this._wakeFn = done
      function done() { clearTimeout(timer); resolve() }
    }).finally(() => { this._wakeFn = null })
  }

  // ─── consumer ─────────────────────────────────────────────────

  /** 启动 consumer：返回的 Promise 在 requestStop() 之后 resolve */
  async runLoop() {
    logger.info(`[Reporter] 启动，轮询间隔 ${this.pollIntervalSec}s`)
    let deadline = Date.now() + this.pollIntervalSec * 1000
    while (!this._stopped) {
      if (this._queue.length) {
        await this._handleEntry(this._queue.shift()) // 新事件立刻试一次
        deadline = Date.now() + this.pollIntervalSec * 1000 // 刚失败的条目不在同一瞬间被扫描再试
        continue
      }
      const remain = deadline - Date.now()
      if (remain <= 0) { // 一个轮询间隔内无新事件：扫 retryPool
        deadline = Date.now() + this.pollIntervalSec * 1000
        try { await this._sweepRetryPool() } catch (e) { logger.error('[Reporter] 扫描 retryPool 异常', e) }
        continue
      }
      await this._wait(remain / 1000)
    }
    logger.info('[Reporter] consumer 已退出')
  }

  /** 处理单个 entry：试一次，分类结果 */
  async _handleEntry(entry) {
    let outcome
    try {
      outcome = await this._tryReport(entry)
    } catch (e) { // 意外异常：条目绝不能丢
      logger.error(`[Reporter] 处理条目异常，放回重试池: ${e && e.message}`, e)
      outcome = 'retry'
    }
    if (outcome === 'retry') this._retryPool.push(entry)
  }

  /** 扫一遍 retryPool，每条试 1 次；停机时没处理完的条目放回池子（含被 abort 的那条） */
  async _sweepRetryPool() {
    if (!this._retryPool.length) return
    const pool = this._retryPool
    this._retryPool = []
    logger.debug(`[Reporter] 轮询 retryPool: ${pool.length} 条`)
    const remaining = []
    let done = 0
    try {
      for (const entry of pool) {
        if (this._stopped) break
        const outcome = await this._tryReport(entry).catch((e) => {
          logger.error(`[Reporter] 处理条目异常，保留: ${e && e.message}`, e)
          return 'retry'
        })
        done += 1
        if (outcome === 'retry') remaining.push(entry)
      }
    } finally {
      remaining.push(...pool.slice(done))
      this._retryPool = remaining.concat(this._retryPool)
    }
  }

  // ─── 单次重试（核心） ─────────────────────────────────────────

  /** 尝试上报一次，返回 'success'（200，出队）/ 'drop'（拒绝，出队）/ 'retry'（保留） */
  async _tryReport(entry) {
    const action = entry.action || ''
    const taskId = entry.task_id || ''
    const payload = entry.payload || {}

    if (action === ACTION_FAIL_TASK && !(await this._safeToFail(taskId))) {
      logger.warning(`[Reporter] fail_task 跳过 task_id=${taskId}：server 状态已变`)
      return 'drop'
    }

    let status
    let reason
    try {
      [status, reason] = await this._doPost(action, taskId, payload, entry.fencing)
    } catch (e) {
      logger.warning(`[Reporter] ${action} task_id=${taskId} 异常: ${e && e.message}`)
      return 'retry'
    }

    if (status === 200) {
      logger.info(`[Reporter] ${action} task_id=${taskId} 成功`)
      return 'success'
    }
    if (BUSINESS_REJECT_CODES.includes(status)) {
      logger.warning(`[Reporter] ${action} task_id=${taskId} 被业务拒绝(${status})，丢弃`)
      return 'drop'
    }
    if (reason) {
      // fencing 终态拒绝 = 这一轮已作废（被回收 / 用户 retry / 已推进到下一步）或同一租户的重复上报已达终态（幂等成功）。
      // 两种都是「再试一万次也是同一个答案」，必须出队；与「401/403 一律保留」不冲突——那条防的是配置错误，
      // 这条是数据结论，判据是响应体 reason，二者不会互相误伤。
      logger.info(`[Reporter] ${action} task_id=${taskId} 被 fencing 拒绝（${reason}），丢弃`)
      return 'drop'
    }
    if (AUTH_RETAIN_CODES.includes(status)) {
      // 只记 DEBUG：linkHealth 已发聚合 ERROR，这里逐条 ERROR 会让积压 N 条时每 20s 刷 N 行同因日志，淹没真正的告警
      logger.debug(`[Reporter] ${action} task_id=${taskId} 鉴权错误(${status})，保留待人工（聚合告警见 linkHealth）`)
      return 'retry'
    }
    logger.warning(`[Reporter] ${action} task_id=${taskId} 失败(${status})，下轮再试`)
    return 'retry'
  }

  /**
   * 按 action 调用任务服务，返回 [状态码, fencing 拒绝原因]；后者非 null 即「这一轮已作废，该丢」。
   * 单返回状态码不够：403 有两种含义（配置错 → 保留 / fencing 拒绝 → 丢弃），只有响应体 reason 分得开。
   *
   * ⚠️ fencing 必须由条目提供，不能靠 callback 自动注入的头：那读的是活注册表，而重放恒发生在 Lane 结束、
   * 令牌已被 unregister 之后（跨重启更是从盘上恢复的）。令牌同时放 body 与请求头：callback 不会覆盖显式头，
   * 否则同一台 worker 重新领到该任务后，旧条目会带着新一轮的令牌通过校验。
   */
  async _doPost(action, taskId, payload, fenceTok = null) {
    const base = this.callback.baseUrl
    let body = { ...payload } // 拷贝：_mode 取自拷贝，条目本身不被修改（修复 K8）
    const headers = {}
    if (fenceTok) {
      body = { ...body, ...fenceTok }
      if (fenceTok.worker_id && fenceTok.lease_seq !== undefined && fenceTok.lease_seq !== null) {
        headers[E.HEADER_WORKER_ID] = String(fenceTok.worker_id)
        headers[E.HEADER_LEASE_SEQ] = String(fenceTok.lease_seq)
      }
    }
    const send = (method, url, data) => this.callback.request(method, url, data, { headers, signal: this._abort.signal })

    // action → (方法, 路径)：与 docs/worker-protocol.md §4 一致
    let res
    if (action === ACTION_UPDATE_STEP_STATUS) {
      res = await send('PATCH', `${base}/internal/tasks/${taskId}/steps`, body)
    } else if (action === ACTION_UPDATE_TASK_FIELDS) {
      const mode = body._mode || 'overwrite'
      delete body._mode
      res = await send('PATCH', `${base}/internal/tasks/${taskId}/fields${mode === 'merge' ? '?mode=merge' : ''}`, body)
    } else if (action === ACTION_UPDATE_STATUS) {
      res = await send('PUT', `${base}/internal/tasks/${taskId}/status`, body)
    } else if (action === ACTION_COMPLETE_TASK) {
      res = await send('POST', `${base}/internal/tasks/${taskId}/complete`, body)
    } else if (action === ACTION_FAIL_TASK) {
      res = await send('POST', `${base}/internal/tasks/${taskId}/fail`, body)
    } else {
      logger.error(`[Reporter] 未知 action: ${action}, 丢弃`)
      return [400, null] // 当作业务拒绝丢弃
    }
    return [res.status, fenceReason(res)]
  }

  /**
   * fail_task 前置校验：仅 server 当前 status=running 时允许重放。
   * 任务不存在 / 读不到状态 / 非 running 一律不放行（读不到 → 丢弃，由 Recovery / 补偿器兜底）。
   */
  async _safeToFail(taskId) {
    const result = await this.callback.getTaskStatus(taskId)
    if (result.isNotFound || result.isError || !result.isOk) return false
    return result.taskStatus === 'running'
  }

  // ─── 启动恢复 / 退出快照 ─────────────────────────────────────

  /** worker 启动时把上次的快照 + 各任务目录的降级文件灌回队列，返回条数 */
  async restoreFromSnapshot() {
    let count = 0
    const snapshotFile = path.join(this.workBaseDir, SNAPSHOT_FILENAME)
    if (fs.existsSync(snapshotFile)) {
      try {
        const entries = parseJsonl(fs.readFileSync(snapshotFile, 'utf8'), (e) => logger.warning(`[Reporter] snapshot 行解析失败: ${e.message}`))
        for (const entry of entries) this._queue.push(entry)
        count += entries.length
        fs.rmSync(snapshotFile, { force: true })
        if (entries.length) logger.info(`[Reporter] 已恢复 ${entries.length} 条 snapshot 事件`)
      } catch (e) {
        logger.error(`[Reporter] snapshot 读取失败: ${e.message}`)
      }
    }
    // 无论有没有快照都要扫降级文件（修复 K7）
    count += this._restorePerTaskFiles()
    if (count) this._wake()
    return count
  }

  /** 扫 {WORK_DIR}/{task_id}/pending_reports.jsonl，灌回队列后删除 */
  _restorePerTaskFiles() {
    let count = 0
    let dirents
    try {
      dirents = fs.readdirSync(this.workBaseDir, { withFileTypes: true })
    } catch (e) {
      if (e.code !== 'ENOENT') logger.error(`[Reporter] 扫描任务目录失败: ${e.message}`)
      return 0
    }
    for (const d of dirents) {
      if (!d.isDirectory()) continue
      const file = path.join(this.workBaseDir, d.name, TASK_PENDING_FILENAME)
      let text
      try { text = fs.readFileSync(file, 'utf8') } catch (e) {
        if (e.code !== 'ENOENT') logger.error(`[Reporter] 任务级 pending 文件读取失败 ${file}: ${e.message}`)
        continue
      }
      const entries = parseJsonl(text, () => {})
      for (const entry of entries) this._queue.push(entry)
      count += entries.length
      try { fs.rmSync(file, { force: true }) } catch (e) { logger.error(`[Reporter] 删除任务级 pending 文件失败 ${file}: ${e.message}`) }
    }
    if (count) logger.info(`[Reporter] 已恢复 ${count} 条任务级降级事件`)
    return count
  }

  /** worker 退出时把队列 + retryPool 落盘，返回持久化的条数；排除 fail_task（由 Recovery / 补偿器兜底） */
  snapshotToFile() {
    const entries = [...this._queue, ...this._retryPool]
    this._queue = []
    this._retryPool = []

    const persistable = entries.filter((e) => e.action !== ACTION_FAIL_TASK)
    const droppedFail = entries.length - persistable.length
    if (droppedFail) {
      logger.warning(`[Reporter] snapshot 跳过 ${droppedFail} 条 fail_task entry （由 Recovery / 补偿器兜底）`)
    }
    if (!persistable.length) {
      logger.info('[Reporter] snapshot 无内容')
      return 0
    }
    const snapshotFile = path.join(this.workBaseDir, SNAPSHOT_FILENAME)
    const tmp = `${snapshotFile}.tmp`
    try {
      fs.mkdirSync(this.workBaseDir, { recursive: true })
      fs.writeFileSync(tmp, persistable.map((e) => JSON.stringify(e) + '\n').join(''), 'utf8')
      fs.renameSync(tmp, snapshotFile)
      logger.info(`[Reporter] snapshot 已写入: ${persistable.length} 条 -> ${snapshotFile}`)
    } catch (e) {
      logger.error(`[Reporter] snapshot 写入失败: ${e.message}`)
    }
    return persistable.length
  }

  // ─── 停止 ─────────────────────────────────────────────────────

  /** 请求 consumer 退出：置标志、唤醒循环、abort 在途请求（在途条目回到 retryPool） */
  requestStop() {
    this._stopped = true
    this._abort.abort()
    this._wake()
  }
}

// ─── 全局单例（由应用启动时填充）──────────────────────────────

let globalReporter = null

/** worker 主入口启动时注入全局实例 */
function setGlobalReporter(reporter) { globalReporter = reporter }

/** callback 等模块通过此函数访问全局实例（运行时绑定，避免 require 顺序问题） */
function getGlobalReporter() { return globalReporter }

/** 模块级入队入口：callback 失败时调用 */
async function enqueueGlobal(entry) {
  if (globalReporter === null) {
    logger.error(`[Reporter] 全局实例未初始化，事件丢失: ${entry.task_id}/${entry.action}`)
    return
  }
  await globalReporter.enqueue(entry)
}

module.exports = {
  PendingReporter, setGlobalReporter, getGlobalReporter, enqueueGlobal,
  POLL_INTERVAL_SEC, SNAPSHOT_FILENAME, TASK_PENDING_FILENAME,
  ACTION_UPDATE_STEP_STATUS, ACTION_UPDATE_TASK_FIELDS, ACTION_UPDATE_STATUS, ACTION_COMPLETE_TASK, ACTION_FAIL_TASK,
}
