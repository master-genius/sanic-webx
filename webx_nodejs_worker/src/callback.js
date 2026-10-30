'use strict'

/**
 * 任务服务回调客户端 —— worker 向任务服务上报任务状态（路径与方法见 docs/worker-protocol.md §4）。
 *
 * ServerResult 区分三类服务端响应：
 *   · 200 正常 → isOk，data 有值；404 任务不存在 → isNotFound；网络故障 / 5xx → isError
 *
 * 汇报重试策略（按「重试是否可能改善」分档，而非按状态码大小）：
 *   · 200            成功
 *   · 400 / 404      业务拒绝，立即出局，不重试、不入队
 *   · 401 / 403      确定性拒绝，前台只试 1 次即入队（同一令牌重试 5 次必然 5 次都错，空转还占 Lane）；
 *                    入队后由 PendingReporter 永不放弃地重试，并由 linkHealth 发持续告警。
 *                    fencing 终态拒绝（响应体 reason ∈ not_lessee / gone / already_final）例外：
 *                    记闩锁，入队时直接丢弃——这类拒绝再试一万次也是同一个答案
 *   · 5xx / 网络错   前台 5 次退避重试（≈3s，覆盖反代闪断），仍失败则入队交后台
 *
 * fencing 头：所有 /internal/tasks/<id>/... 请求按 URL 里的 task_id 自动补 X-Worker-Id / X-Lease-Seq
 * （injectFencingHeaders；不覆盖调用方显式传入的头——重放旧条目时令牌来自条目快照，不能被当前活注册表顶替）。
 *
 * 与 Python 版的差异：
 *   · 无 httpx 客户端对象，每次请求现取配置（INTERNAL_TOKEN 等不再被烤进懒建客户端，改配置立即生效）；
 *     close() 保留为空操作以对齐调用方。请求一律经 httpClient（链路观测见 linkHealth.js）。
 *   · 新增公开方法 request(method, url, payload, {headers, timeout, signal}) 给 PendingReporter / 自检复用
 *     （对应 Python 里它们直接拿的 _get_client()）。
 *   · 重试间隔 / 次数挂在实例上（backoffBaseSec / maxAttempts / fieldsMaxAttempts / fieldsRetryDelaySec），便于测试压缩时间。
 *   · 关键字参数改位置参数，同时接受 options 对象：
 *       updateTaskFields(taskId, logPath, resultPath, mode)  或  updateTaskFields(taskId, {logPath, resultPath, mode})
 *       updateStepStatus(taskId, steps, subStep, subStatus, stepMessage)  或  updateStepStatus(taskId, {steps, subStep, ...})
 *       uploadResultFile(taskId, filename, content, timeout)  第四个参数可为数字（秒）或 {timeout}
 *       failTask(taskId, errorMessage)  第二个参数可为字符串或 {errorMessage}
 *     options 对象的判定：必须是非数组对象且键全部落在该方法的参数名内（log_path 在线路上是 JSON 字符串，不会冲突）。
 *   · 上传的 multipart 体自行拼装（content 来自内存而非文件路径）。
 */

const E = require('./envnames')
const { settings } = require('./config')
const fencing = require('./fencing')
const httpClient = require('./httpClient')
const aio = require('./aio')
const { getLogger } = require('./log')
require('./linkHealth') // 副作用：把链路观测器挂到 httpClient 上

const logger = getLogger('callback')

// 前台重试：覆盖反代闪断（总耗时 ≈ 3s）
const CALLBACK_MAX_ATTEMPTS = 5
const CALLBACK_BACKOFF_BASE_SEC = 0.2 // 0.2, 0.4, 0.8, 1.6, ...
const BUSINESS_REJECT_CODES = [400, 404]
// 确定性拒绝：前台只试 1 次。注意这与 linkHealth「是不是鉴权故障」是两个问题：
// 这里问「重试有没有意义」（401、403 都没有）；linkHealth 问「要不要发全局鉴权告警」（只有 401）。别合并。
const NO_RETRY_CODES = [401, 403]

// 心跳专用超时（秒）。心跳是续租载体，节拍封顶 29s；若超时也取 30s，一次失败就吃掉 88s，与租约时长贴得过近。
// 取 10s 而非 5s：5s 对偶发慢链路偏紧。两次成功续租的最坏间隔 = 29 + 39N（N=连续失败次数）。
// ⚠️ 改动此值必须同步核对服务端的租约时长。
const HEARTBEAT_TIMEOUT_SEC = 10.0
const DEFAULT_TIMEOUT_SEC = 30.0

// 字段更新（log_path / result_path）：首发 + 重试 1 次。它写的是可见性字段而非终态，且 PATCH /fields 严格 fencing，
// 一旦本轮结束重放必被判 not_lessee——入队只会堆僵尸、搞脏「队列长度即待送达数」这个运维信号。
const FIELDS_MAX_ATTEMPTS = 2
const FIELDS_RETRY_DELAY_SEC = 1.0

// 任务 ID 在内部端点 URL 里的位置：/internal/tasks/<task_id>/xxx（GET /internal/tasks/<id> 也要能取到）
const TASK_ID_IN_URL = /\/internal\/tasks\/([^/?]+)/

/**
 * 按 URL 自动给内部请求补上 fencing 头。做成统一出口而非逐调用点传参：请求出口十余个且还会新增，
 * 逐个传参必然漏掉将来新加的那个；task_id 本来就在 URL 里。
 * 未持有租约的任务不加任何头（按协议，服务端对写操作会明确拒绝，而不是悄悄放行）。
 * 调用方显式带的头（不分大小写）不覆盖。就地修改并返回 headers。
 */
function injectFencingHeaders(url, headers = {}) {
  const m = TASK_ID_IN_URL.exec(String(url))
  if (!m) return headers
  const have = new Set(Object.keys(headers).map((k) => k.toLowerCase()))
  for (const [k, v] of Object.entries(fencing.headersFor(m[1]))) {
    if (!have.has(k.toLowerCase())) headers[k] = v
  }
  return headers
}

/** 入队原因的人话描述——不让日志撒谎：鉴权失败只试了 1 次，不能写成「5 次重试失败」 */
function enqueueReason(res, lastExc, maxAttempts = CALLBACK_MAX_ATTEMPTS) {
  if (res && NO_RETRY_CODES.includes(res.status)) {
    return `确定性拒绝(HTTP ${res.status})，前台仅试 1 次——重试不会改善（401 另有链路告警，见上方）`
  }
  return `${maxAttempts} 次重试失败(last_exc=${lastExc})`
}

/** 服务端请求结果 —— 区分 404 / 网络故障 / 正常 */
class ServerResult {
  constructor({ data = null, isNotFound = false, isError = false } = {}) {
    this.data = data
    this.isNotFound = isNotFound
    this.isError = isError
  }

  /** 200 正常响应 */
  get isOk() { return !this.isNotFound && !this.isError }

  /** 心跳响应中的 status 字段 */
  get heartbeatStatus() { return this._status() }

  /** 任务状态查询中的 status 字段 */
  get taskStatus() { return this._status() }

  _status() {
    return this.data && typeof this.data === 'object' && this.data.status !== undefined ? this.data.status : null
  }
}

const isPlainObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v)
const asOptions = (v, allowed) => isPlainObj(v) && Object.keys(v).length > 0 && Object.keys(v).every((k) => allowed.includes(k))
const bodyHead = (res) => { try { return res.text().slice(0, 200) } catch (_) { return '' } }
const errText = (e) => (e && e.message ? `${e.name || 'Error'}: ${e.message}` : String(e))

/** 拼装 multipart/form-data（单文件 + 若干文本字段）；文件名中的引号 / 换行转义，防头注入 */
function buildMultipart(fields, file) {
  const boundary = `----webxworker${Date.now().toString(16)}${Math.random().toString(16).slice(2)}`
  const esc = (s) => String(s).replace(/"/g, '%22').replace(/\r/g, '%0D').replace(/\n/g, '%0A')
  const parts = []
  for (const [k, v] of Object.entries(fields)) {
    parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${esc(k)}"\r\n\r\n${v}\r\n`))
  }
  parts.push(Buffer.from(
    `--${boundary}\r\nContent-Disposition: form-data; name="${esc(file.field)}"; filename="${esc(file.filename)}"\r\n` +
    `Content-Type: ${file.contentType}\r\n\r\n`))
  parts.push(Buffer.isBuffer(file.content) ? file.content : Buffer.from(file.content))
  parts.push(Buffer.from(`\r\n--${boundary}--\r\n`))
  return { body: Buffer.concat(parts), contentType: `multipart/form-data; boundary=${boundary}` }
}

class WebCallbackClient {
  constructor() {
    // `${task_id}\0${lease_seq}` → fencing 拒绝原因。由 _noteFenceReject 写入，_enqueuePending 取出并消费（一次性）。
    // ⚠️ 键必须带 lease_seq：闩锁的语义是「这一轮的上报被永久拒了」，不是「这个任务被拒了」。
    // 只用 task_id 时，没人来取的陈旧闩锁（ackLease 那条路置位却不入队）会在下一轮被误消费，
    // 把本该进重试队列的合法上报静默吞掉。带上代次后，上一轮的键与这一轮物理上不可能相等。
    this._fenceRejected = new Map()
    // task_id → [ack 响应里的 lease_remaining_ms, 收到时的 monotonic(秒)]。供心跳注册时预置自熔基准
    this._ackInfo = new Map()
    this.maxAttempts = CALLBACK_MAX_ATTEMPTS
    this.backoffBaseSec = CALLBACK_BACKOFF_BASE_SEC
    this.fieldsMaxAttempts = FIELDS_MAX_ATTEMPTS
    this.fieldsRetryDelaySec = FIELDS_RETRY_DELAY_SEC
  }

  // 以下配置一律【访问期】现读 settings：模块级单例在 import 期构造，早于命令行覆盖，构造期拷贝会冻死覆盖。
  get baseUrl() { return settings.WEB_BASE_URL }

  /** 存储服务【对外】URL（落库 download_url 展示用）。不回退到任务服务地址——协议规定产物只传存储服务 */
  get storageUrl() { return settings.STORAGE_URL }

  /** 存储服务【内网】URL：实际上传 / 下载走此地址，未配置回退对外地址 */
  get storageInternalUrl() { return settings.STORAGE_INTERNAL_URL || this.storageUrl }

  get internalToken() { return settings.INTERNAL_TOKEN }

  /** 请求存储服务用的令牌：专属优先、回退主令牌（与子进程 SDK 同一条回退链） */
  get storageToken() { return settings.STORAGE_INTERNAL_TOKEN.trim() || this.internalToken }

  /** 无连接池需要关闭，保留以对齐调用方（broker 停机时调用） */
  async close() {}

  /**
   * 向任务服务发一次请求（带内部令牌 + 自动 fencing 头），不重试、不吞异常。
   * payload 为对象时以 JSON 发送；null 不带体。返回 gohttp 响应。
   */
  async request(method, url, payload = null, { headers = null, timeout = DEFAULT_TIMEOUT_SEC, signal } = {}) {
    const h = { [E.HEADER_INTERNAL_TOKEN]: this.internalToken, ...(headers || {}) }
    injectFencingHeaders(url, h)
    const opts = { headers: h, timeout }
    if (payload !== null && payload !== undefined) opts.body = payload
    if (signal) opts.signal = signal
    return httpClient.request(method, url, opts)
  }

  /**
   * 前台退避重试（method 由调用点按协议给出：POST / PUT / PATCH）。
   * 返回 {res, lastExc}：res 非 null = 200、业务拒绝(400/404)或鉴权/fencing 拒绝(401/403)；
   * res 为 null = 5xx / 网络错重试耗尽。
   * 401/403 也带 res 返回而不是当异常重试：确定性拒绝重试只是空转，且让调用点能区分「鉴权失败」与「重试耗尽」。
   */
  async _sendWithRetry(method, url, payload = null) {
    let lastExc = null
    for (let attempt = 0; attempt < this.maxAttempts; attempt++) {
      try {
        const res = await this.request(method, url, payload)
        if (res.status === 200 || BUSINESS_REJECT_CODES.includes(res.status)) return { res, lastExc: null }
        if (NO_RETRY_CODES.includes(res.status)) {
          this._noteFenceReject(url, res) // 只试 1 次即返回；告警已由 linkHealth 观测器统一发出
          return { res, lastExc: null }
        }
        lastExc = new Error(`HTTP ${res.status}`)
      } catch (e) {
        lastExc = e // 超时 / 连接失败 / TLS：都是传输层异常，重试
      }
      if (attempt < this.maxAttempts - 1) await aio.sleep(this.backoffBaseSec * (2 ** attempt))
    }
    return { res: null, lastExc }
  }

  /**
   * 发一个「终态一致性」类上报（status / complete / fail）：200 → true；业务拒绝 → false 不入队；
   * 其余入队交 PendingReporter 并返回 false。
   */
  async _report(method, url, taskId, action, payload, { rejectMsg, enqueueMsg }) {
    const { res, lastExc } = await this._sendWithRetry(method, url, payload)
    if (res && res.status === 200) return true
    if (res && BUSINESS_REJECT_CODES.includes(res.status)) {
      logger.error(`${rejectMsg}（不入队）: status=${res.status}, body=${bodyHead(res)}`)
      return false
    }
    // 已被 fencing 终态拒绝（闩锁在）的上报会被直接丢弃，此时再说「入队」就是撒谎，_noteFenceReject 已经记过日志
    if (!this._fenceRejected.has(this._fenceKey(taskId))) {
      logger.warning(`${enqueueMsg}: 原因=${enqueueReason(res, lastExc, this.maxAttempts)}`)
    }
    await this._enqueuePending(action, taskId, payload)
    return false
  }

  /** 更新任务状态 → PUT /internal/tasks/{id}/status */
  async updateStatus(taskId, status) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}/status`
    return this._report('PUT', url, taskId, 'update_status', { status }, {
      rejectMsg: `更新状态被业务拒绝: ${taskId} -> ${status}`,
      enqueueMsg: `更新状态未成功，入父进程队列: ${taskId} -> ${status}`,
    })
  }

  /**
   * 记下一次 fencing 终态拒绝，供 _enqueuePending 据此丢弃而非入队。
   * 为什么必须丢弃：PendingReporter「永不放弃」假设失败都是暂时性的；fencing 造出了一种永久性拒绝，
   * 当成暂时故障会永远重试，且混进永远送不出去的僵尸后「队列长度即系统状态」这个运维指标就废了。
   */
  _noteFenceReject(url, res) {
    let reason = null
    try { reason = fencing.isTerminalReject(res.json()) } catch (_) { reason = null }
    if (!reason) return
    const m = TASK_ID_IN_URL.exec(url)
    if (!m) return
    const taskId = m[1]
    this._fenceRejected.set(this._fenceKey(taskId), reason)
    if (reason === fencing.REASON_ALREADY_FINAL) {
      // 这不是错误：同一租户的重复上报（回调重投、超时后其实成功了）。记 error 会淹没真正的异常
      logger.info(`[fence] ${taskId} 已达终态（幂等成功），丢弃本次上报`)
    } else {
      logger.warning(`[fence] ${taskId} 上报被拒（${reason}），永久丢弃，不入队`)
    }
  }

  _fenceKey(taskId) { return `${taskId}\0${fencing.leaseSeqOf(taskId)}` }

  /**
   * 父进程内部入队。fencing 终态拒绝直接丢弃（见 _noteFenceReject）。
   * 若全局 PendingReporter 未启动，事件被记录但丢失（见 enqueueGlobal）。
   */
  async _enqueuePending(action, taskId, payload) {
    const key = this._fenceKey(taskId)
    if (this._fenceRejected.has(key)) {
      this._fenceRejected.delete(key)
      return
    }
    try {
      // fencing 令牌快照进条目，不能等重放时再查注册表：lease 的 finally 早已 unregister，
      // 跨重启从快照恢复的更是必然查不到——于是重放不带令牌、被判 not_lessee 403、落进保留集合永久重试。
      // 令牌的语义是「这条上报是哪一次运行产生的」：随数据走，不随进程走。
      const entry = { task_id: taskId, action, payload, context: {}, ts: Date.now() / 1000 }
      const seq = fencing.leaseSeqOf(taskId)
      if (seq !== null) entry.fencing = { worker_id: settings.WORKER_ID, lease_seq: seq }
      await require('./pendingReporter').enqueueGlobal(entry) // 延迟引入，避免循环依赖
    } catch (e) {
      logger.error(`[Callback] 入父进程队列失败 task_id=${taskId} action=${action}: ${errText(e)}`)
    }
  }

  /**
   * 上报心跳 → POST /internal/tasks/{id}/heartbeats。
   * 撤销（租约已被收走）时服务端回 200 {"ok": false, "status": "revoked"}（协议 §4.2），
   * 调用方据 status != "running" 取消子进程，无需额外分支。
   */
  async updateHeartbeat(taskId) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}/heartbeats`
    try {
      const res = await this.request('POST', url, {}, { timeout: HEARTBEAT_TIMEOUT_SEC })
      if (res.status === 200) return new ServerResult({ data: res.json() })
      if (res.status === 404) {
        logger.warning(`心跳 404：任务不存在 task_id=${taskId}`)
        return new ServerResult({ isNotFound: true })
      }
      logger.error(`心跳上报失败：${taskId}, 状态码：${res.status}`)
      return new ServerResult({ isError: true })
    } catch (e) {
      logger.error(`心跳上报异常：${taskId}, 错误：${errText(e)}`)
      return new ServerResult({ isError: true })
    }
  }

  /**
   * 领取后立刻发出的确认，把短租约续到常规时长（POST /internal/tasks/<id>/ack）。
   * 短租约只需覆盖一个网络往返，启动多慢（环境激活、拉起子进程、下载输入）都不再有关系；
   * 同时确认 worker 真的接住了这个任务。
   */
  async ackLease(taskId) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}/ack`
    const { res, lastExc } = await this._sendWithRetry('POST', url, {})
    if (res && res.status === 200) {
      try {
        const remaining = res.json().lease_remaining_ms
        if (typeof remaining === 'number' && Number.isFinite(remaining)) {
          this._ackInfo.set(taskId, [Math.trunc(remaining), aio.monotonic()])
        }
      } catch (_) { /* 响应体不是 JSON：没有剩余时长可记 */ }
      return true
    }
    // ⚠️ 本方法是唯一「会置 fencing 闩锁却不入队」的出口：403 时 _sendWithRetry 已记下闩锁，这里直接返回，
    // 没有 _enqueuePending 来消费它。键带代次已堵掉误伤，这里再清一次是为了不留慢性泄漏。
    this._fenceRejected.delete(this._fenceKey(taskId))
    // ack 失败不入队、不重试到底：任务会在短租约到期后自动回池，比抱着一个可能已被回收的任务硬跑安全
    logger.warning(`[lease] ack 未成功 task_id=${taskId}，原因=${enqueueReason(res, lastExc, this.maxAttempts)}（任务将在短租约到期后回池）`)
    return false
  }

  /**
   * 取走 ack 时服务端告知的剩余租约（毫秒，已扣除自收到起流逝的时间）；没有则 null。
   * 用来给心跳预置自熔基准：否则「ack 成功后一次心跳也没成功过」时没有 deadline，自熔永远不启动。
   */
  popAckLeaseRemainingMs(taskId) {
    const info = this._ackInfo.get(taskId)
    if (info === undefined) return null
    this._ackInfo.delete(taskId)
    const [remainingMs, at] = info
    return Math.trunc(remainingMs - (aio.monotonic() - at) * 1000)
  }

  /**
   * 主动归还租约 → DELETE /internal/tasks/{id}/lease，任务立刻回池，不必等租约自然到期。
   * 场景：优雅退出 / drain；重启恢复（recovery 扫到没有 ok.json 即知上一轮没跑完）。
   * 被拒（403）是正常结果：租约早已到期、任务被别人接管了，什么都不做才对，故不重试不入队。
   *
   * extraHeaders：显式传入 fencing 令牌，供停机路径用——停机时要先同步取消在跑任务，它们的 finally 会清空注册表，
   * 等请求真正构造时已取不到令牌，归还会被服务端判 missing_fencing_token。故先同步捕获再显式传入
   * （injectFencingHeaders 不覆盖显式头，两者可共存）。DELETE 不带请求体，令牌只走请求头。
   */
  async releaseLease(taskId, extraHeaders = null) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}/lease`
    try {
      const res = await this.request('DELETE', url, null, { timeout: 10.0, headers: extraHeaders })
      if (res.status === 200) {
        logger.info(`[lease] 已归还租约 task_id=${taskId}`)
        return true
      }
      logger.info(`[lease] 归还未生效 task_id=${taskId} status=${res.status}（多半已被接管，属正常）`)
    } catch (e) {
      // 归还失败只是慢一点：任务会在租约到期后自动回池，不值得重试
      logger.warning(`[lease] 归还请求失败 task_id=${taskId}: ${errText(e)}（将由租约到期兜底）`)
    }
    return false
  }

  /** 完成任务（仅触发状态迁移）→ POST /internal/tasks/{id}/complete；重试失败入队 */
  async completeTask(taskId) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}/complete`
    return this._report('POST', url, taskId, 'complete_task', {}, {
      rejectMsg: `完成任务被业务拒绝: ${taskId}`,
      enqueueMsg: `完成任务未成功，入父进程队列: ${taskId}`,
    })
  }

  /**
   * 失败任务（只报错误信息，不传 log_path）→ POST /internal/tasks/{id}/fail。
   * 重试失败入队；PendingReporter 重放前会做 server 状态前置校验（避免覆盖补偿器重投后的工作），
   * 且停机快照不持久化 fail_task 条目。
   */
  async failTask(taskId, errorMessage) {
    if (asOptions(errorMessage, ['errorMessage'])) errorMessage = errorMessage.errorMessage // 也接受 {errorMessage}
    const url = `${this.baseUrl}/internal/tasks/${taskId}/fail`
    const payload = { error_message: errorMessage === null || errorMessage === undefined ? '' : errorMessage }
    return this._report('POST', url, taskId, 'fail_task', payload, {
      rejectMsg: `失败任务被业务拒绝: ${taskId}`,
      enqueueMsg: `失败任务未成功，入父进程队列（带前置校验）: ${taskId}`,
    })
  }

  /** 查询任务状态信息 → GET /internal/tasks/{id}（协议 §4.10） */
  async getTaskStatus(taskId) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}`
    try {
      const res = await this.request('GET', url)
      if (res.status === 200) {
        let body = res.json()
        // 协议 §4.10 没写清响应是否包在 data 里 —— 两种都兼容，见 docs/worker.md 的协议缺口
        if (isPlainObj(body) && isPlainObj(body.data)) body = body.data
        return new ServerResult({ data: isPlainObj(body) ? body : null })
      }
      if (res.status === 404) {
        logger.warning(`查询任务状态 404：任务不存在 task_id=${taskId}`)
        return new ServerResult({ isNotFound: true })
      }
      logger.warning(`查询任务状态失败：${taskId}, 状态码：${res.status}`)
      return new ServerResult({ isError: true })
    } catch (e) {
      logger.error(`查询任务状态异常：${taskId}, 错误：${errText(e)}`)
      return new ServerResult({ isError: true })
    }
  }

  /** 更新步骤进度 → PATCH /internal/tasks/{task_id}/steps（失败只记日志，不入队） */
  async updateStepStatus(taskId, steps = null, subStep = null, subStatus = null, stepMessage = null) {
    if (asOptions(steps, ['steps', 'subStep', 'subStatus', 'stepMessage'])) {
      ({ steps = null, subStep = null, subStatus = null, stepMessage = null } = steps)
    }
    const url = `${this.baseUrl}/internal/tasks/${taskId}/steps`
    const payload = {}
    if (steps !== null && steps !== undefined) payload.steps = steps
    if (subStep !== null && subStep !== undefined) payload.sub_step = subStep
    if (subStatus !== null && subStatus !== undefined) payload.sub_status = subStatus
    if (stepMessage !== null && stepMessage !== undefined) payload.step_message = stepMessage
    try {
      const res = await this.request('PATCH', url, payload)
      if (res.status === 200) return true
      logger.error(`步骤进度更新失败：${taskId}, 状态码：${res.status}`)
      return false
    } catch (e) {
      logger.error(`步骤进度更新异常：${taskId}, 错误：${errText(e)}`)
      return false
    }
  }

  /**
   * 上传结果文件到存储服务，返回下载 URL（失败返回 null）。
   * POST {存储服务}/internal/tasks/<task_id>/results（multipart，创建成功 201），产物不经过任务服务（协议 §5）。
   * 存储服务只认令牌、不做 fencing，故不注入 fencing 头。
   * timeout（秒）：终态产物用默认 60；心跳增量日志流式上传传更短的 35，避免单次上传长时间占住「在途」槽。
   */
  async uploadResultFile(taskId, filename, content, timeout = 60.0) {
    if (isPlainObj(timeout)) timeout = timeout.timeout === undefined ? 60.0 : timeout.timeout
    const base = this.storageInternalUrl.trim()
    if (!base) {
      logger.error(`上传文件失败：未配置存储服务地址（${E.ENV_STORAGE_INTERNAL_URL} / ${E.ENV_STORAGE_URL}）：${taskId}/${filename}`)
      return null
    }
    const url = `${base.replace(/\/+$/, '')}/internal/tasks/${taskId}/results`
    try {
      const { body, contentType } = buildMultipart({ filename }, { field: 'file', filename, contentType: 'text/plain', content })
      const res = await httpClient.request('POST', url, {
        headers: {
          [E.HEADER_INTERNAL_TOKEN]: this.storageToken,
          'content-type': contentType,
          'content-length': body.length,
        },
        rawBody: body,
        timeout,
      })
      // 存储服务创建成功回 201；兼容 200（兼容别名 / 网关改写）
      if (res.status === 200 || res.status === 201) return this._publicDownloadUrl(res.json().download_url)
      logger.error(`上传文件失败：${taskId}/${filename}, 状态码：${res.status}`)
      return null
    } catch (e) {
      logger.error(`上传文件异常：${taskId}/${filename}, 错误：${errText(e)}`)
      return null
    }
  }

  /** 存储端回传相对路径 → 落库用的【对外】URL；未配置对外地址则保持相对路径（由前端按页面 origin 解析） */
  _publicDownloadUrl(rel) {
    if (!rel || /^https?:\/\//.test(rel)) return rel || null
    const pub = settings.STORAGE_URL.trim().replace(/\/+$/, '')
    return pub ? `${pub}/${rel.replace(/^\/+/, '')}` : rel
  }

  /**
   * 更新任务字段（log_path / result_path），支持 merge 模式 → PATCH /internal/tasks/{id}/fields[?mode=merge]。
   * 失败只重试 1 次，不入 PendingReporter（理由见 FIELDS_MAX_ATTEMPTS）。
   * 200 / 业务拒绝(400,404) / 确定性拒绝(401,403) 一律立即返回，只有 5xx 与网络错才再试一次。
   */
  async updateTaskFields(taskId, logPath = null, resultPath = null, mode = 'merge') {
    if (asOptions(logPath, ['logPath', 'resultPath', 'mode'])) {
      ({ logPath = null, resultPath = null, mode = 'merge' } = logPath)
    }
    let url = `${this.baseUrl}/internal/tasks/${taskId}/fields`
    if (mode === 'merge') url += '?mode=merge'
    const payload = {}
    if (logPath !== null && logPath !== undefined) payload.log_path = logPath
    if (resultPath !== null && resultPath !== undefined) payload.result_path = resultPath
    if (Object.keys(payload).length === 0) return true

    let last = ''
    for (let attempt = 0; attempt < this.fieldsMaxAttempts; attempt++) {
      try {
        const res = await this.request('PATCH', url, payload)
        if (res.status === 200) return true
        if (BUSINESS_REJECT_CODES.includes(res.status) || NO_RETRY_CODES.includes(res.status)) {
          // 业务拒绝 / 确定性拒绝：再试一次也是同一个答案
          logger.error(`更新字段被拒（不重试）：${taskId}, 状态码：${res.status}`)
          return false
        }
        last = `HTTP ${res.status}`
      } catch (e) {
        last = errText(e) // 网络错，值得再试一次
      }
      if (attempt < this.fieldsMaxAttempts - 1) await aio.sleep(this.fieldsRetryDelaySec)
    }
    // 说清「已经重试过了」，否则排障时会误以为只发了一次、把注意力引向偶发抖动
    logger.error(`更新字段失败（已重试 ${this.fieldsMaxAttempts - 1} 次，不入队）：${taskId}, last=${last}`)
    return false
  }
}

// 全局单例
const callbackClient = new WebCallbackClient()

module.exports = {
  WebCallbackClient, ServerResult, callbackClient, injectFencingHeaders, enqueueReason,
  CALLBACK_MAX_ATTEMPTS, CALLBACK_BACKOFF_BASE_SEC, BUSINESS_REJECT_CODES, NO_RETRY_CODES,
  HEARTBEAT_TIMEOUT_SEC, FIELDS_MAX_ATTEMPTS, FIELDS_RETRY_DELAY_SEC,
}
