'use strict'

/**
 * lease 分发 —— worker 主动来领（`POST /task/leases`，docs/worker-protocol.md §3.1）。
 *
 * 要点（设计理由见 docs/worker.md §1/§4/§8，这里只留关键几句）：
 *   · Lane 是槽位不是队列：不预取，一条 Lane 同时最多一个任务；没空槽就不发请求 = 背压。
 *   · 一个 worker 一条请求（slots 里每个空闲 Lane 一个元素），回包按 task_type 查 Lane，**不能按下标**。
 *   · 断线重连是基本性质：挂起中的请求尚未认领任务，掐断零损失。
 *   · 停机三步（K5）：beginDrain()（同步停领）→ heldFencingHeaders()（同步捕获令牌）
 *     → cancelRunningTasks()（同步 abort）→ releaseHeldLeases(tokens)（并发归还）。
 *
 * 与 Python 版的差异：
 *   · Python 的 Task.cancel() 换成 AbortController：每个在跑任务一个，handler/executor 签名为
 *     `executor.execute(taskId, payload, signal)`；被 abort 时 executor 抛 aio.Interrupted，这里只记 warning。
 *   · 领取长连接可被取消：acquire(slots, signal) 收到 abort 返回 null；dispatcher.stop() 会 abort 在途请求。
 *   · K22：空领取（200 空列表）不再无条件立即重连，两次请求之间至少间隔 MIN_EMPTY_INTERVAL_SEC。
 *   · 停机后（stop）才回包的认领，同样原样归还而不是丢掉。
 *   · 回调客户端 callback.js 懒加载；releaseLease 的第二个参数就是 extra_headers（位置参数）。
 *   · 传给 executor 的 payload 键保持 snake_case（与 Python 版同名，executor.js 就是这么读的）。
 */

const E = require('./envnames')
const { settings, leaseFilters } = require('./config')
const fencing = require('./fencing')
const httpClient = require('./httpClient')
const { monotonic, Event, Interrupted } = require('./aio')
const { getLogger } = require('./log')

const logger = getLogger('lease')

// 退避：正常返回立即重连（心跳式的常态，不是错误）；只有真出错才退避。抖动不可省——
// 领取服务重启后所有 worker 同时重连会惊群。
const BACKOFF_BASE_SEC = 1.0
const BACKOFF_MAX_SEC = 30.0
const JITTER_RATIO = 0.3

/** 可调参数（测试里压短；生产不要动） */
const tuning = {
  // ack / running 上报失败后本 Lane 的冷却（秒）：不冷却会「认领→失败→放弃→立刻再认领」空转啃池子
  ACK_FAIL_COOLDOWN_SEC: 5.0,
  // 没有任何可领类型时的空转间隔（秒）
  NO_SLOT_SLEEP_SEC: 5.0,
  // K22：空领取后两次请求的最小间隔（秒），从发起请求时刻起算
  MIN_EMPTY_INTERVAL_SEC: 1.0,
  // 上报 running 的重试间隔（秒）
  RUNNING_RETRY_SEC: 1.0,
}

// 401/403 = 确定性拒绝（token 不对），重试无意义，等运维介入
const NO_RETRY_CODES = new Set([401, 403])

// ─────────────────────────── 模块级状态（停机路径拿不到 dispatcher 实例） ───────────────────────────

/** 在跑的任务：task_id -> {controller: AbortController} */
const runningTasks = new Map()

/** 停机排空标志：置位后领取循环不再发请求，且把已领到手的新任务原样还回去 */
let draining = false

/** 活着的 dispatcher（beginDrain 要唤醒等空槽的循环） */
const dispatchers = new Set()

const deps = { callback: null }

/** 测试注入：替换回调客户端（需实现 ackLease / updateStatus / releaseLease） */
function setCallbackClient(c) { deps.callback = c }

function getCallback() {
  if (deps.callback) return deps.callback
  return require('./callback').callbackClient
}

/** 可被 AbortSignal 打断的 sleep（秒）；abort 时提前 resolve（不抛） */
function sleepAbortable(seconds, signal) {
  return new Promise((resolve) => {
    if (signal && signal.aborted) return resolve()
    const done = () => { clearTimeout(timer); if (signal) signal.removeEventListener('abort', done); resolve() }
    const timer = setTimeout(done, Math.max(0, seconds) * 1000)
    if (signal) signal.addEventListener('abort', done, { once: true })
  })
}

// ─────────────────────────── LeaseClient ───────────────────────────

class LeaseClient {
  constructor() {
    this._inflight = new Set() // 在途请求的 AbortController，close() 时统一掐断
  }

  /**
   * 请求领取。返回任务列表（可能为空）；出错返回 null（调用方据此退避）；被 abort 也返回 null。
   * @param {string[][]} slots 每个元素 = 一个空闲 Lane 能跑的类型列表。无 n 参数——一 Lane 一槽
   * @param {AbortSignal} [signal]
   */
  async acquire(slots, signal) {
    // 空 = 回落 WEB_BASE_URL：常见部署是在同一反代下给领取服务挂路径前缀，不新增防火墙面
    const base = settings.TASK_SERVER_URL || settings.WEB_BASE_URL
    const url = `${base.replace(/\/+$/, '')}/task/leases`
    const payload = { worker_id: settings.WORKER_ID, slots }
    // 筛选是整台机器的属性；不配置就不带这两个键
    const [maxLevel, maxPoints] = leaseFilters()
    if (maxLevel !== null) payload.max_level = maxLevel
    if (maxPoints !== null) payload.max_compute_points = maxPoints

    // 单次请求的控制器：既响应外部 signal，也响应 close()
    const ac = new AbortController()
    const onAbort = () => ac.abort()
    if (signal) {
      if (signal.aborted) return null
      signal.addEventListener('abort', onAbort, { once: true })
    }
    this._inflight.add(ac)
    let res
    try {
      res = await httpClient.request('POST', url, {
        headers: {
          [E.HEADER_INTERNAL_TOKEN]: settings.INTERNAL_TOKEN,
          'Content-Type': 'application/json',
          // worker_id 同时走 header 与 body（协议 §3.1，header 优先）
          [E.HEADER_WORKER_ID]: settings.WORKER_ID,
        },
        body: payload,
        // 须 > 服务端 hold 时长，否则会在服务端正常挂起时被客户端掐断
        timeout: settings.LEASE_HOLD_SECONDS + 15.0,
        signal: ac.signal,
      })
    } catch (e) {
      if (httpClient.isAbort(e) || ac.signal.aborted) return null
      // 必须带上 url：领取通道与回调通道是两个地址，失败日志长得一样
      logger.warning(`[lease] 请求失败（将退避重连）：${e && e.message} ← ${url}`)
      return null
    } finally {
      this._inflight.delete(ac)
      if (signal) signal.removeEventListener('abort', onAbort)
    }

    if (res.status === 200) {
      const data = httpClient.jsonOf(res)
      const tasks = data && typeof data === 'object' && !Array.isArray(data) ? (data.tasks || []) : null
      if (!Array.isArray(tasks)) {
        // 200 却不是 {"tasks": [...]}：多半是反代返回了网页。当作一次失败，别让异常带走整个 worker
        logger.warning(`[lease] 200 但响应体不是预期的 JSON 对象（反代返回了网页？）将退避重连 ← ${url}`)
        return null
      }
      return tasks
    }
    if (NO_RETRY_CODES.has(res.status)) {
      logger.error(`[lease] 鉴权被拒 ${res.status} —— 需运维介入（token 配置）← ${url}`)
      return null
    }
    if (res.status === 404) {
      logger.error(
        `[lease] 404 ← ${url} —— 该地址上没有 /task/leases 路由。` +
        '多半是反代未配置转发，或 WEBX_WORKER_TASK_SERVER_URL 为空而回落到了 WEBX_WORKER_WEB_BASE_URL')
      return null
    }
    logger.warning(`[lease] 服务端返回 ${res.status}（将退避重连）← ${url}`)
    return null
  }

  /** 掐断所有在途领取请求 */
  async close() {
    for (const ac of [...this._inflight]) ac.abort()
    this._inflight.clear()
  }
}

const leaseClient = new LeaseClient()

// ─────────────────────────── LeaseDispatcher ───────────────────────────

class LeaseDispatcher {
  /**
   * @param laneManager LaneManager（numLanes / taskToLane / typesOf）
   * @param executor    需实现 execute(taskId, payload, signal)
   */
  constructor(laneManager, executor) {
    this._lm = laneManager
    this._executor = executor
    this._busy = new Map() // lane_id -> boolean
    for (let i = 0; i < laneManager.numLanes; i++) this._busy.set(i, false)
    this._freed = new Event()
    this._running = false
    this._backoff = 0
    this._ac = null // 领取循环自己的 AbortController，stop() 用它掐断挂起的领取
  }

  _laneTypes(laneId) { return this._lm.typesOf(laneId) }

  _idleLanes() { return [...this._busy].filter(([, busy]) => !busy).map(([i]) => i) }

  /** 在指定 Lane 上执行一个任务，结束后释放槽位。controller 由 loop 创建并已登记 */
  async _runOne(laneId, task, controller) {
    const taskId = String(task.id || '')
    const { signal } = controller
    try {
      // 令牌先落注册表，再做任何上报 —— 顺序反了 ack 就不带 fencing header
      fencing.register(taskId, parseInt(task.lease_seq || 0, 10))
      const cb = getCallback()

      // 立刻 ack：把认领的短租约续到常规时长。失败就放弃：任务短租约到期后自动回池
      if (!await cb.ackLease(taskId)) {
        logger.warning(`[lease] ack 失败，放弃执行 task_id=${taskId}（冷却 ${tuning.ACK_FAIL_COOLDOWN_SEC}s 再领下一个）`)
        // 冷却必须在 finally 释放槽位之前，否则主循环绕过冷却又去领，空转啃整个池子
        await sleepAbortable(tuning.ACK_FAIL_COOLDOWN_SEC, signal)
        return
      }
      if (signal.aborted) return

      // 上报 running 不可省：QUEUED→RUNNING，并让服务端写 start_time（grace 依赖它）。3 次重试，失败即放弃
      let reported = false
      for (let attempt = 0; attempt < 3; attempt++) {
        if (await cb.updateStatus(taskId, 'running')) { reported = true; break }
        if (signal.aborted) return
        if (attempt < 2) await sleepAbortable(tuning.RUNNING_RETRY_SEC, signal)
      }
      if (!reported) {
        // 与 ack 失败同一判断，差别是这里主动归还租约（带 fencing），任务立刻回池
        logger.error(`[lease] 上报 running 连续失败，放弃执行 task_id=${taskId}（归还租约回池）`)
        await cb.releaseLease(taskId)
        await sleepAbortable(tuning.ACK_FAIL_COOLDOWN_SEC, signal)
        return
      }
      if (signal.aborted) return

      await this._executor.execute(taskId, toPayload(task), signal)
    } catch (e) {
      if (e instanceof Interrupted || (e && e.name === 'Interrupted')) {
        // 停机 / 外部取消：executor 已走中断分支（不写 ok.json、不上报 fail），租约由停机路径归还
        logger.warning(`[lease] 任务被中断 task_id=${taskId}`)
        return
      }
      logger.error(`[lease] 执行异常 task_id=${taskId}: ${e && e.message}`, e)
      // 异常逃到这里 = 本轮什么都没干成（结构性故障）。必须归还租约，否则抱着租约干等一个周期，
      // 而 worker 重启也救不了（目录是 execute() 内部创建的，recovery 扫不到）。归还带 fencing
      try { await getCallback().releaseLease(taskId) } catch (_) { /* 归还失败只是慢一点，由租约到期兜底 */ }
      // 必须冷却：归还不增加 requeues，同一台 worker 会立刻又领到同一个任务（磁盘满之类的结构性故障）
      await sleepAbortable(tuning.ACK_FAIL_COOLDOWN_SEC, signal)
    } finally {
      fencing.unregister(taskId)
      const cur = runningTasks.get(taskId)
      if (cur && cur.controller === controller) runningTasks.delete(taskId)
      this._busy.set(laneId, false)
      this._freed.set()
    }
  }

  /** 主循环：有空槽就去领，没空槽就等有 Lane 空出来 */
  async loop() {
    // worker_id 是 fencing 令牌的一半，空值会让所有上报被判「不是租户」：必须响亮失败
    if (!(settings.WORKER_ID || '').trim()) {
      logger.error(
        '[lease] WORKER_ID 为空，拒绝启动领取循环。正常启动路径会由 config.deriveWorkerId() 派生；' +
        '若从非标准入口启动，请显式设置 WEBX_WORKER_ID 或 WEBX_WORKER_PREFIX。')
      return
    }

    this._running = true
    this._ac = new AbortController()
    draining = false
    dispatchers.add(this)
    const [lv, mp] = leaseFilters()
    logger.info(
      `[lease] 分发循环启动 worker_id=${settings.WORKER_ID} lanes=${JSON.stringify([...this._lm.taskToLane])} ` +
      `档位上限=${lv !== null ? lv : '不限'} 算力点上限=${mp !== null ? mp : '不限'}`)

    try {
      while (this._running && !draining) {
        const idle = this._idleLanes()
        if (!idle.length) {
          // 满载 = 天然背压。不发请求，任务安静待在 DB 池里
          this._freed.clear()
          await this._freed.wait()
          continue
        }

        const slots = idle.map((i) => this._laneTypes(i)).filter((s) => s.length)
        if (!slots.length) {
          // 空闲 Lane 没配任何任务类型（配置问题），别空转打服务端
          await sleepAbortable(tuning.NO_SLOT_SLEEP_SEC, this._ac.signal)
          continue
        }

        const t0 = monotonic()
        const tasks = await leaseClient.acquire(slots, this._ac.signal)

        if (draining || !this._running) {
          // 长轮询在途时开始了停机，回包里仍可能带着刚认领的任务：不执行，立刻还回池
          if (tasks && tasks.length) await giveBackClaimed(tasks)
          return
        }

        if (tasks === null) {
          await this._sleepBackoff()
          continue
        }
        this._backoff = 0 // 成功一次即清零

        if (!tasks.length) {
          // 服务端挂满 hold 后的正常空返回：立即重连不退避，但要有最小间隔（K22：HOLD=0 / 静态 200 会成紧循环）
          const spent = monotonic() - t0
          if (spent < tuning.MIN_EMPTY_INTERVAL_SEC) {
            await sleepAbortable(tuning.MIN_EMPTY_INTERVAL_SEC - spent, this._ac.signal)
          }
          continue
        }

        for (const task of tasks) {
          const laneId = this._lm.taskToLane.get(task.task_type)
          if (laneId === undefined || this._busy.get(laneId)) {
            // 理论上不会发生（只索取了空闲 Lane 的类型）。真发生了就不执行：任务短租约到期后回池，
            // 比派给错误的 Lane 安全
            logger.error(`[lease] 回包无法映射到空闲 Lane（type=${task.task_type} lane=${laneId}），放弃 task_id=${task.id}`)
            continue
          }
          this._busy.set(laneId, true)
          const controller = new AbortController()
          const tid = String(task.id || '')
          // 登记在册，停机时才取消得掉（见 cancelRunningTasks）；_runOne 的 finally 自动摘除
          if (tid) runningTasks.set(tid, { controller })
          this._runOne(laneId, task, controller).catch((e) => logger.error(`[lease] _runOne 未捕获异常: ${e && e.message}`, e))
        }
      }
    } finally {
      dispatchers.delete(this)
    }
  }

  async _sleepBackoff() {
    this._backoff = Math.min(BACKOFF_MAX_SEC, this._backoff ? this._backoff * 2 : BACKOFF_BASE_SEC)
    const delay = this._backoff * (1 + (Math.random() * 2 - 1) * JITTER_RATIO)
    await sleepAbortable(Math.max(0.1, delay), this._ac && this._ac.signal)
  }

  /** 停止领取循环，并掐断挂起中的领取请求 */
  stop() {
    this._running = false
    if (this._ac) this._ac.abort()
    this._freed.set()
  }

  /** 优雅退出时归还所有在手租约（实现见模块级 releaseHeldLeases） */
  async releaseAll() { await releaseHeldLeases() }
}

// ─────────────────────────── 停机三步用到的函数（模块级） ───────────────────────────

/** 同步停止领取新任务。停机收尾的第一步，必须先于令牌捕获 / 取消 / 归还 */
function beginDrain() {
  draining = true
  for (const d of dispatchers) d._freed.set() // 唤醒等空槽的循环，让它看到标志后退出
}

/** 排空期间领到的任务：带着刚拿到的令牌原样归还，不执行 */
async function giveBackClaimed(tasks) {
  const tokens = {}
  for (const task of tasks) {
    const tid = String(task.id || '')
    if (!tid) continue
    fencing.register(tid, parseInt(task.lease_seq || 0, 10))
    tokens[tid] = fencing.headersFor(tid)
  }
  try {
    await releaseHeldLeases(tokens)
  } finally {
    for (const tid of Object.keys(tokens)) fencing.unregister(tid)
  }
}

/**
 * 同步取消所有在跑任务（abort 其 AbortController），返回取消了几个。
 * abort 在任何 await 之前生效，executor 因此走中断分支（不写 ok.json、绝不上报 fail），
 * 而不是把「停机时子进程被杀」判成任务失败。
 * 调用顺序：先 heldFencingHeaders() 捕获令牌 → 再本函数 → 最后 releaseHeldLeases(tokens)。
 */
function cancelRunningTasks() {
  let n = 0
  for (const { controller } of runningTasks.values()) {
    if (!controller.signal.aborted) { controller.abort(); n += 1 }
  }
  return n
}

/** 同步捕获当前在手租约的 fencing 令牌 `{task_id: headers}`（停机路径专用，须在 cancelRunningTasks 之前） */
function heldFencingHeaders() {
  const out = {}
  for (const tid of fencing.snapshot()) out[tid] = fencing.headersFor(tid)
  return out
}

/**
 * 归还当前在手的全部租约。并发、带 fencing、幂等，返回归还了几个。
 * 并发是因为串行会随 Lane 数线性吃掉停机预算；幂等是因为重复归还只会得到 403，视为正常。
 * tokens：预先捕获的令牌；停机路径必须传（在跑任务被取消后注册表随时会被清空）。
 */
async function releaseHeldLeases(tokens = null) {
  if (tokens === null) tokens = heldFencingHeaders()
  const ids = Object.keys(tokens)
  if (!ids.length) return 0
  logger.info(`[lease] 归还 ${ids.length} 个在手租约`)
  const cb = getCallback()
  await Promise.allSettled(ids.map((tid) => cb.releaseLease(tid, tokens[tid])))
  return ids.length
}

/** 认领回来的任务行 → executor 期望的 payload（字段见协议 §3.1） */
function toPayload(task) {
  let steps = task.steps
  if (typeof steps === 'string') {
    try { steps = steps ? JSON.parse(steps) : [] } catch (_) { steps = [] }
  }
  return {
    task_id: task.id,
    name: task.name || '',
    task_type: task.task_type,
    files_json: task.files_json,
    params_text: task.params_text,
    allowed_retries: task.allowed_retries,
    steps: steps || [],
    sub_step: task.sub_step || '',
    end_step: task.end_step || '',
    timeout_seconds: task.timeout_seconds !== undefined && task.timeout_seconds !== null ? task.timeout_seconds : 172800,
  }
}

module.exports = {
  LeaseClient, leaseClient, LeaseDispatcher,
  beginDrain, cancelRunningTasks, heldFencingHeaders, releaseHeldLeases, giveBackClaimed,
  toPayload, setCallbackClient, sleepAbortable,
  tuning, runningTasks,
  isDraining: () => draining,
  /** 测试用：复位模块级状态 */
  _reset() { draining = false; runningTasks.clear(); dispatchers.clear() },
}
