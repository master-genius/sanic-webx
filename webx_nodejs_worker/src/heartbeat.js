'use strict'

/**
 * 心跳管理器（多任务版）—— 移植自 webx_worker/heartbeat.py。
 *
 * 职责：为每个活跃任务起独立心跳循环；按 task_id 隔离服务端状态；解析响应里的 status 做双向心跳；
 * 失联时按租约自熔。【只置取消 Event，不杀进程】：杀进程由 executor 在等待该 Event 时执行。
 *
 * 与 Python 版的差异：
 *   · 构造函数 new HeartbeatManager(interval = 30, opts = {})，多一个可选 opts，供测试注入/压缩时间参数：
 *     { intervals: 节拍序列(秒), selfFenceMarginSec, logStreamStartBeat, callbackClient }
 *   · 状态按「条目」存放（每次 register 一个新条目）：注销后迟到的心跳响应不会污染新一轮注册；
 *     自熔基准随条目一起销毁（Python 版 _lease_deadline 注销时不清，同 task_id 重注册会读到陈旧值）。
 *   · JS 的 Promise 无法取消：unregister 只置停止标记并唤醒休眠，不等在途的心跳 / on_beat 请求返回；
 *     on_beat 回调会收到第二个参数 AbortSignal，注销时 abort，回调可据此提前放弃上传。
 *   · 允许 seedLease 先于 register 调用（暂存，register 时领取）。
 */

const { Event, monotonic } = require('./aio')
const { getLogger } = require('./log')

const logger = getLogger('heartbeat')

// 动态心跳节拍（秒）：递增质数，跑完封顶保持末值。前期密集 → 取消信号 / 日志早出；
// 稳定段恒 29s → 服务端压力可控、最坏取消延迟 ≤29s。质数错峰，降低多 worker 同相撞击概率。
const HEARTBEAT_INTERVALS = [5, 7, 11, 13, 17, 19, 23, 29]
// 从第几跳起触发 on_beat 日志推送：前 2 跳子进程多半尚无有效输出
const LOG_STREAM_START_BEAT = 3
// 自熔安全余量（秒）：worker 必须【严格早于】服务端回收租约才停下，方向搞反等于没做
const SELF_FENCE_MARGIN_SEC = 10.0

class HeartbeatManager {
  /**
   * @param {number} interval 兼容旧签名保留，不生效（节拍由序列驱动）
   * @param {object} [opts] 见文件头
   */
  constructor(interval = 30, opts = {}) {
    this.interval = interval
    this._intervals = opts.intervals || HEARTBEAT_INTERVALS
    this._marginSec = opts.selfFenceMarginSec !== undefined ? opts.selfFenceMarginSec : SELF_FENCE_MARGIN_SEC
    this._logStartBeat = opts.logStreamStartBeat !== undefined ? opts.logStreamStartBeat : LOG_STREAM_START_BEAT
    this._clientOverride = opts.callbackClient || null
    this._entries = new Map() // taskId -> entry
    this._seeds = new Map() // taskId -> 先于 register 的自熔基准
  }

  // callback.js 由并行同事移植，延迟 require：本模块可独立加载与测试
  _client() { return this._clientOverride || require('./callback').callbackClient }

  /** 用 ack 响应里的剩余租约预置自熔基准，使「从未成功续租过」的任务也有 deadline */
  seedLease(taskId, remainingMs) {
    if (remainingMs === null || remainingMs === undefined || !(remainingMs > 0)) return
    const deadline = monotonic() + remainingMs / 1000 - this._marginSec
    const entry = this._entries.get(taskId)
    if (entry) entry.deadline = deadline
    else this._seeds.set(taskId, deadline)
  }

  /**
   * 注册任务心跳（已存在则先注销旧的）。
   * @param {string} taskId
   * @param {Function|{onBeat?: Function}} [onBeatOrOpts] 回调函数本身，或 { onBeat } 选项对象（两种写法都收）。
   *   onBeat: (taskId, signal: AbortSignal) => Promise<void>，每次成功心跳（确认存活）后、
   *   自第 logStreamStartBeat 跳起【后台派发】；异常被吞（仅告警）
   */
  async register(taskId, onBeatOrOpts = null) {
    const onBeat = typeof onBeatOrOpts === 'function' ? onBeatOrOpts : (onBeatOrOpts && onBeatOrOpts.onBeat) || null
    if (this._entries.has(taskId)) await this.unregister(taskId)
    const entry = {
      taskId,
      status: 'running',
      cancelEvent: new Event(),
      deadline: this._seeds.has(taskId) ? this._seeds.get(taskId) : null,
      onBeat,
      abort: new AbortController(),
      stopped: false,
      wake: null, // 当前休眠的唤醒函数
      beatInflight: false, // on_beat 单任务串行
      beatPending: false, // 在途期间又来了新节拍 → 待补发（合并为一次，以最新内容为准）
    }
    this._seeds.delete(taskId)
    this._entries.set(taskId, entry)
    this._loop(entry).catch((e) => logger.error(`[心跳] 循环意外退出 task_id=${taskId}: ${e}`))
    logger.debug(`[心跳] 注册 task_id=${taskId}`)
  }

  /** 注销任务心跳：置停止标记、唤醒休眠、撤销待补发的增量日志 */
  async unregister(taskId) {
    this._seeds.delete(taskId)
    const entry = this._entries.get(taskId)
    if (!entry) return
    this._entries.delete(taskId)
    entry.stopped = true
    entry.beatPending = false
    entry.abort.abort()
    if (entry.wake) entry.wake()
    logger.debug(`[心跳] 注销 task_id=${taskId}`)
  }

  /** 取消信号 Event（未注册返回 null，调用方据此退化为「不可中断地等子进程跑完」） */
  getCancelEvent(taskId) {
    const e = this._entries.get(taskId)
    return e ? e.cancelEvent : null
  }

  /** 服务端返回的状态（如 running / cancelled / revoked / not_found / lease_expired），默认 running */
  getServerStatus(taskId) {
    const e = this._entries.get(taskId)
    return e ? e.status : 'running'
  }

  _markNonRunning(entry, status) {
    entry.status = status
    entry.cancelEvent.set()
  }

  /** 可被 unregister 打断的休眠；定时器 unref，不拖住进程退出 */
  _sleep(entry, seconds) {
    if (entry.stopped) return Promise.resolve()
    return new Promise((resolve) => {
      const timer = setTimeout(done, Math.max(0, seconds) * 1000)
      timer.unref()
      function done() { clearTimeout(timer); entry.wake = null; resolve() }
      entry.wake = done
    })
  }

  * _intervalSeq() {
    for (const v of this._intervals) yield v
    while (true) yield this._intervals[this._intervals.length - 1]
  }

  /** 派发增量日志上传：单任务串行 + 合并最新（trailing-edge）；只派发，不 await 上传 */
  _fireOnBeat(entry) {
    if (!entry.onBeat) return
    if (entry.beatInflight) { entry.beatPending = true; return }
    this._runOnBeat(entry)
  }

  async _runOnBeat(entry) {
    entry.beatInflight = true
    try {
      await entry.onBeat(entry.taskId, entry.abort.signal)
    } catch (e) {
      logger.warning(`[心跳] on_beat 日志推送失败 task_id=${entry.taskId}: ${e && e.message ? e.message : e}`)
    }
    entry.beatInflight = false
    // 在途期间又有新节拍 → 合并为一次「尾发」，用最新内容补一次
    if (entry.beatPending && !entry.stopped) {
      entry.beatPending = false
      this._runOnBeat(entry)
    } else {
      entry.beatPending = false
    }
  }

  /**
   * 单任务心跳循环。
   *   404 / 非 running → 置取消 Event + 停止心跳；running → 续租并（第 N 跳起）推日志；
   *   网络故障 → 继续循环，不推日志（服务端不可达，上传无意义）。
   * 起跑前的 queued 不是取消信号：心跳从下载输入之前就开始，而 RUNNING 上报可能晚到，
   * 此刻服务端如实答 queued。这样放宽是安全的——真被回收 / 重置时服务端走 fencing 分支答 revoked
   * （协议 §4.2：200 + status=revoked），「holder 还是我 + queued」只可能是自己尚未起跑的那一段。
   * 见过 running 之后，任何非 running 一律当取消。
   */
  async _loop(entry) {
    const taskId = entry.taskId
    const intervals = this._intervalSeq()
    let beat = 0
    let seenRunning = false
    while (!entry.stopped) {
      beat += 1
      try {
        const result = await this._client().updateHeartbeat(taskId)
        if (entry.stopped) return
        if (result.isNotFound) {
          this._markNonRunning(entry, 'not_found')
          logger.warning(`[心跳] ${taskId} → 404(not_found)，停止心跳`)
          break
        }
        if (!result.isError) { // 200 正常响应
          const data = result.data
          const serverStatus = data && data.status ? data.status : null
          if (serverStatus === 'running') {
            seenRunning = true
          } else if (serverStatus === 'queued' && !seenRunning) {
            // 起跑前：续租照做（这正是提前注册心跳的目的），但不推日志
            this._noteRenewed(entry, data)
            if (this._checkSelfFence(entry)) break
            await this._sleep(entry, intervals.next().value)
            continue
          }
          if (serverStatus && serverStatus !== 'running') {
            this._markNonRunning(entry, serverStatus)
            logger.warning(`[心跳] ${taskId} → ${serverStatus}，停止心跳`)
            break
          }
          this._noteRenewed(entry, data)
          if (beat >= this._logStartBeat) this._fireOnBeat(entry)
        }
        // isError → 网络故障，继续循环
      } catch (e) {
        if (entry.stopped) return
        logger.error(`[心跳] 异常 task_id=${taskId}: ${e && e.message ? e.message : e}`)
      }
      // 自熔：即使连不上云端，也要在租约到期前自己停下
      if (this._checkSelfFence(entry)) break
      await this._sleep(entry, intervals.next().value)
    }
  }

  /**
   * 记录一次成功续租：本地单调时钟基准 + 服务端告知的租约时长。
   * 用 monotonic 而不是墙钟（NTP 跳变不该影响熔断）；租约时长必须来自服务端
   * （grace 随任务已投入时长增长，本地推算会漂到服务端回收之后）。
   * 优先 lease_remaining_ms（同一事务里用服务端时钟算好，无跨机偏移，协议 G5）；
   * 缺失才回退「lease_until − 本机墙钟」（旧服务端，时钟偏移会进入判据）。
   */
  _noteRenewed(entry, data) {
    if (!data || typeof data !== 'object') return
    const leaseUntil = data.lease_until
    if (leaseUntil === null || leaseUntil === undefined) {
      entry.deadline = null // 无租约（升级前遗留的在途任务）：不启用自熔
      return
    }
    let remainSec
    if (data.lease_remaining_ms !== null && data.lease_remaining_ms !== undefined) {
      remainSec = Number(data.lease_remaining_ms) / 1000
    } else {
      remainSec = Number(leaseUntil) / 1000 - Date.now() / 1000
    }
    if (!Number.isFinite(remainSec) || remainSec <= 0) return
    // 减去安全余量：必须严格早于服务端回收
    entry.deadline = monotonic() + remainSec - this._marginSec
  }

  /**
   * 租约将到期而续租一直失败 → 置取消 Event（由 executor 终止子进程）。返回是否已熔断。
   * 租约区别于锁的根本性质：过期时刻双方都知道，因此不需要通信就能达成一致，
   * 浪费上界 = 租约时长，与分区持续多久无关。它不阻止 A 继续往共享存储写字节（fencing 只护 DB 指针）。
   */
  _checkSelfFence(entry) {
    if (entry.deadline === null || entry.deadline === undefined) return false
    if (monotonic() < entry.deadline) return false
    logger.error(`[心跳] ${entry.taskId} 租约已到期且续租持续失败 → 自我熔断，终止子进程（服务端多半已把任务重新分配）`)
    this._markNonRunning(entry, 'lease_expired')
    entry.deadline = null
    return true
  }

  /** 停止所有心跳（Worker 优雅退出） */
  async shutdownAll() {
    for (const id of [...this._entries.keys()]) await this.unregister(id)
    logger.info('[心跳] 全部停止')
  }
}

module.exports = {
  HeartbeatManager, HEARTBEAT_INTERVALS, LOG_STREAM_START_BEAT, SELF_FENCE_MARGIN_SEC,
}
