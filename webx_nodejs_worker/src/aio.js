'use strict'

/**
 * 异步小工具 —— 对应 Python asyncio 里本项目用到的几样原语。
 * 时间单位约定：对外接口一律【秒】（与 Python 版、配置项一致），内部再换算成毫秒。
 */

/** 单调时钟（秒）。判定租约临期 / 超时一律用它，不用墙钟 */
const monotonic = () => Number(process.hrtime.bigint()) / 1e9

const sleep = (seconds) => new Promise((resolve) => setTimeout(resolve, Math.max(0, seconds) * 1000))

class TimeoutError extends Error {
  constructor(msg = 'timeout') { super(msg); this.name = 'TimeoutError' }
}

/** 给 promise 加超时（秒）；超时抛 TimeoutError。注意：不会取消底层操作 */
function withTimeout(promise, seconds) {
  let timer
  const t = new Promise((_, reject) => { timer = setTimeout(() => reject(new TimeoutError()), seconds * 1000) })
  return Promise.race([promise, t]).finally(() => clearTimeout(timer))
}

/** 进程退出 / 停机导致的任务中断（对应 Python 的 CancelledError）：executor 走中断分支，不写 ok.json、不上报 fail */
class Interrupted extends Error {
  constructor(msg = 'interrupted') { super(msg); this.name = 'Interrupted' }
}

/** asyncio.Event：set()/clear()/isSet()/wait() */
class Event {
  constructor() { this._set = false; this._waiters = [] }

  isSet() { return this._set }

  set() {
    this._set = true
    const ws = this._waiters
    this._waiters = []
    for (const w of ws) w()
  }

  clear() { this._set = false }

  wait() {
    if (this._set) return Promise.resolve(true)
    return new Promise((resolve) => this._waiters.push(() => resolve(true)))
  }

  /** wait + 超时：被 set 返回 true，超时返回 false（对应 asyncio.wait_for(event.wait()) 吞超时） */
  async waitFor(seconds) {
    try { return await withTimeout(this.wait(), seconds) } catch (e) {
      if (e instanceof TimeoutError) return false
      throw e
    }
  }
}

/** asyncio.Queue（无界）：put 同步，get 返回 Promise */
class Queue {
  constructor() { this._items = []; this._getters = [] }

  get size() { return this._items.length }

  put(item) {
    const g = this._getters.shift()
    if (g) g(item)
    else this._items.push(item)
  }

  get() {
    if (this._items.length) return Promise.resolve(this._items.shift())
    return new Promise((resolve) => this._getters.push(resolve))
  }
}

/** asyncio.Lock：用法 `const release = await lock.acquire(); try {...} finally { release() }`，或 lock.run(fn) */
class Lock {
  constructor() { this._tail = Promise.resolve() }

  acquire() {
    let release
    const next = new Promise((r) => { release = r })
    const prev = this._tail
    this._tail = prev.then(() => next)
    return prev.then(() => release)
  }

  async run(fn) {
    const release = await this.acquire()
    try { return await fn() } finally { release() }
  }
}

module.exports = { monotonic, sleep, withTimeout, TimeoutError, Interrupted, Event, Queue, Lock }
