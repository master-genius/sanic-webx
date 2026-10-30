'use strict'

/**
 * Worker 日志配置 —— 移植自 webx_worker/log_config.py。
 *
 *   · 文件名 worker_task_{YYYY-MM-DD-HH}_{pid}.log：日期在前、字典序即时间序；PID 放最后，
 *     只作并发去重后缀（多实例共用目录互不覆盖）。进程重启换 PID → 新文件，不接续旧文件。
 *   · 单文件双上限：≤ maxBytes 或 ≤ maxLines，任一先达到切新文件；同小时内滚动为 .1.log / .2.log …
 *   · setupLogging 始终挂控制台；logDir 非空时把 process.stdout / stderr tee 到滚动文件，
 *     于是 log.js 输出、console.log、未捕获异常堆栈全部落盘，与控制台完全镜像。
 *
 * 与 Python 版的差异：
 *   · 对接 log.js 的 addSink / setLevel。log.js 一旦挂了 sink 就不再自己写 stderr，
 *     所以控制台也以 sink 形式挂上（写 process.stderr，经 tee 一并落盘）；文件侧不单独挂 sink，避免重复落盘。
 *   · 行数按实际写入文本里的换行数计（Python 版每次 write 计 1 行）。
 *   · 未捕获异常用 'uncaughtExceptionMonitor' 记录，不改变进程默认的崩溃行为。
 *   · 额外导出 teardownLogging()，还原 stdout/stderr 并摘掉 sink（测试与重入用）。
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const log = require('./log')

const DEFAULT_MAX_BYTES = 10 * 1024 * 1024 // 10MB
const DEFAULT_MAX_LINES = 20000

const pad = (n) => String(n).padStart(2, '0')
const hourKeyOf = (ms) => {
  const d = new Date(ms)
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}-${pad(d.getHours())}`
}
const countNewlines = (s) => { let n = 0; for (let i = s.indexOf('\n'); i >= 0; i = s.indexOf('\n', i + 1)) n++; return n }

/** 按小时命名 + 大小 / 行数双上限滚动的文件写入器 */
class HourlyRotatingFileHandler {
  /**
   * @param {string} logDir
   * @param {object} [opts] { prefix, maxBytes, maxLines, pid, now }  now：() => 毫秒时间戳，供测试注入
   */
  constructor(logDir, { prefix = 'worker_task', maxBytes = DEFAULT_MAX_BYTES, maxLines = DEFAULT_MAX_LINES, pid = process.pid, now = Date.now } = {}) {
    const dir = String(logDir).startsWith('~') ? path.join(os.homedir(), String(logDir).slice(1)) : String(logDir)
    this.logDir = path.resolve(dir)
    fs.mkdirSync(this.logDir, { recursive: true })
    this.prefix = prefix
    this.pid = pid
    this.maxBytes = maxBytes
    this.maxLines = maxLines
    this._now = now
    this._hourKey = null
    this._seq = 0
    this._fd = null
    this._curBytes = 0
    this._curLines = 0
  }

  /** seq=0 → worker_task_{hour}_{pid}.log；seq>0 → worker_task_{hour}_{pid}.{seq}.log */
  pathFor(hourKey, seq) {
    const stem = `${this.prefix}_${hourKey}_${this.pid}`
    return path.join(this.logDir, stem + (seq === 0 ? '.log' : `.${seq}.log`))
  }

  /** 该小时本进程已存在文件的最大 seq（路径含 PID，兄弟实例的文件不会被误认） */
  _latestSeq(hourKey) {
    if (!fs.existsSync(this.pathFor(hourKey, 0))) return 0
    let seq = 0
    for (let n = 1; fs.existsSync(this.pathFor(hourKey, n)); n++) seq = n
    return seq
  }

  _open(hourKey, seq) {
    const p = this.pathFor(hourKey, seq)
    this._curBytes = 0
    this._curLines = 0
    if (fs.existsSync(p)) { // 接续已有文件：按实际内容初始化计数器
      this._curBytes = fs.statSync(p).size
      this._curLines = countNewlines(fs.readFileSync(p, 'utf8'))
    }
    this._fd = fs.openSync(p, 'a')
    this._hourKey = hourKey
    this._seq = seq
  }

  _closeFd() {
    if (this._fd !== null) { try { fs.closeSync(this._fd) } catch (_) { /* 忽略 */ } this._fd = null }
  }

  /** 写入原始文本（log 行与 tee 的 stdout/stderr 共用同一份滚动 / 计数逻辑） */
  writeText(text) {
    if (!text) return
    const bytes = Buffer.byteLength(text, 'utf8')
    const lines = countNewlines(text)
    const hourKey = hourKeyOf(this._now())
    if (this._fd === null || hourKey !== this._hourKey) { // 新小时：接续该小时已有最大 seq
      this._closeFd()
      this._open(hourKey, this._latestSeq(hourKey))
    }
    // 同小时内写本条会超限则切 seq+1 新文件；仅当前文件已有内容时才滚，避免单条超大消息空转
    const over = this._curLines + lines > this.maxLines || this._curBytes + bytes > this.maxBytes
    if (over && (this._curBytes > 0 || this._curLines > 0)) {
      this._closeFd()
      this._open(this._hourKey, this._seq + 1)
    }
    fs.writeSync(this._fd, text)
    this._curBytes += bytes
    this._curLines += lines
  }

  close() { this._closeFd() }
}

// ---- 全局状态（setupLogging 幂等）----
let state = null // { handler, restore: [], removeSink, level }

/** 还原 stdout/stderr、摘掉 sink、关文件 */
function teardownLogging() {
  if (!state) return
  for (const fn of state.restore) { try { fn() } catch (_) { /* 忽略 */ } }
  if (state.removeSink) state.removeSink()
  if (state.handler) state.handler.close()
  state = null
}

/** 把写入同时转发给原流与文件；文件侧任何异常都吞掉，绝不影响控制台 / journald */
function teeStream(stream, handler) {
  const orig = stream.write
  stream.write = function (chunk, ...rest) {
    const r = orig.call(stream, chunk, ...rest)
    try { handler.writeText(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8')) } catch (_) { /* 文件侧故障不得波及控制台 */ }
    return r
  }
  return () => { stream.write = orig }
}

let monitorInstalled = false

/**
 * 配置日志：控制台始终挂，logDir 非空时追加文件落盘。
 * @param {object} [opts] { logDir, debug, maxBytes, maxLines }
 * @returns {string|null} 实际生效的日志目录（null = 仅控制台）
 */
function setupLogging({ logDir = null, debug = false, maxBytes = DEFAULT_MAX_BYTES, maxLines = DEFAULT_MAX_LINES } = {}) {
  teardownLogging() // 幂等：重入时清掉旧配置，避免 tee 套 tee / 重复输出
  log.setLevel(debug ? 'DEBUG' : 'INFO')
  const st = { handler: null, restore: [], removeSink: null }

  let effective = null
  if (logDir) {
    st.handler = new HourlyRotatingFileHandler(logDir, { maxBytes, maxLines })
    effective = st.handler.logDir
    // 必须先装 tee 再挂控制台 sink：sink 写的是（已被包裹的）process.stderr
    st.restore.push(teeStream(process.stdout, st.handler))
    st.restore.push(teeStream(process.stderr, st.handler))
  }
  st.removeSink = log.addSink((line) => { process.stderr.write(line + '\n') })
  state = st

  // 兜底：记录未捕获异常（Monitor 事件只观察，不改变默认崩溃行为）
  if (!monitorInstalled) {
    monitorInstalled = true
    const logger = log.getLogger('webx_worker')
    process.on('uncaughtExceptionMonitor', (err) => logger.error('未捕获异常', err))
  }
  return effective
}

module.exports = {
  setupLogging, teardownLogging, HourlyRotatingFileHandler, DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES,
}
