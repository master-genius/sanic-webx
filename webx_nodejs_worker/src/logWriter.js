'use strict'

/**
 * 日志截断写入器 —— 移植自 webx_worker/log_writer.py。
 * 「头 N 行 + 尾 M 行」截断，防止长程任务打满磁盘 / 上传流量；面向前端的内容统一在
 * getTruncatedContent 出口脱敏（基础设施日志整行剔除、URL / 内部接口路径遮蔽、本机绝对路径收敛）。
 *
 * 与 Python 版的差异：
 *   · 只移植 LineAssembler / TruncatedLogWriter / ConcurrentLogWriter 与脱敏函数；
 *     Python 版全 worker 无调用方的 StreamingLogWriter 不移植。
 *   · 构造参数改为 options 对象：new ConcurrentLogWriter({headLines, tailLines, redactRoots, redactors})；
 *     buildRedactRoots({workDir, condaRoot, channels, workerRoot, home, scriptRoot})。
 *     channels 的配置键沿用 channel 配置文件里的写法（env / env_name）。
 *   · 脱敏正则去掉 lru_cache（按需现编，量级可忽略）；Python 的 \w 是 Unicode 感知的，
 *     这里用 \p{L}\p{N}\p{M}_ + u 标志对齐，中文 / 日文目录名同样处理。
 *   · JS 字符串按 UTF-16 码元计长，单行上限 / 覆盖列与 Python 按码点计在非 BMP 字符上略有出入，无实际影响。
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { StringDecoder } = require('node:string_decoder')
const { getLogger } = require('./log')

const logger = getLogger('logWriter')

// ==================== 前端可见日志脱敏 ====================
// 子进程 stdout 会混入与云端交互的「基础设施日志」（结果上传 / 字段回调 / 产物下载），
// 对用户无意义且暴露内部接口路径 / URL。命中整行剔除；残留的 URL / 内部路径再兜底遮蔽。
// 仅作用于面向前端的出口（getTruncatedContent）；本机日志与本地全量日志文件保留原文，便于排障。

const REDACT_DROP_RE = new RegExp(
  '/(?:internal|public)/tasks/' + // 内部/公开任务 API 路径
  '|results/upload' + // 结果上传端点
  '|download_url' + // 上传回传的下载地址字段
  '|结果文件上传|任务字段更新|产物下载|产物 404', // sdk.result_client 的内部回调日志前缀
)
const REDACT_URL_RE = /https?:\/\/[^\s'"]+/g
const REDACT_PATH_RE = /\/(?:internal|public)\/tasks\/[^\s'"]*/g

// 业务脱敏 redactors（来自 TaskKindPlugin.redactors）：整段文本 → 整段文本，在内置规则【之前】执行
// （业务规则可能依赖原文里的 URL 作锚点）；抛异常的 redactor 被跳过。

// 文件系统绝对路径脱敏：已知根精确替换为语义标签（相对结构保留）→ 通用家目录收敛 → 兜底折叠成末尾两段。
const FOLD_KEEP_SEGMENTS = 2
// 【单一来源】一个路径段允许的字符：边界断言与段匹配必须共用，否则会出现「段中途替换」
// （如 /home/ps 命中 /home/psdz2/x → [HOME]dz2/x）。空格刻意不含（见 Python 版注释，已评估后不做）。
const SEG_CHARS = String.raw`[\p{L}\p{N}\p{M}_.+\-@:]`
// 兜底折叠只对名单内顶级目录生效：名单是要被遮掉的根；其余（/tmp /srv /mnt …）原样保留
const FS_PATH_ROOTS = ['root', 'media', 'home', 'var', 'etc', 'opt', 'usr']
// 前一字符不能是字母数字 / ~ / ]（挡 kJ/mol/nm、2026/07/29、已被替换成 [WORKER]/… 的结果）；根后至少还有 2 段
const FS_PATH_RE = new RegExp(
  String.raw`(?<![\p{L}\p{N}_~\]])/(?:${FS_PATH_ROOTS.join('|')})(?:/${SEG_CHARS}+){2,}/?`, 'gu',
)
const GENERIC_HOME_RE = new RegExp(String.raw`(?<![\p{L}\p{N}_~\]])/(?:home|Users)/${SEG_CHARS}+`, 'gu')
// 已知根的路径边界：前缀之后不能再接路径名字符，按「路径段」而非「子串」替换
const PATH_BOUNDARY = `(?!${SEG_CHARS})`

const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

function foldPath(raw) {
  const trailing = raw.endsWith('/') ? '/' : ''
  const segs = raw.split('/').filter(Boolean)
  return '…/' + segs.slice(-FOLD_KEEP_SEGMENTS).join('/') + trailing
}

/**
 * 收集本机的「已知绝对路径根」。返回 [[前缀, 标签], ...]，已按前缀长度倒序（长前缀先匹配）。
 * add 的顺序即「具体 → 泛化」，同前缀保留首次登记的标签。
 */
function buildRedactRoots({
  workDir = '', condaRoot = '', channels = null, workerRoot = null, home = null, scriptRoot = null,
} = {}) {
  const roots = []
  const add = (p, label) => {
    if (!p) return
    const cands = new Set([String(p)])
    const expanded = String(p).startsWith('~') ? path.join(os.homedir(), String(p).slice(1)) : String(p)
    cands.add(expanded)
    try { cands.add(fs.realpathSync(expanded)) } catch (_) { cands.add(path.resolve(expanded)) } // 符号链接：子进程打印的往往是解析后的路径
    for (let c of cands) {
      c = c.replace(/\/+$/, '')
      if (c.length > 1) roots.push([c, label])
    }
  }
  // 计算环境前缀：conda 走 {root}/envs/{name}；uv 的 env_name 本身就是 python 路径
  for (const cfg of Object.values(channels || {})) {
    if (!cfg || typeof cfg !== 'object') continue
    const envName = String(cfg.env_name || '')
    if (!envName) continue
    if (cfg.env === 'conda') { if (condaRoot) add(path.join(condaRoot, 'envs', envName), '[ENV]') }
    else if (cfg.env === 'uv') add(envName, '[ENV]')
  }
  add(condaRoot, '[CONDA]')
  add(workDir, '[WORK_DIR]')
  add(workerRoot || path.resolve(__dirname, '..'), '[WORKER]')
  add(scriptRoot, '[SCRIPTS]')
  add(home || os.homedir(), '[HOME]')

  const uniq = new Map()
  for (const [prefix, label] of roots) if (!uniq.has(prefix)) uniq.set(prefix, label)
  return [...uniq.entries()].sort((a, b) => b[0].length - a[0].length)
}

/** 把连续 ≥2 个空行合并为 1 个（保留该段第一行原文，不改写）；纯空白即算空行 */
function collapseBlankRuns(text) {
  const out = []
  const lines = text.split('\n')
  for (let i = 0; i < lines.length;) {
    if (lines[i].trim()) { out.push(lines[i++]); continue }
    let j = i
    while (j < lines.length && !lines[j].trim()) j++
    if (j - i >= 2) out.push(lines[i])
    else for (let k = i; k < j; k++) out.push(lines[k])
    i = j
  }
  return out.join('\n')
}

// 与 Python str.splitlines() 对齐：尾部换行不产生多余空行
const LINE_SPLIT_RE = new RegExp(String.raw`\r\n|[\n\r\v\f\x1c-\x1e\x85\u2028\u2029]`)

/**
 * 剔除 / 遮蔽面向前端日志中的内部接口路径、URL、上传回调结果与本机文件系统路径。
 * @param {string|null} text
 * @param {Array<[string,string]>} roots buildRedactRoots 的产物（须已按长度倒序）
 * @param {Array<(t: string) => string>} redactors 业务脱敏函数，在内置规则之前依次执行
 */
function redactForFrontend(text, roots = [], redactors = []) {
  if (!text) return text
  for (const fn of redactors || []) {
    try { text = fn(text) } catch (_) { /* 脱敏失败不得影响日志推送 */ }
  }
  const compiled = roots.map(([prefix, label]) => [prefix, new RegExp(escapeRe(prefix) + PATH_BOUNDARY, 'gu'), label])
  const lines = text.split(LINE_SPLIT_RE)
  if (lines.length && lines[lines.length - 1] === '') lines.pop()
  const kept = []
  for (let line of lines) {
    if (REDACT_DROP_RE.test(line)) continue
    line = line.replace(REDACT_URL_RE, '[redacted]').replace(REDACT_PATH_RE, '[redacted]')
    for (const [prefix, re, label] of compiled) {
      if (line.includes(prefix)) line = line.replace(re, () => label)
    }
    line = line.replace(GENERIC_HOME_RE, '[HOME]')
    line = line.replace(FS_PATH_RE, foldPath)
    kept.push(line)
  }
  return collapseBlankRuns(kept.join('\n'))
}

// ==================== 回车（\r）行装配 ====================
// `mdrun -v` 之类用 \r 把进度原地刷新、长时间不输出 \n。若把 \r 当换行会产出成千上万行近乎相同的进度行，
// 挤掉截断缓冲尾部真正有用的收尾输出。还原终端语义：\r = 光标回行首、后续字符原地覆盖。

const MAX_LINE_CHARS = 64 * 1024 // 单行硬上限：无 \r 也无 \n 的超长输出强制定版，防止无界增长

class LineAssembler {
  constructor() {
    this._line = '' // 当前行缓冲（已按 \r 覆盖合成）
    this._col = 0 // 光标所在列
    // StringDecoder 跨 chunk 保留被切开的多字节字符；坏字节替换为 U+FFFD，不抛异常
    this._decoder = new StringDecoder('utf8')
  }

  /** 当前尚未结束的活动行内容（空 = 无） */
  get line() { return this._line }

  /** 丢弃当前行状态（异常兜底）。不重置解码器：它持有的是半个字符，与行内容无关 */
  reset() { this._line = ''; this._col = 0 }

  /**
   * 喂入一段 stdout 字节（Buffer，也接受 string），返回行事件 [[行内容, 是否仍未结束], ...]：
   *   [text, false] 该行已由 \n 结束（或触顶强制定版），后续不会再变；
   *   [text, true]  该行还在被 \r 刷新中的快照，调用方应覆盖上一次的同一行。
   */
  feed(chunk) {
    const events = []
    const segments = (typeof chunk === 'string' ? chunk : this._decoder.write(chunk)).split('\n')
    for (let idx = 0; idx < segments.length; idx++) {
      const pieces = segments[idx].split('\r')
      for (let j = 0; j < pieces.length; j++) {
        if (j) this._col = 0 // 遇到 \r：光标归零，后续字符原地覆盖
        const piece = pieces[j]
        if (!piece) continue
        if (this._col === 0 && piece.length >= this._line.length) {
          this._line = piece // 快路径：从行首整行覆盖（进度刷新的常态）
        } else {
          this._line = this._line.slice(0, this._col) + piece + this._line.slice(this._col + piece.length)
        }
        this._col += piece.length
      }
      if (idx < segments.length - 1) {
        events.push([this._line, false]) // 以 \n 结束 → 定版
        this.reset()
      } else if (this._line.length > MAX_LINE_CHARS) {
        events.push([this._line, false])
        this.reset()
      }
    }
    if (this._line) events.push([this._line, true])
    return events
  }

  /** EOF 收尾：残留活动行定版（写入器会覆盖其槽位，不产生重复行） */
  close() {
    const tail = this._decoder.end() // 吐出尚未凑齐的残余字节（替换符）
    if (tail) this.feed(tail)
    if (!this._line) return []
    const events = [[this._line, false]]
    this.reset()
    return events
  }
}

// ==================== 截断缓冲 ====================

class TruncatedLogWriter {
  constructor(headLines = 100, tailLines = 2000) {
    this.headLines = headLines
    this.tailLines = tailLines
    this._head = []
    this._tail = [] // 环形语义：超过 tailLines 从头丢
    this._headFilled = false
    this._totalLines = 0
    // 内容修订号：每次 write 自增，含活动行原地覆盖。增量推送的去冗余水位必须用它而非行数——
    // \r 刷新进度时行数不变、内容在变，按行数判断会让长程 MD 期间的前端日志假死
    this._writes = 0
    this._liveSlot = null // 上一条 partial 行落在哪个缓冲（'head'/'tail'）；记缓冲名而非索引，head 写满翻转后仍命中
  }

  /**
   * 写入一行（不含换行符）。partial=true 表示该行尚未结束（\r 刷新中的活动行）：
   * 连续 partial 互相覆盖只占一个槽位，随后一次 partial=false 把槽位定版关闭。
   * 不变量：单一生产者串行调用（活动行槽位打开期间，partial=false 是「定版当前槽位」而非「另起一行」）。
   */
  write(line, partial = false) {
    this._writes += 1
    if (this._liveSlot !== null) {
      // 槽位可能已不存在（tailLines=0 时不保留任何元素）：静默丢弃，绝不让异常冒出日志路径
      const buf = this._liveSlot === 'head' ? this._head : this._tail
      if (buf.length) buf[buf.length - 1] = line
      if (!partial) this._liveSlot = null
      return
    }
    this._totalLines += 1
    let slot
    if (!this._headFilled) {
      this._head.push(line)
      slot = 'head'
      if (this._head.length >= this.headLines) this._headFilled = true
    } else {
      this._tail.push(line)
      if (this._tail.length > this.tailLines) this._tail.shift()
      slot = 'tail'
    }
    this._liveSlot = partial ? slot : null
  }

  /** 截断后的完整日志文本 */
  getContent() {
    const out = [...this._head]
    if (this._totalLines > this.headLines + this.tailLines) {
      out.push(`\n... [省略 ${this._totalLines - this.headLines - this.tailLines} 行] ...\n`)
    }
    out.push(...this._tail)
    return out.join('\n')
  }

  saveToFile(filepath) {
    fs.mkdirSync(path.dirname(filepath), { recursive: true })
    fs.writeFileSync(filepath, this.getContent(), 'utf8')
  }

  get totalLines() { return this._totalLines }

  get writes() { return this._writes }

  clear() {
    this._head = []
    this._tail = []
    this._headFilled = false
    this._totalLines = 0
    this._writes = 0
    this._liveSlot = null
  }
}

// ==================== 并发安全日志写入器 ====================

/**
 * Worker 通用日志接口：单实例共享，按 task_id 隔离，同时写本地文件与内存截断缓冲。
 * 任务结束后用 getTruncatedContent 取（已脱敏的）截断日志上报，并调用 closeTask 释放文件句柄。
 */
class ConcurrentLogWriter {
  /**
   * @param {object} opts
   * @param {number} [opts.headLines]
   * @param {number} [opts.tailLines]
   * @param {Array<[string,string]>} [opts.redactRoots] buildRedactRoots 的产物；构造期一次性传入，
   *   保证增量日志从第一次推送起就已脱敏
   * @param {Array<Function>} [opts.redactors] 存引用而非拷贝：插件之后追加的函数同样生效
   */
  constructor({ headLines = 100, tailLines = 2000, redactRoots = [], redactors = [] } = {}) {
    this.headLines = headLines
    this.tailLines = tailLines
    this.redactRoots = redactRoots
    this.redactors = redactors
    this._writers = new Map() // taskId -> TruncatedLogWriter
    this._files = new Map() // taskId -> {fd, path}
    this._taskDirs = new Map() // taskId -> 任务目录（write 收到的 path 的父目录）
  }

  /**
   * 写入一行（同时写本地文件和截断缓冲）。async 以匹配 executor 的 await 调用模式，实际为同步 I/O。
   * partial 只影响截断缓冲（前端可见、需控量）；本地文件每次调用都追加一条，保留活动行的演进快照。
   */
  async write(taskId, filePath, text, { partial = false } = {}) {
    if (!this._writers.has(taskId)) {
      const full = path.resolve(filePath)
      fs.mkdirSync(path.dirname(full), { recursive: true })
      this._files.set(taskId, { fd: fs.openSync(full, 'a'), path: full })
      this._writers.set(taskId, new TruncatedLogWriter(this.headLines, this.tailLines))
      this._taskDirs.set(taskId, path.dirname(full))
    }
    const s = String(text)
    this._writers.get(taskId).write(s.replace(/\n+$/, ''), partial)
    fs.writeSync(this._files.get(taskId).fd, s.endsWith('\n') ? s : s + '\n')
  }

  /** 截断日志内容（已脱敏，面向前端）；所有前端可见出口统一经此收口。任务不存在返回 null */
  getTruncatedContent(taskId) {
    const w = this._writers.get(taskId)
    if (!w) return null
    return redactForFrontend(w.getContent(), this._rootsFor(taskId), this.redactors)
  }

  /** 该任务的脱敏根 = 任务目录（最长，必须排最前）+ 构造期登记的静态根 */
  _rootsFor(taskId) {
    const dir = this._taskDirs.get(taskId)
    if (!dir) return this.redactRoots
    const cands = new Set([dir])
    try { cands.add(fs.realpathSync(dir)) } catch (_) { /* 目录已被清理 */ }
    const head = [...cands].map((c) => c.replace(/\/+$/, '')).filter((c) => c.length > 1).map((c) => [c, '[TASK_DIR]'])
    head.sort((a, b) => b[0].length - a[0].length)
    return [...head, ...this.redactRoots]
  }

  /** 保存截断日志到文件；默认旁边的 .truncated.log。任务不存在返回 null */
  saveTruncated(taskId, targetPath = null) {
    const w = this._writers.get(taskId)
    if (!w) return null
    if (!targetPath) {
      const f = this._files.get(taskId)
      if (!f) return null
      targetPath = f.path.replace(/\.[^./]*$/, '') + '.truncated.log'
    }
    w.saveToFile(targetPath)
    return targetPath
  }

  /** 关闭文件句柄并清理内存缓冲 */
  async closeTask(taskId) {
    const f = this._files.get(taskId)
    this._files.delete(taskId)
    if (f) { try { fs.closeSync(f.fd) } catch (e) { logger.debug(`关闭日志句柄失败 ${taskId}: ${e.message}`) } }
    this._writers.delete(taskId)
    this._taskDirs.delete(taskId)
  }

  getTotalLines(taskId) {
    const w = this._writers.get(taskId)
    return w ? w.totalLines : 0
  }

  /** 内容修订号（含 \r 活动行原地刷新）；增量推送用它做「内容是否变过」的水位 */
  getWriteCount(taskId) {
    const w = this._writers.get(taskId)
    return w ? w.writes : 0
  }
}

module.exports = {
  LineAssembler, TruncatedLogWriter, ConcurrentLogWriter,
  buildRedactRoots, redactForFrontend, collapseBlankRuns, MAX_LINE_CHARS,
}
