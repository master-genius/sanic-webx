'use strict'

/**
 * 任务目录清理 —— 移植自 webx_worker/cleanup.py：对超龄的终态任务目录做硬删除（纯本地，无网络依赖）。
 *
 * 依据 ok.json 协议（docs/worker.md §7）：executor 接收新任务时先删旧 ok.json，所以 ok.json 始终
 * 对应「上一次完整执行」。判定规则按顺序短路：
 *   1 无 ok.json → 跳过（执行中 / 旧版本目录）    2 ok.json 损坏 → 跳过
 *   3 status 不在 policy → 跳过                   4 server_confirmed=false → 跳过（终态未上报，留作诊断 / 补报）
 *   5 age(now - completed_at) < 阈值 → 跳过        5.5 仍被上传队列引用 → 跳过   6 否则 rmtree
 *
 * 与 Python 版的差异：
 *   · 时长一律用【秒】：parseDuration 返回秒；数字取值按【秒】（Python 版 int/float 按天），
 *     字符串写法不变（"30d" / "3h" / "120m" / "300s"，无后缀的数字串按天）。
 *   · 扫描时跳过点号开头的目录（如 uploader 队列 .uploads/，它们不是任务目录），不计入 scanned。
 *   · 上传队列目录布局内联（<WORK_DIR>/.uploads/{pending,inflight}/{task_id}__{upload_id}.json），
 *     不依赖 uploader 模块的落地顺序。
 *   · 结果对象用 camelCase 字段；toDict() 给出与 Python 一致的 snake_case 结构（CLI 输出用）。
 *   · scanAndPurge 同步执行（与 Python 一致，主要用于 CLI 子命令）。
 */

const fs = require('node:fs')
const path = require('node:path')
const { parseArgs } = require('node:util')
const { getLogger } = require('./log')

const logger = getLogger('cleanup')

const DAY = 86400
// 默认策略：failed 2 天、completed 7 天（秒）。产物已确认上传，本地目录只是可重建的中间态，
// 磁盘峰值占用比追溯窗口更值钱。cancelled 不在默认列表，留给手动 CLI 触发。
const DEFAULT_POLICY = Object.freeze({ failed: 2 * DAY, completed: 7 * DAY })

const QUEUE_DIRNAME = '.uploads'
const QUEUE_SUBDIRS = ['pending', 'inflight']

class CleanupResult {
  constructor() {
    this.scanned = 0
    this.purged = 0
    this.purgedTaskIds = []
    this.skipped = {}
    this.errors = []
  }

  skip(key) { this.skipped[key] = (this.skipped[key] || 0) + 1 }

  toDict() {
    return {
      scanned: this.scanned,
      purged: this.purged,
      purged_task_ids: this.purgedTaskIds,
      skipped: { ...this.skipped },
      errors: this.errors,
    }
  }
}

/**
 * 解析时长，返回【秒】。数字 → 秒；"30" / "30d" → 天；"3h" 小时；"120m" 分钟；"300s" 秒。
 */
function parseDuration(spec) {
  const bad = () => new Error(`无效的时长: ${JSON.stringify(spec)}`)
  if (typeof spec === 'number') {
    if (!Number.isFinite(spec)) throw bad()
    return spec
  }
  const s = String(spec).trim().toLowerCase()
  if (!s) throw bad()
  const unit = { d: DAY, h: 3600, m: 60, s: 1 }[s[s.length - 1]]
  const num = unit ? s.slice(0, -1) : s
  if (!/^[+-]?(\d+\.?\d*|\.\d+)$/.test(num)) throw bad()
  return Number(num) * (unit || DAY)
}

/** 取 ok.json 的参考时间（毫秒）；缺失或解析失败回退文件 mtime。无时区的 ISO 串按 UTC 解读 */
function parseOkTimestamp(okData, okPath) {
  const ts = okData.completed_at
  if (ts && typeof ts === 'string') {
    const s = /T/.test(ts) && !/(Z|[+-]\d{2}:?\d{2})$/i.test(ts) ? ts + 'Z' : ts
    const ms = Date.parse(s)
    if (!Number.isNaN(ms)) return ms
  }
  return fs.statSync(okPath).mtimeMs
}

/** 人类可读的 age：'30d' / '1d 5h' / '2h 10m' / '30m' */
function formatAge(seconds) {
  const total = Math.floor(seconds)
  const days = Math.floor(total / 86400)
  const hours = Math.floor((total % 86400) / 3600)
  const minutes = Math.floor((total % 3600) / 60)
  if (days >= 1) return hours ? `${days}d ${hours}h` : `${days}d`
  if (hours >= 1) return minutes ? `${hours}h ${minutes}m` : `${hours}h`
  return `${minutes}m`
}

class TaskDirCleaner {
  /** @param {string} workBaseDir  @param {boolean} [dryRun] */
  constructor(workBaseDir, dryRun = false) {
    this.workBaseDir = String(workBaseDir)
    this.dryRun = dryRun
    this._pendingTasksCache = null // 每轮 scanAndPurge 重置
  }

  /**
   * 扫描工作目录，按策略清理超龄任务。
   * @param {object|null} policy {status: 时长}，时长由 parseDuration 解析；null = DEFAULT_POLICY
   * @param {boolean|null} dryRun 覆盖实例标记；null = 沿用 this.dryRun
   */
  scanAndPurge(policy = null, dryRun = null) {
    const effective = {}
    for (const [s, t] of Object.entries(policy || DEFAULT_POLICY)) {
      effective[s] = parseDuration(t)
    }
    const isDry = dryRun === null ? this.dryRun : dryRun
    this._pendingTasksCache = null

    const result = new CleanupResult()
    if (!fs.existsSync(this.workBaseDir)) {
      logger.warning(`[Cleanup] 工作目录不存在: ${this.workBaseDir}`)
      return result
    }
    const nowMs = Date.now()
    logger.info(`[Cleanup] 开始扫描 ${this.workBaseDir}，策略={ ${Object.entries(effective).map(([s, t]) => `${s}: ${formatAge(t)}`).join(', ')} }${isDry ? ' (dry-run)' : ''}`)

    const names = fs.readdirSync(this.workBaseDir).sort()
    for (const name of names) {
      if (name.startsWith('.')) continue // 基础设施目录（.uploads 等）不是任务
      const dir = path.join(this.workBaseDir, name)
      let isDir = false
      try { isDir = fs.statSync(dir).isDirectory() } catch (_) { /* 竞态消失 */ }
      if (!isDir) continue
      result.scanned += 1
      try {
        this._evaluateOne(dir, effective, nowMs, isDry, result)
      } catch (e) {
        logger.error(`[Cleanup] task=${name} 异常: ${e.message}`, e)
        result.errors.push(`${name}: ${e.message}`)
        result.skip('error')
      }
    }
    logger.info(`[Cleanup] 完成 scanned=${result.scanned} purged=${result.purged} skipped=${JSON.stringify(result.skipped)} errors=${result.errors.length}`)
    return result
  }

  /**
   * 上传队列当前引用到的 task_id 集合（每轮只读一次并缓存）。目录本身就是共享状态，无需与 uploader 通信。
   * 读取失败一律返回已读到的部分：清理是空间回收，不该因观测失败而停摆。
   */
  _tasksWithPendingUploads() {
    if (this._pendingTasksCache) return this._pendingTasksCache
    const tasks = new Set()
    for (const sub of QUEUE_SUBDIRS) {
      try {
        for (const f of fs.readdirSync(path.join(this.workBaseDir, QUEUE_DIRNAME, sub))) {
          if (f.endsWith('.json')) tasks.add(f.split('__', 1)[0]) // {task_id}__{upload_id}.json，不必解析 JSON
        }
      } catch (e) {
        logger.debug(`[Cleanup] 读取上传队列失败（按无待传处理）: ${e.message}`)
      }
    }
    this._pendingTasksCache = tasks
    return tasks
  }

  _evaluateOne(taskDir, policy, nowMs, dryRun, result) {
    const taskId = path.basename(taskDir)
    const okPath = path.join(taskDir, 'ok.json')
    if (!fs.existsSync(okPath)) return result.skip('no_ok_json') // 规则 1

    let okData
    try { // 规则 2
      okData = JSON.parse(fs.readFileSync(okPath, 'utf8'))
      if (!okData || typeof okData !== 'object' || Array.isArray(okData)) throw new Error('ok.json 不是对象')
    } catch (e) {
      result.skip('bad_ok_json')
      logger.warning(`[Cleanup] task=${taskId} ok.json 解析失败: ${e.message}`)
      return
    }
    const status = okData.status || ''
    if (!Object.prototype.hasOwnProperty.call(policy, status)) return result.skip(`out_of_scope_${status || 'unknown'}`) // 规则 3
    if (!okData.server_confirmed) { // 规则 4：终态可能尚未上报到服务端
      result.skip(`unconfirmed_${status}`)
      logger.warning(`[Cleanup] task=${taskId} status=${status} confirmed=False，跳过清理`)
      return
    }
    const ageSec = (nowMs - parseOkTimestamp(okData, okPath)) / 1000 // 规则 5
    if (ageSec < policy[status]) return result.skip(`fresh_${status}`)
    // 规则 5.5：队列只记路径、不复制文件，清理必须避开还没传完的产物（删了产物就永久缺失）
    if (this._tasksWithPendingUploads().has(taskId)) {
      result.skip(`pending_upload_${status}`)
      logger.info(`[Cleanup] task=${taskId} 仍有产物待上传，跳过清理`)
      return
    }
    if (dryRun) {
      logger.info(`[Cleanup][DRY-RUN] task=${taskId} status=${status} age=${formatAge(ageSec)} → 会清理`)
    } else { // 规则 6：硬删
      fs.rmSync(taskDir, { recursive: true, force: true })
      logger.info(`[Cleanup] task=${taskId} status=${status} age=${formatAge(ageSec)} → 已清理`)
    }
    result.purged += 1
    result.purgedTaskIds.push(taskId)
  }
}

// ─── CLI ────────────────────────────────────────────────────────────────

/** 合并 --status 与 --timeout（可重复）为 policy（秒）；规则同 Python 版，出错抛 Error */
function buildPolicyFromArgs(statuses, timeouts) {
  statuses = statuses || []
  timeouts = timeouts || []
  if (!statuses.length && !timeouts.length) return { ...DEFAULT_POLICY }
  if (timeouts.length && !statuses.length) throw new Error('--timeout 必须与 --status 配对使用')
  const policy = {}
  if (!timeouts.length) {
    for (const s of statuses) {
      if (!(s in DEFAULT_POLICY)) throw new Error(`status='${s}' 不在默认策略中，必须显式提供 --timeout`)
      policy[s] = DEFAULT_POLICY[s]
    }
    return policy
  }
  if (timeouts.length === 1) {
    const t = parseDuration(timeouts[0])
    for (const s of statuses) policy[s] = t
    return policy
  }
  if (timeouts.length === statuses.length) {
    statuses.forEach((s, i) => { policy[s] = parseDuration(timeouts[i]) })
    return policy
  }
  throw new Error(`--status 数量(${statuses.length}) 与 --timeout 数量(${timeouts.length}) 不匹配`)
}

/** CLI 入口：返回退出码（有错误 → 1）。结果 JSON（snake_case）打印到 stdout */
function main(argv = process.argv.slice(2)) {
  const { values } = parseArgs({
    args: argv,
    options: {
      'work-dir': { type: 'string' },
      status: { type: 'string', multiple: true },
      timeout: { type: 'string', multiple: true },
      'dry-run': { type: 'boolean', default: false },
      quiet: { type: 'boolean', short: 'q', default: false },
    },
  })
  if (values.quiet) require('./log').setLevel('WARNING')
  try {
    const workDir = values['work-dir'] || require('./config').settings.WORK_DIR
    const policy = buildPolicyFromArgs(values.status, values.timeout)
    const result = new TaskDirCleaner(workDir, values['dry-run']).scanAndPurge(policy)
    process.stdout.write(JSON.stringify(result.toDict(), null, 2) + '\n')
    return result.errors.length ? 1 : 0
  } catch (e) {
    process.stderr.write(`[ERR] ${e.message}\n`)
    return 1
  }
}

if (require.main === module) process.exit(main())

module.exports = {
  TaskDirCleaner, CleanupResult, DEFAULT_POLICY, parseDuration, formatAge, buildPolicyFromArgs, main,
}
