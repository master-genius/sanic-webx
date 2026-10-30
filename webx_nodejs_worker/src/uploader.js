'use strict'

/**
 * 独立上传进程 —— 把大产物的上传从计算的关键路径上摘出去。
 *
 *     node bin/webx-worker.js uploader     # 由 worker 父进程经 uploaderSupervisor 拉起
 *
 * 零回边：不通知任何人，唯一出边是存储服务（传）与任务服务（权威检查 / 交付地址）。
 * 不需要谁通知它该传什么——扫 `{WORK_DIR}/.uploads/pending/`，写文件成功就是送达（见 sdk/uploadQueue）。
 *
 * 判据：每次上传前问一次 `GET /internal/tasks/<id>`：任务存在（非 404）且状态不是 failed/cancelled → 要传；
 * 问不到 / 5xx / 401 / 403 → 保留（部署问题修好即自愈，不能因一次令牌填错清空队列）。
 * 传完后 `POST /internal/tasks/<id>/deliveries`（协议 §4.9）：刻意不带 fencing；404 且体里有 error → 丢弃，
 * 404 无 error（路由缺失）→ 保留；其余 → 幂等 merge。两阶段：上传成功先记 uploadedUrl，交付失败只重做交付。
 *
 * 与 Python 版的差异：
 *   · K18：分片 409 且响应缺 Upload-Offset 头时，先退而看 JSON 的 offset、再 GET 会话取 offset；
 *     都拿不到，或连续 409 未能前进超过 MAX_RESYNC_STRIKES 次 → 本轮失败，交队列的指数退避，不再无限重发。
 *   · K17：同名条目被 redo 覆盖后旧条目的 drop/release 误伤新条目——由 sdk/uploadQueue 的 nonce 校验解决
 *     （drop/release 发现磁盘上是新一代条目就不删不覆盖）；本文件只保证条目对象原样往返（nonce 不丢）。
 *   · 分片读盘用 FileHandle.read（异步，不阻塞事件循环）；整传用 gohttp 的 multipart 流式上传。
 *   · 并发控制：在途数达到 UPLOADER_CONCURRENCY 即停止本轮派发（Python 用信号量，效果相同）。
 *   · 父进程消失自杀：没有 PDEATHSIG，改为轮询 process.ppid（cdpc 在父进程正常退出时也会收走子进程）。
 *   · 队列接口按命名规则取自 sdk/uploadQueue.js：iterPending/claim/release/update/drop/requeueInflight/
 *     ensureDirs/queueRoot/inflightDir；Entry 字段 taskId/localPath/remoteName/keyPath/size/queuedAt/attempts/
 *     uploadedUrl/sessionId/firstQueuedAt/filename。
 */

const fs = require('node:fs')
const path = require('node:path')
const E = require('./envnames')
const { settings, storageConfigured } = require('./config')
const httpClient = require('./httpClient')
const q = require('./sdk/uploadQueue')
const { Event, sleep, withTimeout, TimeoutError } = require('./aio')
const { getLogger } = require('./log')

const logger = getLogger('uploader')

/** 可调参数（测试里压小）。CHUNK_SIZE：storage 单片上限默认 64MB，取 8MB 在「往返次数」与「单次读盘」间折中 */
const tuning = {
  CHUNK_SIZE: 8 * 1024 * 1024,
  RECHECK_EVERY_CHUNKS: 8, // 每传多少分片复查一次云端（8 × 8MB = 64MB）
  SHUTDOWN_GRACE_SEC: 10,
  MAX_RESYNC_STRIKES: 5, // K18：409 重对齐连续未前进的容忍次数
}

// 单次 HTTP 的空闲超时（秒）：gohttp 的 timeout 是「无数据」超时，不限制整体传输时长
const CTRL_TIMEOUT = 30
const DATA_TIMEOUT = 300

// ─────────────────────────── 配置解析 ───────────────────────────

/** worker 实际请求存储服务的地址：内网 → 对外。没有回退到任务服务的路径（协议 §5） */
function storageBase() {
  return (settings.STORAGE_INTERNAL_URL.trim() || settings.STORAGE_URL.trim()).replace(/\/+$/, '')
}

/** 存储服务专属 Token，未配则回退主 Token */
function storageToken() {
  return settings.STORAGE_INTERNAL_TOKEN.trim() || settings.INTERNAL_TOKEN
}

/** 落库 URL 用的【对外】地址。空则保持相对路径 */
function publicPrefix() {
  return settings.STORAGE_URL.trim().replace(/\/+$/, '')
}

/** 存储端回传相对路径 → 落库用的绝对/相对 URL（与 sdk upload_result 同口径） */
function normalizeDownloadUrl(rel) {
  if (!rel || /^https?:\/\//.test(rel)) return rel
  const pub = publicPrefix()
  return pub ? `${pub}/${rel.replace(/^\/+/, '')}` : rel
}

const taskUrl = (taskId, suffix = '') =>
  `${settings.WEB_BASE_URL.replace(/\/+$/, '')}/internal/tasks/${encodeURIComponent(taskId)}${suffix}`
const storageTaskUrl = (taskId, suffix) =>
  `${storageBase()}/internal/tasks/${encodeURIComponent(taskId)}${suffix}`

// ─────────────────────────── 重试退避 ───────────────────────────
// 队列语义是「永不放弃」，但永不放弃不等于每 3 秒重试一次：恒定失败的条目会每轮重发整个文件、刷屏。
// 按 attempts 指数退避（release() 已记 attempts 与本次失败时刻 queuedAt）。封顶 5 分钟以保持自愈及时性。
const RETRY_BACKOFF_BASE_SEC = 5.0
const RETRY_BACKOFF_MAX_SEC = 300.0
const RETRY_ALERT_EVERY = 10 // 连续失败到这个次数升一级日志

/** 第 attempts 次失败后应等待的秒数（5,10,20,…,封顶 300） */
function retryDelay(attempts) {
  if (attempts <= 0) return 0
  return Math.min(RETRY_BACKOFF_BASE_SEC * (2 ** (attempts - 1)), RETRY_BACKOFF_MAX_SEC)
}

/** 该条目本轮是否该重试；now 为墙钟秒（queuedAt 是墙钟）。首次（attempts=0）恒 true */
function isDue(entry, now) {
  if (entry.attempts <= 0) return true
  return (now - entry.queuedAt) >= retryDelay(entry.attempts)
}

// 业务拒绝 → 丢弃（404=任务没了，400=请求不合法）；鉴权 401/403 → 保留（部署问题不是数据问题）
const BUSINESS_REJECT_CODES = new Set([400, 404])
const AUTH_RETAIN_CODES = new Set([401, 403])
const DEAD_TASK_STATUSES = new Set(['cancelled', 'failed'])

/** ["step2","out_url"] + url → {step2: {out_url: url}}（协议 §4.9：服务端按嵌套 key 幂等 merge） */
function nest(keyPath, value) {
  let out = value
  for (let i = keyPath.length - 1; i >= 0; i--) out = { [keyPath[i]]: out }
  return out
}

/** 404 是否为「任务不存在」（体里有 error 键）而非「路由不存在」。认不出来一律 false（保守保留） */
function isTaskNotFound(res) {
  const body = httpClient.jsonOf(res)
  return Boolean(body) && typeof body === 'object' && !Array.isArray(body) && 'error' in body
}

// ─────────────────────────── Uploader ───────────────────────────

class Uploader {
  /**
   * @param {string} workDir
   * @param {AbortSignal} [signal] 强制中止在途 HTTP（优雅停止宽限期过后用）
   */
  constructor(workDir, signal = null) {
    this.workDir = workDir
    this.signal = signal
  }

  // ───── 云端权威判据 ─────

  /**
   * 这条上传现在还需要吗？上传前必问。问不到时返回 true 保留条目：上传是幂等覆盖，多传只费带宽；误丢是产物永久缺失。
   */
  async stillWanted(entry) {
    const url = taskUrl(entry.taskId)
    let res
    try {
      res = await httpClient.request('GET', url, {
        headers: { [E.HEADER_INTERNAL_TOKEN]: settings.INTERNAL_TOKEN }, timeout: CTRL_TIMEOUT, signal: this.signal,
      })
    } catch (e) {
      logger.warning(`[uploader] 权威检查失败（按仍需要处理）: ${entry.remoteName}, ${e && e.message}`)
      return true
    }
    const code = res.status
    if (BUSINESS_REJECT_CODES.has(code)) {
      logger.info(`[uploader] 云端拒绝(${code})，删记录: task=${entry.taskId} ${entry.remoteName}`)
      return false
    }
    if (AUTH_RETAIN_CODES.has(code)) {
      logger.warning(`[uploader] 权威检查鉴权失败(${code})，保留待人工处理: ${entry.remoteName}`)
      return true
    }
    if (code !== 200) {
      logger.warning(`[uploader] 权威检查返回 ${code}（按仍需要处理）: ${entry.remoteName}`)
      return true
    }
    const body = httpClient.jsonOf(res)
    if (!body || typeof body !== 'object') {
      logger.warning('[uploader] 任务信息解析失败（按仍需要处理）')
      return true
    }
    // 协议 §4.10 是扁平对象；兼容包在 data 里的写法
    const data = body.data && typeof body.data === 'object' ? body.data : body
    const status = String(data.status || '').trim().toLowerCase()
    if (DEAD_TASK_STATUSES.has(status)) {
      logger.info(`[uploader] 任务已 ${status}，删记录: task=${entry.taskId} ${entry.remoteName}`)
      return false
    }
    return true
  }

  // ───── 上传 ─────

  /** 整传（小文件）。返回相对 download_url，失败 null。`POST /internal/tasks/<id>/results`（201） */
  async uploadSingle(entry) {
    let res
    try {
      res = await httpClient.upload(storageTaskUrl(entry.taskId, '/results'), {
        files: { file: entry.localPath }, form: { filename: entry.remoteName },
        headers: { [E.HEADER_INTERNAL_TOKEN]: storageToken() }, timeout: DATA_TIMEOUT, signal: this.signal,
      })
    } catch (e) {
      logger.warning(`[uploader] 整传失败: ${entry.remoteName}, ${e && e.message}`)
      return null
    }
    if (res.status !== 200 && res.status !== 201) {
      logger.warning(`[uploader] 整传被拒 ${res.status}: ${entry.remoteName}`)
      return null
    }
    return (httpClient.jsonOf(res, {}) || {}).download_url || ''
  }

  /** 查续传会话已收字节；会话不存在（过期/被清）返回 null，调用方重新创建。依据响应头 Upload-Offset，缺失才看 JSON */
  async sessionOffset(sessionId) {
    let res
    try {
      res = await httpClient.request('GET', `${storageBase()}/internal/uploads/${encodeURIComponent(sessionId)}`, {
        headers: { [E.HEADER_INTERNAL_TOKEN]: storageToken() }, timeout: CTRL_TIMEOUT, signal: this.signal,
      })
    } catch (_) { return null }
    if (res.status !== 200) return null
    return parseOffset(res)
  }

  async abortSession(sessionId) {
    if (!sessionId) return
    try {
      await httpClient.request('DELETE', `${storageBase()}/internal/uploads/${encodeURIComponent(sessionId)}`, {
        headers: { [E.HEADER_INTERNAL_TOKEN]: storageToken() }, timeout: CTRL_TIMEOUT, signal: this.signal,
      })
    } catch (_) { /* 失败不重要——storage 侧有超龄会话清理兜底 */ }
  }

  /**
   * 分片续传（大文件）。返回相对 download_url，失败 null。
   * 断点续传（崩溃后凭 sessionId 取 offset 接着传）+ 中途可放弃（每 RECHECK_EVERY_CHUNKS 片复查一次云端）。
   */
  async uploadResumable(entry) {
    const headers = { [E.HEADER_INTERNAL_TOKEN]: storageToken() }
    const total = fs.statSync(entry.localPath).size

    // 复用已有会话（上次崩在半路）；取不到 offset 说明会话已过期 → 重新创建
    let offset = entry.sessionId ? await this.sessionOffset(entry.sessionId) : null
    if (offset === null) {
      let res
      try {
        res = await httpClient.request('POST', storageTaskUrl(entry.taskId, '/results/uploads'), {
          headers, body: { filename: entry.remoteName, total }, timeout: CTRL_TIMEOUT, signal: this.signal,
        })
      } catch (e) {
        logger.warning(`[uploader] 创建续传会话失败: ${entry.remoteName}, ${e && e.message}`)
        return null
      }
      if (res.status !== 200 && res.status !== 201) {
        logger.warning(`[uploader] 创建续传会话被拒 ${res.status}: ${entry.remoteName}`)
        return null
      }
      const meta = httpClient.jsonOf(res, {}) || {}
      entry.sessionId = String(meta.upload_id || '')
      offset = parseInt(meta.offset || 0, 10) || 0
      if (!entry.sessionId) return null
      q.update(this.workDir, entry) // 会话 ID 立刻落盘，崩溃后才接得上
    } else {
      logger.info(`[uploader] 续传接力: ${entry.remoteName} 从 ${offset}/${total} 继续`)
    }

    const sessionUrl = `${storageBase()}/internal/uploads/${encodeURIComponent(entry.sessionId)}`
    const CHUNK = tuning.CHUNK_SIZE
    let chunkNo = 0
    let strikes = 0 // K18：连续 409 次数，成功前进一片清零
    const fh = await fs.promises.open(entry.localPath, 'r')
    try {
      const buf = Buffer.allocUnsafe(CHUNK)
      while (offset < total) {
        const { bytesRead } = await fh.read(buf, 0, CHUNK, offset)
        if (!bytesRead) break
        let res
        try {
          res = await httpClient.request('PATCH', sessionUrl, {
            headers: { ...headers, 'Upload-Offset': String(offset), 'Content-Type': 'application/octet-stream' },
            rawBody: buf.subarray(0, bytesRead), timeout: DATA_TIMEOUT, signal: this.signal,
          })
        } catch (e) {
          logger.warning(`[uploader] 分片发送失败 @${offset}: ${entry.remoteName}, ${e && e.message}`)
          return null
        }

        if (res.status === 409) {
          // 服务端 offset 与我们的不一致 → 按服务端给的真实值重对齐。头缺失（反代吞头）时退而看 JSON / GET 会话
          let serverOff = parseOffset(res)
          if (serverOff === null) serverOff = await this.sessionOffset(entry.sessionId)
          strikes += 1
          if (serverOff === null || serverOff < 0 || serverOff > total || strikes > tuning.MAX_RESYNC_STRIKES) {
            logger.warning(
              `[uploader] 409 无法重对齐（server_offset=${serverOff} strikes=${strikes}），本轮放弃交由退避重试: ${entry.remoteName}`)
            return null
          }
          logger.info(`[uploader] offset 重同步 ${offset} → ${serverOff}: ${entry.remoteName}`)
          offset = serverOff
          continue
        }
        if (res.status !== 200 && res.status !== 204) {
          logger.warning(`[uploader] 分片被拒 ${res.status} @${offset}: ${entry.remoteName}`)
          return null
        }

        strikes = 0
        offset += bytesRead
        chunkNo += 1

        // 中途复查：几 GB 传到一半才发现任务已取消，白烧的带宽很可观
        if (chunkNo % tuning.RECHECK_EVERY_CHUNKS === 0 && !await this.stillWanted(entry)) {
          await this.abortSession(entry.sessionId)
          entry.sessionId = ''
          return null
        }
      }
    } finally {
      await fh.close().catch(() => {})
    }

    let res
    try {
      res = await httpClient.request('POST', `${sessionUrl}/complete`, { headers, timeout: DATA_TIMEOUT, signal: this.signal })
    } catch (e) {
      logger.warning(`[uploader] 续传 complete 失败: ${entry.remoteName}, ${e && e.message}`)
      return null
    }
    if (res.status !== 200 && res.status !== 201) {
      logger.warning(`[uploader] 续传 complete 被拒 ${res.status}: ${entry.remoteName}`)
      return null
    }
    entry.sessionId = ''
    return (httpClient.jsonOf(res, {}) || {}).download_url || ''
  }

  // ───── 回写 ─────

  /**
   * 把真 URL 交付给任务（merge 进 result_path）。返回三态字符串，不是 bool：
   * 'done' 写进去了 / 'drop' 任务已不存在，出队但没有交付 / 'retry' 暂时性失败
   */
  async writeback(entry) {
    // 走产物交付专用端点：本进程长驻、手里永远没有 per-task 租约令牌，不能走严格 fencing 的 PATCH /fields
    const payload = { result_path: nest(entry.keyPath, normalizeDownloadUrl(entry.uploadedUrl)) }
    let res
    try {
      res = await httpClient.request('POST', taskUrl(entry.taskId, '/deliveries'), {
        headers: { [E.HEADER_INTERNAL_TOKEN]: settings.INTERNAL_TOKEN }, body: payload,
        timeout: CTRL_TIMEOUT, signal: this.signal,
      })
    } catch (e) {
      logger.warning(`[uploader] 交付失败: ${entry.remoteName}, ${e && e.message}`)
      return 'retry'
    }
    if (res.status === 200) return 'done'
    if (res.status === 404) {
      // 404 有两种成因，丢弃前必须分清：① 任务真的没了 → 丢；② 反代/服务端缺 /deliveries 路由 → 丢了产物地址永久缺失。
      // 判据是响应体形状（任务不存在回 {"error":...}）。认不出来一律保留
      if (isTaskNotFound(res)) {
        logger.info(`[uploader] 回写时任务已不存在，丢弃: ${entry.taskId}`)
        return 'drop'
      }
      logger.warning(
        `[uploader] 交付 404 但不是「任务不存在」，保留待重试: ${entry.remoteName} —— 多半是任务服务/反代缺 /deliveries 路由，就位后自愈`)
      return 'retry'
    }
    logger.warning(`[uploader] 交付被拒 ${res.status}: ${entry.remoteName}`)
    return 'retry'
  }

  // ───── 单条处理 ─────

  /** 处理一条：权威检查 → 上传 → 回写。任一环失败即退回队列，下轮再来 */
  async process(entry) {
    const claimed = q.claim(this.workDir, entry)
    if (!claimed) return // 已被取走（rename 天然互斥）
    entry = claimed

    try {
      let isFile = false
      try { isFile = fs.statSync(entry.localPath).isFile() } catch (_) { /* 不存在 */ }
      if (!isFile) {
        // 清理侧设计上会跳过队列引用的文件，这里是竞态兜底——补传一个不存在的文件没有意义
        logger.warning(`[uploader] 本地文件已不在，丢弃: ${entry.localPath}`)
        q.drop(this.workDir, entry)
        return
      }

      if (!await this.stillWanted(entry)) {
        await this.abortSession(entry.sessionId)
        q.drop(this.workDir, entry)
        return
      }

      // 两阶段：已经传成功过就只做回写，绝不重传几个 GB
      if (!entry.uploadedUrl) {
        const size = fs.statSync(entry.localPath).size
        const rel = size > tuning.CHUNK_SIZE ? await this.uploadResumable(entry) : await this.uploadSingle(entry)
        if (!rel) {
          q.release(this.workDir, entry)
          return
        }
        entry.uploadedUrl = rel
        q.update(this.workDir, entry) // 先落盘再回写：崩在这里也不会重传
        logger.info(`[uploader] 上传完成: task=${entry.taskId} ${entry.remoteName} (${size} 字节)`)
      }

      const outcome = await this.writeback(entry)
      if (outcome === 'retry') {
        q.release(this.workDir, entry)
      } else {
        q.drop(this.workDir, entry)
        if (outcome === 'done') logger.info(`[uploader] 已交付: ${entry.remoteName} → ${JSON.stringify(entry.keyPath)}`)
      }
    } catch (e) {
      // 单条出错绝不能掀翻整个 uploader
      logger.error(`[uploader] 处理条目异常: ${entry.remoteName}, ${e && e.message}`, e)
      try { q.release(this.workDir, entry) } catch (_) { /* 兜底 */ }
    }
  }
}

/** 响应头 Upload-Offset → 整数；头缺失才退而看 JSON 的 offset；都没有返回 null */
function parseOffset(res) {
  const hdr = res.headers && res.headers['upload-offset']
  if (hdr !== undefined && hdr !== null && hdr !== '') {
    const n = parseInt(String(hdr), 10)
    return Number.isFinite(n) ? n : null
  }
  const body = httpClient.jsonOf(res)
  if (body && typeof body === 'object' && body.offset !== undefined && body.offset !== null) {
    const n = parseInt(body.offset, 10)
    return Number.isFinite(n) ? n : null
  }
  return null
}

// ─────────────────────────── 主循环 ───────────────────────────

/**
 * 扫描—分发主循环。永不主动退出，靠 stop（Event/signal）停。
 * @param {string} workDir
 * @param {{stopping?: Event}} [opts] 外部传入的停止 Event（main 里由 SIGTERM 置位）
 */
async function run(workDir, { stopping = new Event() } = {}) {
  q.ensureDirs(workDir)

  // 自检：正常路径由 supervisor 拦住，这里是手工单独拉起时的兜底。没有存储地址时每条上传都无处可去，
  // 而失败即退回队列、每轮重试 —— 纯粹的空转与刷屏
  if (!storageConfigured()) {
    logger.error(
      `[uploader] 未配置存储服务（${E.ENV_STORAGE_INTERNAL_URL} / ${E.ENV_STORAGE_URL}），拒绝启动 —— ` +
      '产物只能上传到存储服务，任务服务上没有对应端点。配好后再启动。')
    return
  }

  q.requeueInflight(workDir)

  const interval = Math.max(0.5, settings.UPLOADER_SCAN_INTERVAL)
  const concurrency = Math.max(1, settings.UPLOADER_CONCURRENCY)
  const abortAc = new AbortController()
  const uploader = new Uploader(workDir, abortAc.signal)
  const running = new Map() // filename -> Promise

  logger.info(`[uploader] 启动: dir=${q.queueRoot(workDir)} 并发=${concurrency} 扫描间隔=${interval}s storage=${storageBase()}`)

  while (!stopping.isSet()) {
    const now = Date.now() / 1000
    for (const entry of q.iterPending(workDir)) {
      if (stopping.isSet() || running.size >= concurrency) break
      // 在途守卫：一轮扫描别给同一条目重复起协程
      if (running.has(entry.filename)) continue
      // 退避守卫：失败过的条目要等够 retryDelay 才再试
      if (!isDue(entry, now)) continue
      if (entry.attempts && entry.attempts % RETRY_ALERT_EVERY === 0) {
        logger.error(`[uploader] 条目已连续失败 ${entry.attempts} 次，仍在重试（间隔 ${retryDelay(entry.attempts)}s）: ` +
          `task=${entry.taskId} ${entry.remoteName}`)
      }
      const p = uploader.process(entry).catch((e) => logger.error(`[uploader] 条目协程异常: ${e && e.message}`, e))
        .finally(() => running.delete(entry.filename))
      running.set(entry.filename, p)
    }
    await stopping.waitFor(interval)
  }

  // 优雅停止：给在途的一个宽限期把当前分片写完并落 offset，超时就强制中止。
  // 不等它们传完——传不完下次启动接着传，这本来就是队列的语义
  if (running.size) {
    logger.info(`[uploader] 收到停止信号，等待 ${running.size} 条在途落盘…`)
    try { await withTimeout(Promise.allSettled([...running.values()]), tuning.SHUTDOWN_GRACE_SEC) } catch (e) {
      if (!(e instanceof TimeoutError)) throw e
    }
    abortAc.abort()
  }
  logger.info('[uploader] 已退出')
}

/** 父进程被收养到 init 下（或换了父亲）即退出，避免 worker 被 SIGKILL 后留下孤儿上传进程 */
function watchParent(onOrphan, intervalSec = 2) {
  const initial = process.ppid
  const timer = setInterval(() => {
    const now = process.ppid
    if (now === 1 || now !== initial) {
      logger.warning('[uploader] 父进程已消失，退出')
      clearInterval(timer)
      onOrphan()
    }
  }, intervalSec * 1000)
  timer.unref()
  return () => clearInterval(timer)
}

/** 入口：`node bin/webx-worker.js uploader` */
async function main(argv = []) { // eslint-disable-line no-unused-vars
  const { setupLogging } = require('./logConfig')
  setupLogging({
    logDir: settings.LOG_DIR || null, debug: settings.DEBUG,
    maxBytes: settings.LOG_MAX_BYTES, maxLines: settings.LOG_MAX_LINES,
  })
  const stopping = new Event()
  const onSig = () => stopping.set()
  process.on('SIGTERM', onSig)
  process.on('SIGINT', onSig)
  const unwatch = watchParent(onSig)
  try {
    await run(settings.WORK_DIR, { stopping })
  } finally {
    unwatch()
  }
  // keep-alive 连接等残留句柄不应拖住退出
  await sleep(0)
  process.exit(0)
}

module.exports = {
  main, run, Uploader, watchParent,
  retryDelay, isDue, nest, normalizeDownloadUrl, parseOffset, tuning,
}
