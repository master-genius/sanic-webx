'use strict'

/**
 * 结果客户端 —— 任务脚本在子进程里调用任务服务 / 存储服务的内部 API。对应 Python sdk/result_client.py。
 *
 * 连接信息全部读环境变量（每次调用现读，executor 注入，见 envnames.js）：
 *   任务服务  WEBX_WORKER_INTERNAL_TOKEN / WEBX_WORKER_WEB_BASE_URL
 *   存储服务  WEBX_WORKER_STORAGE_URL(对外，落库用) / _STORAGE_INTERNAL_URL(内网，实际请求用) / _STORAGE_INTERNAL_TOKEN
 *   令牌      WEBX_WORKER_ID / WEBX_WORKER_LEASE_SEQ（随请求头发送）
 *
 * 所有函数都是 async（Node 没有同步 HTTP）；服务端 JSON 原样返回（snake_case，如 download_url），
 * 本地占位响应沿用 Python 的 {task_id, message}。可选参数改为 options 对象（见各函数签名）。
 *
 * 重试按「消息语义」分层 —— 可丢弃的消息不该阻塞计算，必须成功的消息不该只试一次：
 *   ① 状态迁移 / 字段更新：5 次 0.2s 起指数退避；仍失败转 IPC 交父进程持久重试（承重，丢了任务卡在 running）
 *   ② 进度消息：8s 短超时 2 次即弃，不入 IPC（过期进度补发只会覆盖掉更新的进度，是倒退）
 *   ③ 结果上传：5 次，2/4/8/16s 退避（累计 30s，盖住 storage 一次重启；期间占着 Lane，故不能更长）
 *
 * 与 Python 版的差异：
 *   · 403 按响应体 reason 处理，【不重试】（修 K10：原版把 403 当 5xx 试满 5 次）：
 *       already_final → 幂等成功（info 日志，不入队）；
 *       not_lessee / gone → 租约已丢，抛 HttpError（.status/.reason），不入队；
 *       bad_lease_seq / missing_fencing_token → 调用方自身 bug，抛 HttpError；
 *       其它 403（如网关）与 401 → 确定性拒绝只试 1 次，转 IPC 兜底（父进程保守保留，修好即自愈）。
 *   · 状态码 0（gohttp 在响应流出错时的返回）算传输层故障，重试；
 *   · uploadResult 的单次超时是 gohttp 的「无数据」空闲超时（300s）；httpClient 无独立 connect 超时；
 *   · downloadArtifactStream 直接用 node:http(s) 流式落盘 —— httpClient 会把响应整体读进内存（上限 500MB），
 *     且 gohttp.download 不支持 Range 续传；
 *   · uploadResult 只要求有 token，不再强求 WEB_BASE_URL（上传只走存储服务）；
 *   · 步骤进度成功日志降为 debug（高频、量大，会挤占日志截断窗口）。
 * 日志前缀「结果文件上传 / 任务字段更新 / 产物下载 / 产物 404」被 worker 的脱敏规则按字面匹配，请勿改动。
 */

const fs = require('node:fs')
const http = require('node:http')
const https = require('node:https')
const path = require('node:path')
const { pipeline } = require('node:stream/promises')
const E = require('../envnames')
const httpClient = require('../httpClient')
const { getLogger } = require('../log')
const { sendReport } = require('./ipcClient')
const { sleep, envStr, isTransient, bodySnippet } = require('./util')

const logger = getLogger('sdk.resultClient')

// ===== 重试策略 =====
const REPORT_MAX_ATTEMPTS = 5
const PROGRESS_MAX_ATTEMPTS = 2
const PROGRESS_TIMEOUT_SEC = 8.0
const UPLOAD_MAX_ATTEMPTS = 5
// 只防「一次连不上就抱着 Lane 干等」；gohttp 的超时是无数据空闲超时，不是总时长，大文件只要有进展不会超时
const UPLOAD_TIMEOUT_SEC = 300.0
const STREAM_IDLE_SEC = 120.0 // 相邻两个数据块之间的空闲超时，不是总时长
const STREAM_MAX_ATTEMPTS = 5

/** 退避基数（秒）。测试可改小；生产保持默认 */
const _tuning = { reportBackoffBase: 0.2, uploadBackoffBase: 2.0, streamBackoffBase: 2.0 }

// 业务规则拒绝（不重试，直接抛）——重试也不会变好（sub_step 守门不匹配、任务已迁出等）
const BUSINESS_REJECT_CODES = new Set([400, 404])
// 这些状态码拿到就返回给上层判断，不再重试（200 成功；400/404 业务拒绝；401/403 确定性拒绝）
const NO_RETRY_CODES = new Set([200, 400, 404, 401, 403])

// fencing 403 的 reason（与 fencing.js 同值；SDK 不能依赖 fencing.js，它会拉进 config.js）
const REASON_ALREADY_FINAL = 'already_final'
const PERMANENT_REASONS = new Set(['not_lessee', 'gone', 'bad_lease_seq', 'missing_fencing_token'])

class HttpError extends Error {
  constructor(message, { status = 0, reason = '' } = {}) {
    super(message)
    this.name = 'HttpError'
    this.status = status
    this.reason = reason
  }
}

// ===== 环境 =====

/** 本子进程所属任务的 fencing header；缺省为空 = 无租约（手动跑脚本调试），此时不带 header，服务端会明确拒绝 */
function fencingHeaders() {
  const wid = envStr(E.ENV_WORKER_ID)
  const seq = envStr(E.ENV_LEASE_SEQ)
  if (!wid || !seq) return {}
  return { [E.HEADER_WORKER_ID]: wid, [E.HEADER_LEASE_SEQ]: seq }
}

/** 任务服务连接信息 */
function getConfig() {
  const baseUrl = envStr(E.ENV_WEB_BASE_URL)
  const token = envStr(E.ENV_INTERNAL_TOKEN)
  if (!baseUrl || !token) {
    throw envError(`缺少必要的环境变量: ${E.ENV_WEB_BASE_URL} 和 ${E.ENV_INTERNAL_TOKEN}。这些变量由 Worker executor 在启动子进程时注入。`)
  }
  return { baseUrl, token }
}

function envError(msg) {
  const e = new Error(msg)
  e.name = 'EnvironmentError'
  return e
}

/** 请求 storage 用的 Token：专属优先、回退主 Token（与父进程 callback.storage_token / uploader 同一条链） */
const storageToken = (fallback) => envStr(E.ENV_STORAGE_INTERNAL_TOKEN) || fallback

/** worker 实际请求存储服务的基础 URL：内网 → 对外。没有回退到任务服务的路径，都没配直接报错 */
function getStorageAccessUrl() {
  const url = envStr(E.ENV_STORAGE_INTERNAL_URL) || envStr(E.ENV_STORAGE_URL)
  if (!url) {
    throw envError(`未配置存储服务地址: ${E.ENV_STORAGE_INTERNAL_URL} / ${E.ENV_STORAGE_URL}。产物只能上传到存储服务，没有回退路径。`)
  }
  return url
}

const enc = encodeURIComponent
const trimSlash = (u) => u.replace(/\/+$/, '')

/** 把 result_path 里的 download_url（相对路径）解析成指向存储服务【内网】地址的绝对 URL；已是绝对 URL 原样返回 */
function resolveStorageUrl(downloadUrl) {
  if (!downloadUrl) return downloadUrl
  if (/^https?:\/\//.test(downloadUrl)) return downloadUrl
  const access = envStr(E.ENV_STORAGE_INTERNAL_URL) || envStr(E.ENV_STORAGE_URL)
  return access ? `${trimSlash(access)}/${downloadUrl.replace(/^\/+/, '')}` : downloadUrl
}

/** 产物文件名必须是单段：不含 / 或 \，不是空串 / . / ..，不含 NUL。在客户端入口就响亮失败 */
function checkResultFilename(name) {
  if (!name || name === '.' || name === '..' || name.includes('/') || name.includes('\\') || name.includes('\0')) {
    throw new Error(`非法产物文件名（不得为空，不得含路径分隔符）: ${JSON.stringify(name)}`)
  }
  return name
}

// ===== PATCH 通道（steps / fields）=====

/**
 * 前台重试请求。返回 {resp, lastErr}：
 *   resp 非空 = 拿到了不再重试的响应（200 / 400 / 404 / 401 / 403，由上层判断）；
 *   resp 为 null = 传输层故障或 5xx 试满 maxAttempts 次（lastErr 为最后一个错误）。
 */
async function sendWithRetry(method, url, payload, token, { timeout = 30.0, maxAttempts = REPORT_MAX_ATTEMPTS } = {}) {
  const headers = { [E.HEADER_INTERNAL_TOKEN]: token, ...fencingHeaders() }
  let lastErr = null
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const resp = await httpClient.request(method, url, { headers, body: payload, timeout })
      if (resp && NO_RETRY_CODES.has(resp.status)) return { resp, lastErr: null }
      const status = resp ? resp.status : 0
      lastErr = new HttpError(`HTTP ${status}: ${bodySnippet(resp)}`, { status })
    } catch (e) {
      if (!isTransient(e)) throw e
      lastErr = e
    }
    if (attempt < maxAttempts - 1) await sleep(_tuning.reportBackoffBase * 2 ** attempt)
  }
  return { resp: null, lastErr }
}

/**
 * 解释 PATCH 的结果。返回 {kind: 'ok', json} | {kind: 'final'} | {kind: 'retain', why}；业务/租约拒绝直接抛。
 * 'retain' = 暂时无法送达（5xx 耗尽 / 网络 / 401 / 不明 403），由调用方决定转 IPC 还是丢弃。
 */
function interpret(resp, lastErr, label, taskId) {
  if (resp && resp.status === 200) return { kind: 'ok', json: httpClient.jsonOf(resp, {}) }
  if (resp && BUSINESS_REJECT_CODES.has(resp.status)) {
    logger.error(`${label}被业务拒绝（不入队）: task_id=${taskId}, status=${resp.status}, body=${bodySnippet(resp)}`)
    throw new HttpError(`更新失败 (HTTP ${resp.status}): ${bodySnippet(resp)}`, { status: resp.status })
  }
  if (resp && resp.status === 403) {
    const body = httpClient.jsonOf(resp, null)
    const reason = body && typeof body === 'object' ? String(body.reason || '') : ''
    if (reason === REASON_ALREADY_FINAL) {
      logger.info(`${label}：任务已达终态（幂等成功），丢弃本次上报: task_id=${taskId}`)
      return { kind: 'final' }
    }
    if (PERMANENT_REASONS.has(reason)) {
      logger.warning(`${label}被拒（${reason}），不重试不入队: task_id=${taskId}`)
      throw new HttpError(`更新被拒 (HTTP 403 ${reason})`, { status: 403, reason })
    }
  }
  const why = resp ? `HTTP ${resp.status}: ${bodySnippet(resp)}` : String(lastErr && lastErr.message)
  return { kind: 'retain', why }
}

/**
 * 更新任务字段 → PATCH /internal/tasks/{id}/fields[?mode=merge]
 * @param {string} taskId
 * @param {{resultPath?: string|object, logPath?: string|object, mode?: 'overwrite'|'merge'}} [opts]
 *   resultPath / logPath 为 null/undefined = 不更新；mode 'merge' 按 key 深度合并
 * @returns 服务端响应；暂时送不达则转 IPC 并返回占位 {task_id, message}
 */
async function updateTaskFields(taskId, { resultPath, logPath, mode = 'overwrite' } = {}) {
  const { baseUrl, token } = getConfig()
  let url = `${baseUrl}/internal/tasks/${enc(taskId)}/fields`
  if (mode === 'merge') url += '?mode=merge'

  const payload = {}
  if (resultPath != null) payload.result_path = resultPath
  if (logPath != null) payload.log_path = logPath
  if (Object.keys(payload).length === 0) {
    logger.warning(`无字段需更新: task_id=${taskId}`)
    return { task_id: taskId, message: 'No fields to update' }
  }

  const { resp, lastErr } = await sendWithRetry('PATCH', url, payload, token)
  const r = interpret(resp, lastErr, '任务字段更新', taskId)
  if (r.kind === 'ok') {
    logger.info(`任务字段更新成功: task_id=${taskId}, mode=${mode}, fields=${JSON.stringify(Object.keys(payload))}`)
    return r.json
  }
  if (r.kind === 'final') return { task_id: taskId, message: 'Already final' }

  logger.warning(`任务字段更新失败，转交父进程异步重试: task_id=${taskId}, 原因=${r.why}`)
  await sendReport('update_task_fields', { ...payload, _mode: mode }, { taskId }) // _mode 给 PendingReporter 用
  return { task_id: taskId, message: 'Queued for async retry' }
}

/**
 * 更新步骤进度 → PATCH /internal/tasks/{id}/steps
 * @param {string} taskId
 * @param {object} [opts]
 * @param {string[]} [opts.steps]        已完成步骤列表（覆盖写入）
 * @param {string} [opts.subStep]        当前步骤名
 * @param {string} [opts.subStatus]      pending / running / success / failed
 * @param {string} [opts.stepMessage]    动态消息（服务端要求同时传 subStep 且匹配）
 * @param {number} [opts.maxAttempts]    前台重试次数。承重的状态迁移用默认值；进度消息传 PROGRESS_MAX_ATTEMPTS
 * @param {number} [opts.timeout]        单次请求超时（秒）
 * @param {boolean} [opts.enqueueOnFailure]  失败后是否转 IPC。状态迁移必须 true；纯进度消息必须 false
 */
async function updateStepStatus(taskId, {
  steps, subStep, subStatus, stepMessage,
  maxAttempts = REPORT_MAX_ATTEMPTS, timeout = 30.0, enqueueOnFailure = true,
} = {}) {
  const { baseUrl, token } = getConfig()
  const url = `${baseUrl}/internal/tasks/${enc(taskId)}/steps`

  const payload = {}
  if (steps != null) payload.steps = steps
  if (subStep != null) payload.sub_step = subStep
  if (subStatus != null) payload.sub_status = subStatus
  if (stepMessage != null) payload.step_message = stepMessage

  const { resp, lastErr } = await sendWithRetry('PATCH', url, payload, token, { timeout, maxAttempts })
  const r = interpret(resp, lastErr, '步骤进度更新', taskId)
  if (r.kind === 'ok') {
    logger.debug(`步骤进度更新成功: task_id=${taskId}, payload=${JSON.stringify(payload)}`)
    return r.json
  }
  if (r.kind === 'final') return { task_id: taskId, message: 'Already final' }

  if (!enqueueOnFailure) {
    // 纯进度消息：失败即弃（过期进度补发是倒退，不是补救）
    logger.debug(`步骤进度消息上报失败（已丢弃，不入队）: task_id=${taskId}, 原因=${r.why}`)
    return { task_id: taskId, message: 'Progress message dropped' }
  }
  logger.warning(`步骤状态更新 ${maxAttempts} 次重试失败，转交父进程异步重试: task_id=${taskId}, 原因=${r.why}`)
  await sendReport('update_step_status', payload, { taskId })
  return { task_id: taskId, message: 'Queued for async retry' }
}

// ===== 上传 =====

/**
 * 上传结果文件到存储服务 → POST {存储}/internal/tasks/<id>/results（multipart，201）。
 * 实际请求走【内网】地址，落库的 download_url 用【对外】地址。5 次退避重试；4xx 业务拒绝立即抛。
 * 超大文件应走 uploader 的分片续传（见 uploadRoute）。
 * @returns 服务端 JSON（含 download_url）
 */
async function uploadResult(taskId, filePath, filename) {
  const localPath = path.resolve(String(filePath))
  const targetName = checkResultFilename(filename || path.basename(localPath)) // 入参校验先于任何配置 / I/O
  const baseUrl = getStorageAccessUrl()
  const token = storageToken(envStr(E.ENV_INTERNAL_TOKEN))
  if (!token) throw envError(`缺少必要的环境变量: ${E.ENV_INTERNAL_TOKEN}（或 ${E.ENV_STORAGE_INTERNAL_TOKEN}）`)
  if (!fs.existsSync(localPath)) {
    const e = new Error(`文件不存在: ${localPath}`)
    e.code = 'ENOENT'
    throw e
  }

  const url = `${trimSlash(baseUrl)}/internal/tasks/${enc(taskId)}/results`
  // 产物是任务的全部价值：一次抖动就丢，本地副本又会被清掉 → 永久丢失，只能整步重跑。
  // gohttp 的 part 文件名取本地 basename，所以【总是】带表单字段 filename 指定远端名。
  let resp = null
  let lastErr = null
  for (let attempt = 0; attempt < UPLOAD_MAX_ATTEMPTS; attempt++) {
    try {
      resp = await httpClient.upload(url, {
        files: { file: localPath },
        form: { filename: targetName },
        headers: { [E.HEADER_INTERNAL_TOKEN]: token },
        timeout: UPLOAD_TIMEOUT_SEC,
      })
      if (resp && (resp.status === 200 || resp.status === 201)) break
      if (resp && resp.status >= 400 && resp.status < 500) break // 业务拒绝：重试无意义
      lastErr = new HttpError(`HTTP ${resp ? resp.status : 0}: ${bodySnippet(resp)}`) // 5xx / 状态 0 → 重试
      resp = null
    } catch (e) {
      if (!isTransient(e)) throw e
      lastErr = e
      resp = null
    }
    if (attempt < UPLOAD_MAX_ATTEMPTS - 1) {
      const wait = _tuning.uploadBackoffBase * 2 ** attempt
      logger.warning(`结果文件上传失败，${wait.toFixed(0)}s 后重试 (${attempt + 1}/${UPLOAD_MAX_ATTEMPTS}): file=${targetName}, error=${lastErr && lastErr.message}`)
      await sleep(wait)
    }
  }

  if (!resp) {
    logger.error(`结果文件上传失败（${UPLOAD_MAX_ATTEMPTS} 次重试均失败）: task_id=${taskId}, file=${targetName}, error=${lastErr && lastErr.message}`)
    throw new HttpError(`上传失败（网络层，${UPLOAD_MAX_ATTEMPTS} 次重试）: ${lastErr && lastErr.message}`)
  }
  if (resp.status !== 200 && resp.status !== 201) {
    logger.error(`结果文件上传失败: task_id=${taskId}, file=${targetName}, status=${resp.status}, body=${bodySnippet(resp)}`)
    throw new HttpError(`上传失败 (HTTP ${resp.status}): ${bodySnippet(resp)}`, { status: resp.status })
  }

  const result = httpClient.jsonOf(resp, null)
  if (!result || typeof result !== 'object') throw new HttpError('上传失败：存储服务响应不是 JSON', { status: resp.status })

  // 落库/前端用的 download_url 恒用【对外】地址拼接，与上面请求用的内网地址严格解耦；未配对外地址则保持相对路径
  const publicUrl = envStr(E.ENV_STORAGE_URL)
  const rel = result.download_url
  if (rel && !/^https?:\/\//.test(rel) && publicUrl) {
    result.download_url = `${trimSlash(publicUrl)}/${String(rel).replace(/^\/+/, '')}`
  }
  logger.info(`结果文件上传成功: task_id=${taskId}, file=${targetName}, download_url=${result.download_url}`)
  return result
}

// ===== 下载 =====

const publicResultUrl = (taskId, filename) =>
  `${trimSlash(getStorageAccessUrl())}/public/tasks/${enc(taskId)}/results/${enc(filename)}`

/** 从存储服务下载结果文件（小文件，整份读进内存；大文件用 downloadArtifactStream）。公开路径无需认证。404/任何失败返回 false */
async function downloadArtifact(taskId, filename, savePath) {
  const url = publicResultUrl(taskId, filename)
  const localPath = path.resolve(String(savePath))
  fs.mkdirSync(path.dirname(localPath), { recursive: true })
  try {
    const resp = await httpClient.request('GET', url, { timeout: 300.0 })
    if (resp.status === 200) {
      fs.writeFileSync(localPath, resp.data)
      logger.debug(`产物下载成功: task_id=${taskId}, filename=${filename}`)
      return true
    }
    if (resp.status === 404) {
      logger.warning(`产物 404: task_id=${taskId}, filename=${filename}`)
      return false
    }
    logger.error(`产物下载失败: task_id=${taskId}, filename=${filename}, status=${resp.status}`)
    return false
  } catch (e) {
    logger.error(`产物下载异常: task_id=${taskId}, filename=${filename}, error=${e && e.message}`)
    return false
  }
}

/**
 * 列出任务 results 目录下的文件名（公开端点，无需认证）。
 * 请求失败返回 null —— 「不知道」与「确实为空」要分开，调用方据此决定是否回源。
 */
async function listArtifacts(taskId) {
  try {
    const url = `${trimSlash(getStorageAccessUrl())}/public/tasks/${enc(taskId)}/results`
    const resp = await httpClient.request('GET', url, { timeout: 30.0 })
    if (resp.status !== 200) {
      logger.warning(`列目录失败: task_id=${taskId}, status=${resp.status}`)
      return null
    }
    const body = httpClient.jsonOf(resp, null)
    return body && Array.isArray(body.files) ? body.files.map((f) => String(f)) : null
  } catch (e) { // 网络/解析异常一律当「不知道」，不让探测拖垮任务
    logger.warning(`列目录异常: task_id=${taskId}, error=${e && e.message}`)
    return null
  }
}

/** Content-Range（`bytes a-b/total` 或 `bytes 星号/total`）→ {start: 起点|null, total: 总长|null} */
function parseContentRange(value) {
  const m = /^\s*bytes\s+(\*|\d+)(?:-\d+)?\s*\/\s*(\*|\d+)\s*$/i.exec(String(value || ''))
  if (!m) return { start: null, total: null }
  return { start: m[1] === '*' ? null : parseInt(m[1], 10), total: m[2] === '*' ? null : parseInt(m[2], 10) }
}

/** node:http(s) GET；响应到达后交给 handler 消费，handler 返回值即本函数结果。空闲超时/连接错误抛出 */
function streamGet(urlStr, headers, idleSec, handler) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr)
    const lib = u.protocol === 'https:' ? https : http
    const req = lib.request(u, { method: 'GET', headers }, (res) => {
      Promise.resolve().then(() => handler(res)).then(resolve, reject)
    })
    req.setTimeout(idleSec * 1000, () => {
      const err = new Error('idle timeout')
      err.code = 'ETIMEDOUT'
      req.destroy(err)
    })
    req.on('error', reject)
    req.end()
  })
}

const fileSize = (p) => { try { return fs.statSync(p).size } catch (_) { return 0 } }
const unlinkQuiet = (p) => { try { fs.unlinkSync(p) } catch (_) { /* missing_ok */ } }

/**
 * 流式下载大文件：分块落盘、Range 断点续传、完成后原子改名。
 *   · 先写 `${savePath}.part`；重试 / 再次调用时从 .part 现有长度续传（Range: bytes=N-）；
 *   · 服务端忽略 Range 回 200 → 丢弃 .part 从头来；206 起点与本地长度不符 → 从头来；
 *     416 且总长恰等于本地长度 → 视为已完整；416 其余 → 丢弃重来；
 *   · 落盘长度与声明总长不符（被截断）算失败并重试；
 *   · 404 / 其他 4xx 不重试返回 false；5xx 与传输层异常指数退避重试；
 *   · 全部失败返回 false，保留 .part 供下次续传。
 * @param {{maxAttempts?: number, backoffBase?: number}} [opts]
 */
async function downloadArtifactStream(taskId, filename, savePath, {
  maxAttempts = STREAM_MAX_ATTEMPTS, backoffBase = _tuning.streamBackoffBase,
} = {}) {
  const url = publicResultUrl(taskId, filename)
  const finalPath = path.resolve(String(savePath))
  fs.mkdirSync(path.dirname(finalPath), { recursive: true })
  const part = `${finalPath}.part`

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const offset = fileSize(part)
    // identity：续传偏移按「传输字节」算，被 gzip 等内容编码会与落盘字节错位
    const headers = { 'Accept-Encoding': 'identity' }
    if (offset > 0) headers.Range = `bytes=${offset}-`
    try {
      const outcome = await streamGet(url, headers, STREAM_IDLE_SEC, async (res) => {
        const code = res.statusCode
        if (code === 404) {
          res.resume()
          logger.warning(`产物 404: task_id=${taskId}, filename=${filename}`)
          unlinkQuiet(part)
          return 'false'
        }
        if (code === 416) {
          res.resume()
          const { total } = parseContentRange(res.headers['content-range'])
          if (total !== null && total === offset) { fs.renameSync(part, finalPath); return 'true' }
          unlinkQuiet(part) // 本地残片与远端对不上：丢弃重来
          return 'again'
        }
        if (code === 200 || code === 206) {
          let total
          let flags
          if (code === 206) {
            const cr = parseContentRange(res.headers['content-range'])
            if (cr.start !== offset) { res.resume(); unlinkQuiet(part); return 'again' }
            total = cr.total
            flags = 'a'
          } else { // 200：服务端没按 Range 给，整份重来
            const cl = res.headers['content-length']
            total = cl && /^\d+$/.test(cl) ? parseInt(cl, 10) : null
            flags = 'w'
          }
          // 每收到一块就落盘，断线时丢得最少
          await pipeline(res, fs.createWriteStream(part, { flags }))
          const got = fileSize(part)
          if (total !== null && got !== total) throw new Error(`下载被截断: ${got}/${total}`)
          fs.renameSync(part, finalPath)
          return 'true'
        }
        res.resume()
        if (code >= 400 && code < 500) {
          logger.error(`产物下载被拒: ${filename}, status=${code}`)
          return 'false'
        }
        throw new HttpError(`HTTP ${code}`, { status: code }) // 5xx → 重试
      })
      if (outcome === 'true') return true
      if (outcome === 'false') return false
      continue // 'again'：不退避直接下一轮
    } catch (e) {
      logger.warning(`产物流式下载失败 (${attempt + 1}/${maxAttempts}): task_id=${taskId}, filename=${filename}, error=${e && e.message}`)
    }
    if (attempt < maxAttempts - 1 && backoffBase > 0) await sleep(backoffBase * 2 ** attempt)
  }
  return false
}

module.exports = {
  uploadResult, updateTaskFields, updateStepStatus,
  downloadArtifact, downloadArtifactStream, listArtifacts,
  resolveStorageUrl, checkResultFilename,
  HttpError, REPORT_MAX_ATTEMPTS, PROGRESS_MAX_ATTEMPTS, PROGRESS_TIMEOUT_SEC,
  _tuning,
}
