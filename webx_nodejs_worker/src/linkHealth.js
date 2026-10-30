'use strict'

/**
 * 内部回调链路的健康记账 —— 把「每任务一行 warning」聚合成「每故障一条 ERROR」。
 *
 * 为什么需要：worker 有两条彼此独立的通道（领取 / 回调）。证书、内部令牌、反代任意一环坏掉只断回调，
 * 领取照常——外在表现是任务收得到却全部无声地跑不起来。教训不是「没发现」而是「发现了只肯小声说」，
 * 所以确定性故障（鉴权 / TLS）要在这里聚合成一处响亮的 ERROR：首次立刻报，持续期间按
 * ALARM_INTERVAL_SEC 限流继续报（带累计次数与时长），恢复时 INFO 收尾，每条都列出可能原因。
 *
 * 只做旁路观测：不改变任何重试 / 放弃语义，摘掉不影响功能路径（httpClient.setObserver(null) 即可）。
 *
 * 与 Python 版的差异：
 *   · 观测挂在 httpClient 的 observer 上（Python 是包 httpx transport）。本模块加载时自动 installObserver()，
 *     于是领取 / 回调 / 下载 / 上传等所有经 httpClient 的请求都被观测；不经 httpClient 的出口（gohttp 直接下载）
 *     须自行调用导出的 observer.onResponse / observer.onError（见 httpFileClient.js）。
 *   · observer 只拿得到 URL（拿不到方法），故告警现象里没有 HTTP 方法；URL 的查询串会被剔除。
 *   · classifyException 适配 Node 错误：沿 err.cause / AggregateError.errors 找 TLS 错误码
 *     （UNABLE_TO_VERIFY_LEAF_SIGNATURE、CERT_*、DEPTH_ZERO_SELF_SIGNED_CERT、ERR_TLS_CERT_ALTNAME_INVALID、
 *     ERR_SSL_* 等），超时 / 取消 / 连接被拒 / DNS 失败一律视为瞬时故障。
 *   · checkWebLink 返回数组 [ok, kind, detail]；选项走第二个参数 {attempts, delaySec, timeout}（秒）。
 */

const aio = require('./aio')
const httpClient = require('./httpClient')
const { getLogger, stamp } = require('./log')

const logger = getLogger('link_health')

// 确定性故障：重试不会改善，必须人工介入
const KIND_AUTH = 'auth' // 401
const KIND_TLS = 'tls' // 证书链 / 主机名 / CA 校验失败
// 启动自检专用：连不上 / 应答不对。运行期不用它——连接被拒是瞬时故障，重试有意义
const KIND_UNREACHABLE = 'unreachable'

// 首次之后的持续告警间隔（秒）：既不刷屏，也不会安静超过一分钟
const ALARM_INTERVAL_SEC = 60.0

// 各类故障的排查提示 —— 按「最常见 → 最少见」排列
const HINTS = {
  [KIND_AUTH]: [
    'WEBX_WORKER_INTERNAL_TOKEN 与服务端配置的内部令牌是否逐字一致' +
      '（注意：同名项写两次时取【最后一行】；真实环境变量会【覆盖】.env 与覆盖层）',
    '反向代理是否透传了 X-Internal-Token 请求头',
    'WEBX_WORKER_WEB_BASE_URL 是否指向了别的服务（证书/SNI 不匹配时 nginx 会落到 default server）',
  ],
  [KIND_TLS]: [
    '服务端是否用了 fullchain 证书（只配叶子证书会缺中间 CA，客户端无法构建信任链）',
    '证书是否过期，或域名与 WEBX_WORKER_WEB_BASE_URL 不匹配',
    'worker 所在机器的 CA 根证书是否过旧（ca-certificates / NODE_EXTRA_CA_CERTS）',
  ],
  [KIND_UNREACHABLE]: [
    '任务服务是否已启动（启动顺序：服务端 → worker；worker 先起会探不通）',
    'WEBX_WORKER_WEB_BASE_URL 是否正确（协议/域名/端口）',
    '网络与防火墙是否放行，反向代理的 upstream 是否健康（502/504 多为此因）',
  ],
}

const fmtHints = (kind) => (HINTS[kind] || []).map((h, i) => `\n         ${i + 1}) ${h}`).join('')

class LinkHealth {
  /** Node 单线程且方法体内无 await，不存在交错，无需加锁 */
  constructor(alarmIntervalSec = ALARM_INTERVAL_SEC) {
    this.alarmIntervalSec = alarmIntervalSec
    this._kind = null // 当前故障类别，null = 健康
    this._count = 0 // 本轮故障累计次数
    this._firstMono = 0 // 本轮首次失败的单调时刻（秒）
    this._firstWall = 0 // 本轮首次失败的墙钟（毫秒，仅展示）
    this._lastAlarmMono = null // 上次打 ERROR 的单调时刻；null = 还没打过
  }

  /** 当前是否处于确定性故障中 */
  get failing() { return this._kind !== null }

  /** 本轮故障累计次数（恢复后归零） */
  get failureCount() { return this._count }

  /** 记录一次确定性故障；首次立刻 ERROR，之后按间隔限流继续 ERROR。 */
  noteFailure(kind, detail, { taskId = '' } = {}) {
    const now = aio.monotonic()
    // 换了一类故障 → 当作新一轮重新计数（避免 auth 的计数被 tls 继承）
    if (this._kind !== kind) {
      this._kind = kind
      this._count = 0
      this._firstMono = now
      this._firstWall = Date.now()
      this._lastAlarmMono = null
    }
    this._count += 1

    const due = this._lastAlarmMono === null || (now - this._lastAlarmMono) >= this.alarmIntervalSec
    if (!due) return

    this._lastAlarmMono = now
    const elapsed = Math.floor(now - this._firstMono)
    const sample = taskId ? ` 样本 task=${taskId}` : ''
    logger.error(
      `⛔ 内部回调链路持续失败（${kind}）：累计 ${this._count} 次，自 ${stamp(new Date(this._firstWall))} 起已 ${elapsed}s${sample}\n` +
      `         现象: ${detail}\n` +
      '         注意: 领取通道与本通道相互独立——能领到任务不代表回调可用。\n' +
      `         请依次检查:${fmtHints(kind)}`)
  }

  /**
   * 记录一次成功往返；若此前处于故障中，打一条恢复日志并归零。
   * 「成功」= 服务端给出了业务语义的应答（200/400/403/404 都算），而不只是没抛异常。
   */
  noteSuccess() {
    if (this._kind === null) return
    const elapsed = Math.floor(aio.monotonic() - this._firstMono)
    logger.info(
      `✅ 内部回调链路已恢复（此前 ${this._kind} 故障，累计失败 ${this._count} 次，持续约 ${elapsed}s）` +
      '；积压的状态上报将由 PendingReporter 自动回放，无需重启 worker。')
    this._kind = null
    this._count = 0
  }
}

// 全局单例：worker 单进程单事件循环
const linkHealth = new LinkHealth()

// 瞬时故障的错误码：命中即不看文本（消息里常带主机名，主机名含 "ssl" 之类会误判）
const TRANSIENT_CODES = new Set([
  'ECONNREFUSED', 'ECONNRESET', 'ECONNABORTED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH',
  'ETIMEDOUT', 'EPIPE', 'WEBX_TIMEOUT', 'ABORT_ERR', 'ERR_TLS_HANDSHAKE_TIMEOUT',
])
const TLS_CODES = new Set([
  'UNABLE_TO_GET_ISSUER_CERT', 'UNABLE_TO_GET_ISSUER_CERT_LOCALLY', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'DEPTH_ZERO_SELF_SIGNED_CERT', 'SELF_SIGNED_CERT_IN_CHAIN', 'HOSTNAME_MISMATCH', 'EPROTO',
])
const TLS_CODE_PREFIXES = ['CERT_', 'ERR_TLS_', 'ERR_SSL_', 'ERR_OSSL_']

/** 沿 cause / AggregateError.errors 展开错误链（防环） */
function* errorChain(err) {
  const seen = new Set()
  const stack = [err]
  while (stack.length) {
    const cur = stack.pop()
    if (!cur || typeof cur !== 'object' || seen.has(cur)) continue
    seen.add(cur)
    yield cur
    if (cur.cause) stack.push(cur.cause)
    if (Array.isArray(cur.errors)) stack.push(...cur.errors)
  }
}

/**
 * 把异常归类为确定性故障；返回 null 表示「瞬时故障，重试有意义」。
 * TLS 判据首选错误码（沿 cause 链找），兜底再看文本（覆盖被字符串化掉的情况）。
 */
function classifyException(err) {
  if (!err) return null
  if (httpClient.isTimeout(err) || httpClient.isAbort(err)) return null
  for (const e of errorChain(err)) {
    const code = String(e.code || '')
    if (TLS_CODES.has(code) || TLS_CODE_PREFIXES.some((p) => code.startsWith(p) && !TRANSIENT_CODES.has(code))) {
      return KIND_TLS
    }
  }
  for (const e of errorChain(err)) {
    if (TRANSIENT_CODES.has(String(e.code || ''))) return null
  }
  const text = String((err && err.message) || err).toLowerCase()
  if (text.includes('certificate') || text.includes('ssl') || text.includes('tlsv1')) return KIND_TLS
  return null
}

/**
 * 把 HTTP 状态码归类为确定性故障；返回 null 表示不属于本模块管辖。
 *
 * 只认 401，不认 403（依据 docs/worker-protocol.md §2）：内部鉴权失败恒为 401；
 * 403 是业务/安全拒绝（fencing 的 not_lessee / gone / already_final / missing_fencing_token、路径守卫），
 * 客户端不应重试，也不应触发全局鉴权告警。把 403 算成鉴权故障，一次正常的租约丢失就会让运维去核对令牌——
 * 「大声地指错方向」。若将来协议把鉴权失败改为 403，必须同步修改此处。
 */
const classifyStatus = (status) => (status === 401 ? KIND_AUTH : null)

// 启动自检用的哨兵任务 ID：GET /internal/tasks/<id> 对不存在的任务回 404（协议 §4.10），
// 因此探测纯只读，不改任何真实任务，也不依赖写端点的 fencing。
// 取「字母数字、长度 15」：保守沿用一个合规形状（见 docs/worker.md 已知限制）。
const PROBE_TASK_ID = 'probe0000000000'
const PROBE_OK_CODES = [200, 404]

const withoutQuery = (url) => String(url).split('?')[0]

/** 现象文本：错误名 + 消息（消息里没有错误码时补上，TLS 类错误尤其需要） */
function describeError(err) {
  const name = (err && err.name) || 'Error'
  const msg = String((err && err.message) || err)
  const code = err && err.code
  return code && !msg.includes(code) ? `${name}: ${msg} (${code})` : `${name}: ${msg}`
}

/** httpClient 的观测器：旁路记账，永不抛 */
const observer = {
  onResponse(res, url) {
    const status = res && res.status
    const kind = classifyStatus(status)
    if (kind !== null) linkHealth.noteFailure(kind, `HTTP ${status} ${withoutQuery(url)}`)
    else linkHealth.noteSuccess() // 服务端给出了业务语义应答（含 400/403/404）→ 链路本身是通的
  },
  onError(err, url) {
    const kind = classifyException(err)
    // 瞬时故障（超时/连接重置/5xx）不归本模块管，交给各调用点的重试逻辑
    if (kind !== null) linkHealth.noteFailure(kind, `${describeError(err)} — ${withoutQuery(url)}`)
  },
}

const installObserver = () => httpClient.setObserver(observer)
const uninstallObserver = () => httpClient.setObserver(null)
installObserver() // 加载即挂接：谁引入本模块，谁的请求就被观测

/**
 * 启动自检：把内部回调链路在收到第一个任务之前探通一次。
 *
 * 探测路径刻意选 /internal/tasks/{id} 而非 /health：它与真实回调同 base_url、同 TLS、同 token、同鉴权关卡，
 * 「探测通过」才能代表「回调可用」。耗时有界：逐请求 timeout 默认 5s；确定性故障（401 / TLS）立即返回不重试，
 * 只有「不可达」才重试（对应启动顺序竞态），最坏 3×5 + 2×3 = 21s，调用方应放后台跑（selfcheckAndReport）。
 *
 * @param callback 需提供 baseUrl 与 request(method, url, payload, {timeout})
 * @returns [ok, kind, detail]  kind 由分类函数判定后带出（成功时 null），展示层不得再从 detail 文本猜类别
 */
async function checkWebLink(callback, { attempts = 3, delaySec = 3.0, timeout = 5.0 } = {}) {
  const url = `${callback.baseUrl}/internal/tasks/${PROBE_TASK_ID}`
  let kind = KIND_UNREACHABLE
  let detail = ''
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const res = await callback.request('GET', url, null, { timeout })
      if (PROBE_OK_CODES.includes(res.status)) return [true, null, '']
      detail = `HTTP ${res.status}（期望 404 或 200）`
      // 401 → 鉴权；403/5xx/意外 2xx-3xx → 服务端可达但应答不对
      kind = classifyStatus(res.status) || KIND_UNREACHABLE
    } catch (err) { // 连接/TLS/超时
      detail = describeError(err)
      kind = classifyException(err) || KIND_UNREACHABLE
    }
    // 确定性故障重试没有意义，立刻返回，不占启动时间
    if (kind === KIND_AUTH || kind === KIND_TLS) return [false, kind, detail]
    if (attempt < attempts) await aio.sleep(delaySec)
  }
  return [false, kind, detail]
}

/** 跑一次启动自检并打横幅，供调用方放到后台任务里执行（启动路径零延迟） */
async function selfcheckAndReport(callback) {
  const [ok, kind, detail] = await checkWebLink(callback)
  logLinkCheckBanner(ok, kind, detail, callback.baseUrl)
  return ok
}

/** 把自检结果打成一眼可见的横幅；失败时按类别列出排查清单（类别由 checkWebLink 带入，不从文本反推） */
function logLinkCheckBanner(ok, kind, detail, baseUrl) {
  if (ok) {
    logger.info(`[自检] 内部回调链路正常 → ${baseUrl}`)
    return
  }
  const bar = '='.repeat(72)
  logger.error(
    `\n${bar}\n` +
    '  ⛔ 内部回调链路自检失败 —— worker 仍会启动，但**领到的任务多半跑不起来**\n' +
    `${bar}\n` +
    `  目标: ${baseUrl}\n` +
    `  现象: ${detail}\n` +
    '  说明: 这条通道与领取通道相互独立；能领到任务也不代表回调可用。\n' +
    `  请检查:${fmtHints(kind || '')}\n` +
    bar)
}

module.exports = {
  KIND_AUTH, KIND_TLS, KIND_UNREACHABLE, ALARM_INTERVAL_SEC, PROBE_TASK_ID, HINTS,
  LinkHealth, linkHealth, classifyException, classifyStatus, describeError,
  checkWebLink, selfcheckAndReport, logLinkCheckBanner,
  observer, installObserver, uninstallObserver,
}
