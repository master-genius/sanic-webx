'use strict'

/**
 * `webx-worker doctor` —— 部署自检：对【真实服务】验证这台机器能不能当 worker。
 *
 * 在把 worker 交给 systemd 之前跑一遍，把「配置对不对、网通不通、令牌有没有效」这类只会在
 * 任务投递之后才暴露的问题提前到现在。**不会领取任何任务、不会改任何数据**：
 *
 *   channel 配置      能加载；CHANNELS 非空；PLUGIN 类型对；列出 Lane 与能力声明
 *   channel 脚本/解释器  脚本存在；.js 用当前 node（process.execPath），.sh 用 bash，
 *                    其余按 channel 的 env / executor（conda / uv / 指定解释器须在 PATH 上）
 *   工作目录          能创建、可写
 *   令牌              已配置（空令牌 = 服务端拒绝一切请求）
 *   领取通道          POST /task/leases 发【空 slots】：400 = 可达且鉴权通过；401 = 令牌错；
 *                    404 = 该地址没有这条路由（反代未转发）。空 slots 永远领不到任务，探测安全
 *   回调通道          GET /internal/tasks/<哨兵>：404（或 200）= 通；401 = 令牌错
 *   存储服务（已配置时） GET /internal/uploads/<哨兵>：404 = 通；401 = 令牌错
 *
 * 退出码：0 = 全部通过（可有警告）；1 = 有失败项；2 = 参数错误。
 *
 * 与 Python 版的差异：不改 process.env（--home 直接作为参数传给 resolveConfigPath / loadBaseEnv）；
 * HTTP 经 httpClient（连接失败抛原错误，超时抛 HttpTimeoutError）；.js 脚本的解释器检查对象是
 * process.execPath 而不是 PATH 上的 node。
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { parseArgs } = require('node:util')
const E = require('./envnames')
const cfg = require('./config')
const { buildCmdPrefix, loadChannelConfig, resolveConfigPath } = require('./channelLoader')
const { LaneManager } = require('./laneManager')
const { request } = require('./httpClient')
const { PROBE_TASK_ID, classifyException, classifyStatus, installObserver, uninstallObserver } = require('./linkHealth')

const { settings } = cfg

const OK = 'ok'
const WARN = 'warn'
const FAIL = 'fail'
const MARK = { [OK]: '[通过]', [WARN]: '[警告]', [FAIL]: '[失败]' }

class Report {
  constructor() { this.items = [] }

  add(level, name, detail = '') {
    this.items.push([level, name, detail])
    console.log(`  ${MARK[level]} ${name}${detail ? ` —— ${detail}` : ''}`)
  }

  get failed() { return this.items.some(([lv]) => lv === FAIL) }

  summary() {
    const n = (lv) => this.items.filter(([x]) => x === lv).length
    return `通过 ${n(OK)} 项，警告 ${n(WARN)} 项，失败 ${n(FAIL)} 项`
  }
}

// ───────────────────────── 单项检查 ─────────────────────────

function classifyConnError(err) {
  if (classifyException(err) === 'tls') {
    return ['TLS/证书问题', '检查服务端是否用了 fullchain 证书、域名是否与地址一致、本机 CA 是否过旧']
  }
  return ['连接失败', '检查服务是否已启动、地址（协议/域名/端口）是否正确、防火墙与反代 upstream']
}

function errText(err) {
  // AggregateError（如 localhost 解析出多个地址全部连不上）自身 message 为空，取子错误的
  let msg = (err && err.message) || ((err && err.errors) || []).map((e) => e.message).filter(Boolean).join('; ')
  if (!msg) msg = String(err)
  return `${err && err.name ? err.name : 'Error'}${err && err.code ? `(${err.code})` : ''}: ${msg}`
}

/** 发请求并把「网络层失败」统一报成 FAIL；成功返回状态码，失败返回 null */
async function probe(rep, label, method, url, opts) {
  try {
    return (await request(method, url, opts)).status
  } catch (err) {
    const [what, hint] = classifyConnError(err)
    rep.add(FAIL, label, `${what}: ${errText(err)} ← ${url}（${hint}）`)
    return null
  }
}

/** 对 /task/leases 发空 slots：不可能领到任务，却足以区分「路由 / 鉴权 / 可达」三类故障 */
async function checkLeaseChannel(rep, base, timeout) {
  const url = `${base.replace(/\/+$/, '')}/task/leases`
  const c = await probe(rep, '领取通道', 'POST', url, {
    headers: { [E.HEADER_INTERNAL_TOKEN]: settings.INTERNAL_TOKEN, [E.HEADER_WORKER_ID]: settings.WORKER_ID },
    body: { worker_id: settings.WORKER_ID, slots: [] },
    timeout,
  })
  if (c === null) return
  if (c === 400) {
    rep.add(OK, '领取通道', `可达且令牌有效（空 slots 被服务端按预期以 400 拒绝）← ${url}`)
  } else if (c === 401) {
    rep.add(FAIL, '领取通道', `401：令牌错误 —— 核对 ${E.ENV_INTERNAL_TOKEN} 与服务端配置、反代是否透传 X-Internal-Token ← ${url}`)
  } else if (c === 404) {
    rep.add(FAIL, '领取通道', `404：该地址上没有 /task/leases —— 多半是反代未转发，或 WEBX_WORKER_TASK_SERVER_URL 为空而回落到了 ${base} ← ${url}`)
  } else if (c === 200) {
    rep.add(WARN, '领取通道', '空 slots 竟被接受（200）：服务端校验比协议宽松，请核对协议实现')
  } else {
    rep.add(FAIL, '领取通道', `意外状态码 ${c}（期望 400）← ${url}`)
  }
}

async function checkCallbackChannel(rep, base, timeout) {
  const url = `${base.replace(/\/+$/, '')}/internal/tasks/${PROBE_TASK_ID}`
  const c = await probe(rep, '回调通道', 'GET', url, { headers: { [E.HEADER_INTERNAL_TOKEN]: settings.INTERNAL_TOKEN }, timeout })
  if (c === null) return
  if (c === 404 || c === 200) rep.add(OK, '回调通道', `可达且令牌有效（哨兵任务回 ${c}）← ${url}`)
  else if (classifyStatus(c)) rep.add(FAIL, '回调通道', `${c}：令牌错误 ← ${url}`)
  else rep.add(FAIL, '回调通道', `意外状态码 ${c}（期望 404）← ${url}`)
}

async function checkStorage(rep, timeout) {
  const base = settings.STORAGE_INTERNAL_URL.trim() || settings.STORAGE_URL.trim()
  if (!base) {
    rep.add(WARN, '存储服务', `未配置（${E.ENV_STORAGE_INTERNAL_URL} / ${E.ENV_STORAGE_URL}）：产物与步骤日志将无处可传，uploader 不会拉起`)
    return
  }
  const token = settings.STORAGE_INTERNAL_TOKEN.trim() || settings.INTERNAL_TOKEN
  const url = `${base.replace(/\/+$/, '')}/internal/uploads/${PROBE_TASK_ID}`
  const c = await probe(rep, '存储服务', 'GET', url, { headers: { [E.HEADER_INTERNAL_TOKEN]: token }, timeout })
  if (c === null) return
  if (c === 404 || c === 200) rep.add(OK, '存储服务', `可达且令牌有效（哨兵会话回 ${c}）← ${url}`)
  else if (c === 401) rep.add(FAIL, '存储服务', `401：令牌错误 —— 核对 ${E.ENV_STORAGE_INTERNAL_TOKEN}（未配则回退主令牌）← ${url}`)
  else rep.add(FAIL, '存储服务', `意外状态码 ${c}（期望 404）；反代是否转发了 /internal ← ${url}`)
}

function checkWorkdir(rep) {
  const wd = path.resolve(settings.WORK_DIR.startsWith('~') ? path.join(os.homedir(), settings.WORK_DIR.slice(1)) : settings.WORK_DIR)
  const probeFile = path.join(wd, `.doctor-${process.pid}-${Date.now()}`)
  try {
    fs.mkdirSync(wd, { recursive: true })
    fs.writeFileSync(probeFile, 'ok')
    rep.add(OK, '工作目录', `可写 ← ${wd}`)
  } catch (err) {
    rep.add(FAIL, '工作目录', `不可写: ${err.message} ← ${wd}`)
  } finally {
    try { fs.unlinkSync(probeFile) } catch (_) { /* 没写成功就没有可删的 */ }
  }
}

/** 等价 shutil.which：含路径分隔符按路径判断，否则扫 PATH；要求可执行 */
function isExecutable(exe) {
  const ok = (p) => { try { fs.accessSync(p, fs.constants.X_OK); return fs.statSync(p).isFile() } catch (_) { return false } }
  if (exe.includes(path.sep)) return ok(path.resolve(exe))
  return (process.env.PATH || '').split(path.delimiter).filter(Boolean).some((d) => ok(path.join(d, exe)))
}

function checkChannels(rep, conf) {
  const channels = conf.CHANNELS
  const lm = new LaneManager(conf.LANES, Object.keys(channels))
  for (let i = 0; i < lm.numLanes; i++) {
    rep.add(OK, `Lane ${i}`, `能力声明 = ${JSON.stringify(lm.typesOf(i))}（一条 Lane 同时最多跑一个任务）`)
  }
  for (const t of Object.keys(channels)) {
    if (!lm.taskToLane.has(t)) rep.add(WARN, `channel ${t}`, '已声明但不在任何 Lane 里，永远领不到该类型的任务')
  }
  for (const [t, c] of Object.entries(channels)) {
    const script = c && c.script
    if (!script) { rep.add(FAIL, `channel ${t}`, '缺少 script 字段'); continue }
    const sp = path.resolve(conf.SCRIPT_ROOT, script)
    if (!fs.existsSync(sp)) { rep.add(FAIL, `channel ${t}`, `脚本不存在: ${sp}`); continue }
    let prefix
    try { prefix = buildCmdPrefix(c) } catch (err) { rep.add(FAIL, `channel ${t}`, err.message); continue }
    const ext = path.extname(script)
    let exe
    if (['.js', '.cjs', '.mjs'].includes(ext)) exe = process.execPath
    else if (ext === '.sh') exe = 'bash'
    else [exe] = prefix
    if (isExecutable(exe)) rep.add(OK, `channel ${t}`, `脚本存在，解释器可用（${exe}）`)
    else rep.add(FAIL, `channel ${t}`, `解释器不在 PATH 上: ${exe}`)
  }
}

// ───────────────────────── 入口 ─────────────────────────

const ARG_OPTIONS = {
  home: { type: 'string' },
  config: { type: 'string' },
  'ext-env': { type: 'string' },
  'work-dir': { type: 'string' },
  timeout: { type: 'string' },
  'no-storage': { type: 'boolean', default: false },
}

const expandHome = (p) => path.resolve(p.startsWith('~') ? path.join(os.homedir(), p.slice(1)) : p)

/**
 * @returns {Promise<number>} 退出码 0 / 1 / 2
 * 探测期间摘掉 linkHealth 的旁路观测：doctor 的探测失败是它要报告的结果，不应触发 worker 的全局链路告警
 */
async function main(argv = []) {
  uninstallObserver()
  try {
    return await mainImpl(argv)
  } finally {
    installObserver()
  }
}

async function mainImpl(argv) {
  const { values } = parseArgs({ args: argv, options: ARG_OPTIONS, strict: false })
  const str = (v) => (typeof v === 'string' && v ? v : undefined)
  const timeout = values.timeout === undefined ? 10 : Number(values.timeout)
  if (!Number.isFinite(timeout) || timeout <= 0) {
    console.error(`doctor: --timeout 必须是正数（秒），收到 ${JSON.stringify(values.timeout)}`)
    return 2
  }

  const home = str(values.home) ? expandHome(values.home) : cfg.workerHome()
  cfg.resetFileLayers() // 清掉同进程里上一轮加载的文件层，并按当前环境变量重建配置
  const rep = new Report()
  console.log(`webx_worker doctor —— 主目录 ${home}\n`)

  // —— 配置加载（与 run 同序：.env → 覆盖层 → worker_id → 命令行覆盖）——
  const baseLoaded = cfg.loadBaseEnv(path.join(home, '.env'))
  rep.add(baseLoaded ? OK : WARN, '.env', baseLoaded || `未找到 ${path.join(home, '.env')}（只用真实环境变量与默认值）`)
  const configPath = resolveConfigPath(str(values.config), home)
  const extEnv = str(values['ext-env'])
  const overlay = cfg.applyEnvOverlay(extEnv || cfg.defaultOverlayFor(configPath))
  if (extEnv && overlay === null) rep.add(FAIL, '环境覆盖层', `--ext-env 指定的文件不存在: ${extEnv}`)
  else if (overlay) rep.add(OK, '环境覆盖层', overlay)
  rep.add(OK, 'worker_id', cfg.deriveWorkerId(path.parse(configPath).name))
  if (str(values['work-dir'])) settings.WORK_DIR = expandHome(values['work-dir'])

  let conf = null
  if (!fs.existsSync(configPath)) {
    rep.add(FAIL, 'channel 配置', `不存在: ${configPath}（首次运行 worker 会写出模板）`)
  } else {
    try {
      conf = loadChannelConfig(configPath)
      rep.add(OK, 'channel 配置', `${configPath}（${Object.keys(conf.CHANNELS).length} 个 channel）`)
    } catch (err) {
      rep.add(FAIL, 'channel 配置', `加载失败: ${err.name}: ${err.message}`)
    }
  }
  if (conf) checkChannels(rep, conf)

  checkWorkdir(rep)

  if (!settings.INTERNAL_TOKEN.trim()) rep.add(FAIL, '令牌', `${E.ENV_INTERNAL_TOKEN} 未配置（空令牌 = 服务端拒绝一切请求）`)
  else rep.add(OK, '令牌', '已配置')

  const leaseBase = settings.TASK_SERVER_URL.trim() || settings.WEB_BASE_URL
  await checkLeaseChannel(rep, leaseBase, timeout)
  await checkCallbackChannel(rep, settings.WEB_BASE_URL, timeout)
  if (!values['no-storage']) await checkStorage(rep, timeout)

  console.log(`\n${rep.summary()}`)
  return rep.failed ? 1 : 0
}

module.exports = { main, Report, checkLeaseChannel, checkCallbackChannel, checkStorage, checkWorkdir, checkChannels }
