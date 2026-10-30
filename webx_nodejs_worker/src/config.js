'use strict'

/**
 * Worker 配置
 *
 * 配置来源分三层，高优先级在前：
 *   ⓪ 真实进程环境变量（process.env）               —— 运维的显式覆盖
 *   ② 实例覆盖层 <channel 配置同目录>/<配置名>_env     —— applyEnvOverlay()
 *   ① 基础层 <worker 主目录>/.env                    —— loadBaseEnv()
 *   默认值
 *
 * .env 语法与 Python 版一致：KEY=VALUE，可带 export；# 注释；成对引号剥掉；
 * 未加引号的值里「空白 + #」起为行内注释；同名取最后一行；不做变量展开与 ~ 展开。
 *
 * ⚠️ 文件层只影响主进程；子进程需要的几项由 executor 显式注入。
 * ⚠️ settings 是单例，刷新只能原地 Object.assign，不能重新绑定（各模块持有的是对象引用）。
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const E = require('./envnames')
const { getLogger } = require('./log')

const logger = getLogger('config')

function parseEnvText(text) {
  const out = {}
  for (const raw of String(text).split(/\r?\n/)) {
    let line = raw.trim()
    if (!line || line.startsWith('#')) continue
    if (line.startsWith('export ')) line = line.slice(7).trimStart()
    const eq = line.indexOf('=')
    if (eq < 0) continue
    const key = line.slice(0, eq).trim()
    if (!key) continue
    let val = line.slice(eq + 1).trim()
    if (val.length >= 2 && val[0] === val[val.length - 1] && (val[0] === '"' || val[0] === "'")) {
      val = val.slice(1, -1)
    } else {
      for (let i = 1; i < val.length; i++) {
        if (val[i] === '#' && /\s/.test(val[i - 1])) { val = val.slice(0, i).trimEnd(); break }
      }
    }
    out[key] = val
  }
  return out
}

const fileLayers = { base: {}, overlay: {} }

function lookup(name) {
  const v = process.env[name]
  if (v !== undefined) return v
  for (const layer of ['overlay', 'base']) {
    if (Object.prototype.hasOwnProperty.call(fileLayers[layer], name)) return fileLayers[layer][name]
  }
  return null
}

const str = (name, def = '') => { const v = lookup(name); return v === null ? def : v }

function int(name, def) {
  const v = (lookup(name) || '').trim()
  if (!v) return def
  if (!/^[+-]?\d+$/.test(v)) { logger.warning(`${name} 不是整数：${JSON.stringify(v)}，使用默认值 ${def}`); return def }
  return parseInt(v, 10)
}

function float(name, def) {
  const v = (lookup(name) || '').trim()
  if (!v) return def
  const n = Number(v)
  if (!Number.isFinite(n)) { logger.warning(`${name} 不是数字：${JSON.stringify(v)}，使用默认值 ${def}`); return def }
  return n
}

function bool(name, def) {
  const v = (lookup(name) || '').trim().toLowerCase()
  if (!v) return def
  return ['1', 'true', 'yes', 'on'].includes(v)
}

/** 按当前三层来源构造一份配置（字段含义见 docs/worker.md §6） */
function buildSettings() {
  return {
    WORKER_ID: str(E.ENV_WORKER_ID),                      // 空 = 尚未派生，见 deriveWorkerId
    WORKER_PREFIX: str('WEBX_WORKER_PREFIX'),
    TASK_SERVER_URL: str('WEBX_WORKER_TASK_SERVER_URL'),  // 空 = 回落 WEB_BASE_URL
    LEASE_MAX_LEVEL: str('WEBX_WORKER_LEASE_MAX_LEVEL'),  // 空串区分「没配」与「配成 0」
    LEASE_MAX_COMPUTE_POINTS: str('WEBX_WORKER_LEASE_MAX_COMPUTE_POINTS'),
    LEASE_HOLD_SECONDS: float('WEBX_WORKER_LEASE_HOLD_SECONDS', 25.0),
    INTERNAL_TOKEN: str(E.ENV_INTERNAL_TOKEN),
    WORK_DIR: str(E.ENV_WORK_DIR, path.join(os.homedir(), 'webx-worker-tasks')),
    HEARTBEAT_INTERVAL: int('WEBX_WORKER_HEARTBEAT_INTERVAL', 30),   // 已废弃，不生效
    LOG_HEAD_LINES: int('WEBX_WORKER_LOG_HEAD_LINES', 100),
    LOG_TAIL_LINES: int('WEBX_WORKER_LOG_TAIL_LINES', 2000),
    WEB_BASE_URL: str(E.ENV_WEB_BASE_URL, 'http://localhost:9009'),
    STORAGE_URL: str(E.ENV_STORAGE_URL),
    STORAGE_INTERNAL_URL: str(E.ENV_STORAGE_INTERNAL_URL),
    STORAGE_INTERNAL_TOKEN: str(E.ENV_STORAGE_INTERNAL_TOKEN),
    UPLOADER_ENABLED: bool(E.ENV_UPLOADER_ENABLED, true),
    UPLOAD_ASYNC_THRESHOLD_MB: int(E.ENV_UPLOAD_ASYNC_THRESHOLD_MB, 15),
    UPLOAD_REQUIRED_GLOBS: str(E.ENV_UPLOAD_REQUIRED_GLOBS),
    UPLOADER_CONCURRENCY: int('WEBX_WORKER_UPLOADER_CONCURRENCY', 3),
    UPLOADER_SCAN_INTERVAL: float('WEBX_WORKER_UPLOADER_SCAN_INTERVAL', 3.0),
    UPLOADER_PENDING_LIMIT_GB: float('WEBX_WORKER_UPLOADER_PENDING_LIMIT_GB', 100.0),
    DEBUG: false,                                          // 由 --debug 覆盖
    LOG_DIR: str('WEBX_WORKER_LOG_DIR'),
    LOG_MAX_BYTES: int('WEBX_WORKER_LOG_MAX_BYTES', 10 * 1024 * 1024),
    LOG_MAX_LINES: int('WEBX_WORKER_LOG_MAX_LINES', 20000),
  }
}

const settings = buildSettings()

/** 原地刷新单例；保留 DEBUG（它来自命令行而非环境） */
function refreshSettings() {
  const debug = settings.DEBUG
  Object.assign(settings, buildSettings())
  settings.DEBUG = debug
}

const ENV_OVERLAY_SUFFIX = '_env'

function expandHome(p) {
  return p.startsWith('~') ? path.join(os.homedir(), p.slice(1)) : p
}

function readLayer(layer, p) {
  const full = path.resolve(expandHome(String(p)))
  let st
  try { st = fs.statSync(full) } catch (_) { return null }
  if (!st.isFile()) return null
  fileLayers[layer] = parseEnvText(fs.readFileSync(full, 'utf8'))
  refreshSettings()
  return full
}

/** 加载基础层 .env；不存在 → 静默跳过（返回 null） */
const loadBaseEnv = (p) => readLayer('base', p)

/** 约定的覆盖层路径：<同目录>/<配置名去扩展名>_env（仅推导，不判断存在性） */
function defaultOverlayFor(configPath) {
  const { dir, name } = path.parse(configPath)
  return path.join(dir, `${name}${ENV_OVERLAY_SUFFIX}`)
}

/** 加载实例覆盖层并原地刷新。必须在任何命令行覆盖（--debug / --work-dir）之前调用 */
const applyEnvOverlay = (p) => readLayer('overlay', p)

function resetFileLayers() {
  fileLayers.base = {}
  fileLayers.overlay = {}
  refreshSettings()
}

/** worker 主目录：WEBX_WORKER_HOME，缺省当前目录（刻意不锚定包源码目录） */
function workerHome() {
  const raw = (process.env.WEBX_WORKER_HOME || '').trim()
  return path.resolve(raw ? expandHome(raw) : process.cwd())
}

const ID_MAX_LEN = 64

function sanitizeIdPart(raw) {
  let cleaned = String(raw).trim().replace(/[^A-Za-z0-9_-]/g, '-')
  cleaned = cleaned.replace(/-{2,}/g, '-')
  return cleaned.replace(/^-+|-+$/g, '')
}

/**
 * 派生并原地写入 settings.WORKER_ID：`{前缀}-{配置名}`。
 * 必须跨重启稳定（recovery 靠它归还上一轮租约），所以不含 PID / 随机数。
 * 必须在 applyEnvOverlay() 之后调用（后者会整体重建配置）。
 */
function deriveWorkerId(configName) {
  const explicit = (settings.WORKER_ID || '').trim()
  if (explicit) {
    settings.WORKER_ID = explicit.slice(0, ID_MAX_LEN)
    return settings.WORKER_ID
  }
  let prefix = (settings.WORKER_PREFIX || '').trim()
  if (!prefix) prefix = os.hostname().split('.')[0]
  prefix = sanitizeIdPart(prefix) || 'worker'
  const suffix = sanitizeIdPart(configName) || 'default'
  settings.WORKER_ID = `${prefix}-${suffix}`.slice(0, ID_MAX_LEN)
  return settings.WORKER_ID
}

/** 是否配置了存储服务地址（产物上传能否工作的硬前提） */
function storageConfigured() {
  return Boolean(settings.STORAGE_INTERNAL_URL.trim() || settings.STORAGE_URL.trim())
}

/** 领取请求要带的筛选字段 [max_level, max_compute_points]，null = 不限 */
function leaseFilters() {
  const out = []
  for (const [name, raw] of [['WEBX_WORKER_LEASE_MAX_LEVEL', settings.LEASE_MAX_LEVEL],
    ['WEBX_WORKER_LEASE_MAX_COMPUTE_POINTS', settings.LEASE_MAX_COMPUTE_POINTS]]) {
    const s = (raw || '').trim()
    if (!s) { out.push(null); continue }
    if (!/^[+-]?\d+$/.test(s)) { logger.warning(`${name} 非整数：${JSON.stringify(s)}，按不限处理`); out.push(null); continue }
    out.push(parseInt(s, 10))
  }
  return out
}

module.exports = {
  settings, parseEnvText, refreshSettings, loadBaseEnv, applyEnvOverlay, defaultOverlayFor,
  resetFileLayers, workerHome, deriveWorkerId, storageConfigured, leaseFilters,
  ENV_OVERLAY_SUFFIX,
}
