'use strict'

/**
 * 产物交付的唯一分流咽喉 —— 决定一个产物同步上传还是交给 uploader。对应 Python sdk/upload_route.py。
 *
 * 三个入口，名字就是契约：
 *   deliverRequired(...)  下游步骤依赖 → 恒同步，不看大小（异步化会让下一步拿到还没传完的 URL，
 *                         且偶发、取决于产物大小，最难排查）
 *   deliverOnly(...)      纯交付给用户 → 小的同步、≥阈值的交 uploader。判据是合取：下游不消费 AND ≥阈值
 *   deliver(...)          由可配置规则决定：WEBX_WORKER_UPLOAD_REQUIRED_GLOBS（逗号分隔的 fnmatch 模式，
 *                         按【远端文件名】匹配）命中的视为 required，其余视为 only。框架不内置任何扩展名判据
 *
 * 返回值（调用方直接落进 result 字段）：真 URL = 同步上传成功；'' = 进了队列（uploader 传完 merge 真 URL）或彻底没戏。
 * 进队列与彻底失败对调用方是同一个返回值，刻意的 —— 不写占位值就没有「占位值要先推上云」的次序问题。
 *
 * 所有入口 async，永不抛出上传错误（只有入参非法 —— 文件名不合规 —— 会响亮失败）。
 * 与 Python 版差异：fnmatch 为本文件自带的最小实现（fnmatchcase）。
 */

const fs = require('node:fs')
const path = require('node:path')
const E = require('../envnames')
const { getLogger } = require('../log')
const q = require('./uploadQueue')
const { checkResultFilename, uploadResult } = require('./resultClient')
const { envStr } = require('./util')

const logger = getLogger('sdk.uploadRoute')

const DEFAULT_THRESHOLD_MB = 15

// 「未配 storage」只告警一次：分流判据每个产物都要过一遍，逐次刷屏没有信息量
let warnedNoStorage = false

// ─── fnmatch ─────────────────────────────────────────────────────────────

const reCache = new Map()

/** glob → RegExp 源码。语义同 Python fnmatch.translate：* ? [seq] [!seq]；`]` 紧跟 `[`/`[!` 时是字面量；`[` 无闭合则字面 */
function globToRegexSource(pat) {
  let out = ''
  const n = pat.length
  let i = 0
  while (i < n) {
    const c = pat[i++]
    if (c === '*') {
      while (pat[i] === '*') i++
      out += '[\\s\\S]*'
    } else if (c === '?') {
      out += '[\\s\\S]'
    } else if (c === '[') {
      let j = i
      if (pat[j] === '!') j++
      if (pat[j] === ']') j++
      while (j < n && pat[j] !== ']') j++
      if (j >= n) {
        out += '\\['
      } else {
        let stuff = pat.slice(i, j)
        i = j + 1
        let neg = false
        if (stuff[0] === '!') { neg = true; stuff = stuff.slice(1) }
        stuff = stuff.replace(/[\\\]\[]/g, '\\$&')
        if (!neg && stuff[0] === '^') stuff = '\\' + stuff
        out += `[${neg ? '^' : ''}${stuff}]`
      }
    } else {
      out += c.replace(/[.*+?^${}()|[\]\\/-]/g, '\\$&')
    }
  }
  return `^${out}$`
}

/** 区分大小写的 fnmatch（对应 fnmatch.fnmatchcase） */
function fnmatchcase(name, pat) {
  let re = reCache.get(pat)
  if (re === undefined) {
    try { re = new RegExp(globToRegexSource(pat)) } catch (_) { re = null } // 如反向范围 [z-a]：按不匹配处理
    reCache.set(pat, re)
  }
  return re !== null && re.test(name)
}

// ─── 环境判据（每次现读）────────────────────────────────────────────────

/** 是否配置了存储服务地址 —— uploader 能工作的前提（协议规定产物只传存储服务，无回退路径） */
const storageConfigured = () => Boolean(envStr(E.ENV_STORAGE_INTERNAL_URL) || envStr(E.ENV_STORAGE_URL))

/**
 * uploader 总开关。未注入即视为关闭 —— 不由 worker 拉起的子进程、直接跑脚本的调试场景，
 * 都退回「一律同步上传」，绝不静默投进一个没人消费的队列。未配存储地址时同样视为关闭。
 */
function asyncEnabled() {
  if (!['1', 'true', 'yes', 'on'].includes(envStr(E.ENV_UPLOADER_ENABLED).toLowerCase())) return false
  if (!envStr(E.ENV_WORK_DIR)) return false
  if (!storageConfigured()) {
    if (!warnedNoStorage) {
      warnedNoStorage = true
      logger.warning(`uploader 已开启但未配置存储服务（${E.ENV_STORAGE_INTERNAL_URL} / ${E.ENV_STORAGE_URL}），大产物改走同步上传——投进队列只会无限重试`)
    }
    return false
  }
  return true
}

function thresholdBytes() {
  const raw = envStr(E.ENV_UPLOAD_ASYNC_THRESHOLD_MB)
  // 严格整数（Python int()）：'1.5' / 'abc' 回落默认值；'0' 是合法值（一律异步）
  const mb = /^[+-]?\d+$/.test(raw) ? parseInt(raw, 10) : DEFAULT_THRESHOLD_MB
  return Math.max(0, mb) * 1024 * 1024
}

/** 解析可配置的「恒同步」规则（逗号分隔的 fnmatch 模式） */
const requiredGlobs = () => envStr(E.ENV_UPLOAD_REQUIRED_GLOBS).split(',').map((g) => g.trim()).filter(Boolean)

/** 按配置的规则判断一个产物是否「下游依赖」（命中任一模式即是） */
const isRequired = (remoteName) => requiredGlobs().some((g) => fnmatchcase(remoteName, g))

// ─── 分流 ────────────────────────────────────────────────────────────────

/** 投队列。成功 true / 失败 false */
function enqueue(taskId, src, remoteName, keyPath) {
  const workDir = envStr(E.ENV_WORK_DIR)
  if (!workDir) return false
  return q.enqueue(workDir, taskId, src, remoteName, keyPath)
}

/** 同步上传，成功返回 download_url，失败返回 ''（异常不外泄） */
async function syncUpload(taskId, src, remoteName) {
  try {
    const resp = await uploadResult(taskId, src, remoteName)
    return resp.download_url || ''
  } catch (e) { // 上传失败绝不能掀翻步骤
    logger.error(`上传失败 ${remoteName}: ${e && e.message}`)
    return ''
  }
}

async function deliverImpl(taskId, localPath, remoteName, keyPath, required) {
  checkResultFilename(remoteName) // 响亮失败：不让坏名字进入重试 / 降级进队列的路径
  const src = path.resolve(String(localPath))
  let st
  try { st = fs.statSync(src) } catch (_) { return '' }
  if (!st.isFile()) return ''

  const goAsync = !required && asyncEnabled() && st.size >= thresholdBytes()
  if (!goAsync) {
    const url = await syncUpload(taskId, src, remoteName)
    if (url) return url
    // 同步失败降级进队列：文件太大、重试超时这类问题交给 uploader 慢慢磨，比「写空串、产物永久丢失」严格改善。
    // required 产物同样降级 —— 落空串对下游本来就是「暂时没有」，进队列只是多一条自愈的路
    if (asyncEnabled() && enqueue(taskId, src, remoteName, keyPath)) {
      logger.warning(`同步上传失败，已转交 uploader 补传: ${remoteName}`)
    }
    return ''
  }

  if (enqueue(taskId, src, remoteName, keyPath)) return '' // 字段留空，uploader 传完 merge 真 URL 进来
  logger.warning(`投递上传队列失败，退回同步上传: ${remoteName}`) // 别让产物无声消失
  return syncUpload(taskId, src, remoteName)
}

/** 下游步骤依赖的产物 —— 恒同步上传，不看大小。多大都等，等的是正确性 */
const deliverRequired = (taskId, localPath, remoteName, keyPath) => deliverImpl(taskId, localPath, remoteName, keyPath, true)

/** 纯交付给用户的产物 —— 按大小分流：小的同步传掉，大的交 uploader（小文件也走同步是为了省一趟队列往返） */
const deliverOnly = (taskId, localPath, remoteName, keyPath) => deliverImpl(taskId, localPath, remoteName, keyPath, false)

/** 由可配置规则决定走 required 还是 only：命中 WEBX_WORKER_UPLOAD_REQUIRED_GLOBS 的远端文件名恒同步，其余按大小 */
const deliver = (taskId, localPath, remoteName, keyPath) => deliverImpl(taskId, localPath, remoteName, keyPath, isRequired(remoteName))

/** 测试用：重置「只告警一次」闩锁 */
function _resetForTest() { warnedNoStorage = false }

module.exports = {
  deliverRequired, deliverOnly, deliver,
  requiredGlobs, isRequired, fnmatchcase,
  _resetForTest,
}
