'use strict'

/**
 * 异步上传队列 —— 磁盘目录即消息，生产者（任务子进程）与消费者（uploader 进程）零通知、零回边。
 * 对应 Python sdk/upload_queue.py；函数名机械转 camelCase，全部【同步】（与 Python 一致，条目都很小）。
 *
 * 目录：<WORK_DIR>/.uploads/pending|inflight/{task_id}__{slug}.json
 *   条目文件名由 (task_id, key_path) 派生 —— 重复投递天然幂等；只记路径，不复制文件。
 *   落库字段一直是空串：不写占位值（占位值有「uploader 先扫到、误当过期删掉」的次序问题）。
 *
 * 磁盘格式沿用 Python 的 snake_case 键（task_id / local_path / remote_name / key_path / size /
 * queued_at / attempts / uploaded_url / session_id / first_queued_at），内存里的 Entry 字段是 camelCase。
 * 时间戳是浮点秒。
 *
 * 与 Python 版的差异（避开 docs/worker.md §14 的 K17）：
 *   条目新增 `nonce`（每次 enqueue 随机生成）。同名条目被 redo 覆盖后，旧条目的 drop() / release()
 *   先校验磁盘上的 nonce，不一致说明那是更新的投递，【不删也不覆盖】。缺 nonce 的历史条目按 '' 比较，
 *   行为与 Python 版一致。校验与删除之间不是原子的（极小窗口），但 uploader 单实例串行处理，足够。
 */

const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { getLogger } = require('../log')
const { nowSec } = require('./util')

const logger = getLogger('sdk.uploadQueue')

const QUEUE_DIRNAME = '.uploads'
const PENDING_DIR = 'pending'
const INFLIGHT_DIR = 'inflight'

// 文件名净化：key_path 通常由任务脚本生成，但混入用户名字时不能出现路径分隔符等危险字符（纯属防御）
const UNSAFE_CHARS = /[^A-Za-z0-9._-]+/g
const MAX_SLUG = 120

/** key_path → 条目文件名主体。["step1","out_url"] → "step1_out"；字段本身叫 url 的保持不动 */
function entrySlug(keyPath) {
  const parts = keyPath.map((k) => String(k))
  const last = parts.length - 1
  if (last >= 0 && parts[last] !== 'url' && parts[last].endsWith('_url')) {
    parts[last] = parts[last].slice(0, -'_url'.length)
  }
  const raw = parts.filter((p) => p).join('_')
  // 先剥首尾的 - 和 . 再截断（与 Python 同序）
  const slug = raw.replace(UNSAFE_CHARS, '-').replace(/^[-.]+|[-.]+$/g, '').slice(0, MAX_SLUG)
  return slug || 'entry'
}

/** 一条待上传记录。`path` 是它当前所在的文件位置（pending/ 或 inflight/） */
class Entry {
  constructor({
    taskId, localPath, remoteName, keyPath, size = 0, queuedAt = 0, attempts = 0,
    uploadedUrl = '', sessionId = '', firstQueuedAt = 0, nonce = '', path: p = null,
  }) {
    this.taskId = taskId
    this.localPath = localPath
    this.remoteName = remoteName
    this.keyPath = keyPath
    this.size = size
    this.queuedAt = queuedAt
    this.attempts = attempts
    this.uploadedUrl = uploadedUrl   // 两阶段：上传成功后原地记下 URL，之后只做回写，不重传
    this.sessionId = sessionId       // 分片续传会话：崩溃重启后凭它取 offset 接着传
    this.firstQueuedAt = firstQueuedAt
    this.nonce = nonce               // 投递身份，见文件头 K17
    this.path = p
  }

  /** 落盘的 JSON 对象（snake_case 键，不含 path） */
  toDict() {
    return {
      task_id: this.taskId,
      local_path: this.localPath,
      remote_name: this.remoteName,
      key_path: this.keyPath,
      size: this.size,
      queued_at: this.queuedAt,
      attempts: this.attempts,
      uploaded_url: this.uploadedUrl,
      session_id: this.sessionId,
      first_queued_at: this.firstQueuedAt,
      nonce: this.nonce,
    }
  }

  /** 条目身份 = 从 (task_id, key_path) 派生（不是随机数）：同一产物恒得同一文件名 */
  get filename() {
    return `${this.taskId.replace(UNSAFE_CHARS, '-')}__${entrySlug(this.keyPath)}.json`
  }
}

/** 磁盘 JSON → Entry；结构非法返回 null */
function fromDict(d, p) {
  try {
    if (d === null || typeof d !== 'object') throw new TypeError('not an object')
    for (const k of ['task_id', 'local_path', 'remote_name']) {
      if (d[k] === undefined || d[k] === null) throw new TypeError(`missing ${k}`)
    }
    const num = (v) => {
      const n = Number(v || 0)
      if (!Number.isFinite(n)) throw new TypeError('bad number')
      return n
    }
    return new Entry({
      taskId: String(d.task_id),
      localPath: String(d.local_path),
      remoteName: String(d.remote_name),
      keyPath: (Array.isArray(d.key_path) ? d.key_path : []).map((k) => String(k)),
      size: Math.trunc(num(d.size)),
      queuedAt: num(d.queued_at),
      attempts: Math.trunc(num(d.attempts)),
      uploadedUrl: String(d.uploaded_url || ''),
      sessionId: String(d.session_id || ''),
      firstQueuedAt: num(d.first_queued_at),
      nonce: String(d.nonce || ''),
      path: p,
    })
  } catch (e) {
    logger.warning(`[upload_queue] 条目结构非法，跳过: ${path.basename(p)}, ${e.message}`)
    return null
  }
}

// ==================== 目录 ====================

const queueRoot = (workDir) => path.join(String(workDir), QUEUE_DIRNAME)
const pendingDir = (workDir) => path.join(queueRoot(workDir), PENDING_DIR)
const inflightDir = (workDir) => path.join(queueRoot(workDir), INFLIGHT_DIR)

function ensureDirs(workDir) {
  fs.mkdirSync(pendingDir(workDir), { recursive: true })
  fs.mkdirSync(inflightDir(workDir), { recursive: true })
}

/** 同目录 .tmp + rename 原子落位（跨文件系统 rename 不原子，消费者可能读到半截 JSON） */
function atomicWrite(target, payload) {
  fs.mkdirSync(path.dirname(target), { recursive: true })
  const tmp = path.join(path.dirname(target), `.${path.basename(target)}.${crypto.randomBytes(4).toString('hex')}.tmp`)
  try {
    fs.writeFileSync(tmp, JSON.stringify(payload), 'utf8')
    fs.renameSync(tmp, target)
  } catch (e) {
    try { fs.unlinkSync(tmp) } catch (_) { /* 已不存在 */ }
    throw e
  }
}

const unlinkQuiet = (p) => { try { fs.unlinkSync(p) } catch (_) { /* missing_ok */ } }

/** 读磁盘上某条目的 nonce：文件不存在返回 null；存在但读不出来按 '' 处理 */
function diskNonce(file) {
  let text
  try { text = fs.readFileSync(file, 'utf8') } catch (_) { return null }
  try {
    const d = JSON.parse(text)
    return String((d && d.nonce) || '')
  } catch (_) { return '' }
}

const realpathSafe = (p) => { try { return fs.realpathSync(p) } catch (_) { return path.resolve(p) } }

// ==================== 投递侧（任务子进程调用） ====================

/**
 * 投一张上传条子。成功 true / 失败 false。
 * 失败时调用方退回同步上传，绝不因为投递失败让步骤失败。
 * ⚠️ 不复制文件，只记路径；对应地，清理侧必须跳过仍被队列引用的文件（见 referencedPaths）。
 */
function enqueue(workDir, taskId, localPath, remoteName, keyPath) {
  const src = path.resolve(String(localPath))
  let st
  try { st = fs.statSync(src) } catch (_) { st = null }
  if (!st || !st.isFile()) {
    logger.warning(`[upload_queue] 投递跳过（文件不存在）: ${src}`)
    return false
  }
  const now = nowSec()
  const entry = new Entry({
    taskId,
    localPath: realpathSafe(src),
    remoteName,
    keyPath: [...keyPath],
    size: st.size,
    queuedAt: now,
    firstQueuedAt: now,
    nonce: crypto.randomBytes(8).toString('hex'),
  })
  try {
    atomicWrite(path.join(pendingDir(workDir), entry.filename), entry.toDict())
  } catch (e) {
    logger.error(`[upload_queue] 投递失败: ${remoteName}, ${e.message}`)
    return false
  }
  logger.info(`[upload_queue] 已投递: task=${taskId} file=${remoteName} size=${entry.size} entry=${entry.filename}`)
  return true
}

// ==================== 消费侧（uploader 进程调用） ====================

function loadDir(d) {
  let names
  try { names = fs.readdirSync(d) } catch (_) { return [] }
  const out = []
  for (const name of names.filter((n) => n.endsWith('.json') && !n.startsWith('.')).sort()) {
    const f = path.join(d, name)
    let data
    try {
      data = JSON.parse(fs.readFileSync(f, 'utf8'))
    } catch (e) {
      if (e.code === 'ENOENT') continue // 扫描期间被别人取走
      // 半截/损坏的条目：只可能来自非原子写入的历史残留。记一笔并删掉，免得每轮重复报错
      logger.warning(`[upload_queue] 条目损坏，已删除: ${name}, ${e.message}`)
      unlinkQuiet(f)
      continue
    }
    const e2 = fromDict(data, f)
    if (e2) out.push(e2)
  }
  return out
}

/** 列出待上传条目（按文件名排序，天然按 task 聚集） */
const iterPending = (workDir) => loadDir(pendingDir(workDir))

/** pending → inflight。返回新位置的条目；已被取走返回 null（rename 原子，多消费者也只有一个拿到） */
function claim(workDir, entry) {
  const src = path.join(pendingDir(workDir), entry.filename)
  const dst = path.join(inflightDir(workDir), entry.filename)
  fs.mkdirSync(path.dirname(dst), { recursive: true })
  try {
    fs.renameSync(src, dst)
  } catch (_) {
    return null
  }
  entry.path = dst
  return entry
}

/**
 * inflight → pending，attempts +1，保留 uploaded_url / session_id（回写失败时下一轮只做回写）。
 * K17：pending 已有 nonce 不同的同名条目 = 期间被 redo 投递了新产物，【不覆盖】，只清掉 inflight 旧副本。
 */
function release(workDir, entry) {
  entry.attempts += 1
  entry.queuedAt = nowSec()
  const target = path.join(pendingDir(workDir), entry.filename)
  const inflight = path.join(inflightDir(workDir), entry.filename)
  try {
    const cur = diskNonce(target)
    if (cur !== null && cur !== entry.nonce) {
      logger.info(`[upload_queue] 退回时发现更新的同名投递，丢弃旧条目: ${entry.filename}`)
      unlinkQuiet(inflight)
      return
    }
    atomicWrite(target, entry.toDict())
    unlinkQuiet(inflight)
  } catch (e) {
    logger.error(`[upload_queue] 退回队列失败: ${entry.filename}, ${e.message}`)
    return
  }
  entry.path = target
}

/** 原地更新 inflight 中的条目（记 uploaded_url / session_id，不改变所在队列） */
function update(workDir, entry) {
  try {
    atomicWrite(path.join(inflightDir(workDir), entry.filename), entry.toDict())
  } catch (e) {
    logger.error(`[upload_queue] 条目更新失败: ${entry.filename}, ${e.message}`)
  }
}

/** 彻底移除条目（传完了，或云端已判定不需要）。两个目录都清，幂等；nonce 不一致的（被覆盖的新条目）保留 */
function drop(workDir, entry) {
  for (const d of [pendingDir(workDir), inflightDir(workDir)]) {
    const f = path.join(d, entry.filename)
    const cur = diskNonce(f)
    if (cur !== null && cur === entry.nonce) unlinkQuiet(f)
  }
}

/**
 * 把 inflight/ 里的条目搬回 pending/ —— uploader 启动的第一件事。幂等。
 * pending 已存在同名条目时不覆盖，只删 inflight 那份（pending 的是更新版本，盖回去会丢 uploaded_url）。
 */
function requeueInflight(workDir) {
  const srcDir = inflightDir(workDir)
  const dstDir = pendingDir(workDir)
  let names
  try { names = fs.readdirSync(srcDir) } catch (_) { return 0 }
  fs.mkdirSync(dstDir, { recursive: true })
  let moved = 0
  for (const name of names.filter((n) => n.endsWith('.json') && !n.startsWith('.')).sort()) {
    const src = path.join(srcDir, name)
    const dst = path.join(dstDir, name)
    try {
      if (fs.existsSync(dst)) {
        unlinkQuiet(src)
      } else {
        fs.renameSync(src, dst)
        moved += 1
      }
    } catch (e) {
      logger.warning(`[upload_queue] 搬回 pending 失败: ${name}, ${e.message}`)
    }
  }
  if (moved) logger.info(`[upload_queue] 启动搬回 ${moved} 条上次未完成的上传`)
  return moved
}

// ==================== 观测 / 背压 ====================

const allEntries = (workDir) => [...iterPending(workDir), ...loadDir(inflightDir(workDir))]

/** 待传总字节数（读条目里记录的 size，不 stat 文件）——父进程接新任务前的背压依据 */
function pendingBytes(workDir) {
  return allEntries(workDir).reduce((sum, e) => sum + e.size, 0)
}

/** 队列当前引用的本地文件路径集合（realpath）——清理侧据此跳过，避免删掉还没传的产物 */
function referencedPaths(workDir) {
  return new Set(allEntries(workDir).map((e) => realpathSafe(e.localPath)))
}

module.exports = {
  QUEUE_DIRNAME, PENDING_DIR, INFLIGHT_DIR,
  Entry, fromDict, entrySlug,
  queueRoot, pendingDir, inflightDir, ensureDirs,
  enqueue, iterPending, claim, release, update, drop, requeueInflight,
  pendingBytes, referencedPaths,
}
