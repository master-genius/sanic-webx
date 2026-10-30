'use strict'

/**
 * WorkerRecovery —— 启动恢复，移植自 webx_worker/recovery.py（分流表见 docs/worker.md §8）。
 *
 * Worker 重启后扫描工作目录下【非点号开头】的子目录（.uploads/ 等基础设施目录不是任务），
 * 按 ok.json + 服务端状态协商处理：
 *   ok.json: not_found → 超 7 天清理，否则跳过；confirmed=true → 跳过；
 *            completed/未确认 → 用 ok.json 里记的令牌临时登记后重试 complete（服务端已是 completed 视为已确认；404 → not_found）；
 *            cancelled/未确认 → 标记已确认；failed/未确认 → 不重试（避免过期失败信号覆盖补偿器重投后的工作）。
 *   无 ok.json: 404 → 写 not_found 标记保留目录；网络故障 → 保留目录；服务端 completed → 保留目录（防数据丢失）；
 *            queued/running 且租户是本机 → 带令牌主动归还租约（任务立刻回池，被拒是正常结果），再按插件策略清理；
 *            其它终态 / 崩溃 → plugin.softCleanPolicy(taskType)：名字集合 → 软清理，null → rmtree。
 *
 * 与 Python 版的差异：
 *   · callback 约定为 callback.js 的单例形态：completeTask / getTaskStatus / releaseLease，
 *     结果对象字段 isNotFound / isError / data（data 内保持服务端原始 snake_case 键）。
 *   · 写 ok.json 用「临时文件 + rename」，崩溃时不留半截文件。
 */

const fs = require('node:fs')
const path = require('node:path')
const fencing = require('./fencing')
const { settings } = require('./config')
const { TaskKindPlugin } = require('./plugin')
const { getLogger } = require('./log')

const logger = getLogger('recovery')

// not_found 标记超过此天数后清理目录
const NOT_FOUND_EXPIRE_DAYS = 7

const rmtree = (p) => fs.rmSync(p, { recursive: true, force: true })

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'))
}

function writeJsonAtomic(p, data) {
  const tmp = `${p}.${process.pid}.tmp`
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8')
  fs.renameSync(tmp, p)
}

class WorkerRecovery {
  /**
   * @param {object} callback callback.js 的 callbackClient（或同形态对象）
   * @param {string} workBaseDir 工作目录根
   * @param {TaskKindPlugin} [plugin] 提供 softCleanPolicy；缺省为中性实现（一律 rmtree）
   */
  constructor(callback, workBaseDir, plugin = null) {
    this.callback = callback
    this.workBaseDir = String(workBaseDir)
    this.plugin = plugin || new TaskKindPlugin()
  }

  /** 扫描本地工作目录，执行恢复逻辑（逐目录串行；单个目录异常不影响其余） */
  async recover() {
    if (!fs.existsSync(this.workBaseDir)) {
      logger.info('[Recovery] 工作目录不存在，跳过恢复')
      return
    }
    // 点号开头的目录不是任务：不排除的话目录名会被当 task_id 去查服务端，每次启动都刷一条查询失败的告警
    const taskDirs = fs.readdirSync(this.workBaseDir)
      .filter((n) => !n.startsWith('.') && fs.statSync(path.join(this.workBaseDir, n), { throwIfNoEntry: false })?.isDirectory())
      .sort()
    if (!taskDirs.length) {
      logger.info('[Recovery] 无本地任务目录，跳过恢复')
      return
    }
    logger.info(`[Recovery] 开始扫描，发现 ${taskDirs.length} 个任务目录`)

    let recovered = 0
    for (const taskId of taskDirs) {
      const taskDir = path.join(this.workBaseDir, taskId)
      const okPath = path.join(taskDir, 'ok.json')
      try {
        if (fs.existsSync(okPath)) {
          const okData = readJson(okPath)
          if (okData.status === 'not_found') { // 前置过滤：not_found 标记
            if (okData.not_found_at && this._isNotFoundExpired(okData.not_found_at)) {
              logger.info(`[Recovery] task=${taskId} not_found 超过 ${NOT_FOUND_EXPIRE_DAYS} 天，清理目录`)
              rmtree(taskDir)
            } else {
              logger.info(`[Recovery] task=${taskId} 已标记 not_found，跳过`)
            }
            continue
          }
          if (await this._handleWithOkJson(taskId, taskDir, okPath, okData)) recovered += 1
        } else if (await this._handleWithoutOkJson(taskId, taskDir)) {
          recovered += 1
        }
      } catch (e) {
        logger.error(`[Recovery] 处理 task_id=${taskId} 异常: ${e.message}`, e)
      }
    }
    logger.info(`[Recovery] 恢复完成，处理了 ${recovered} 个任务`)
  }

  /** 处理有 ok.json 的目录。返回是否采取了动作 */
  async _handleWithOkJson(taskId, taskDir, okPath, okData) {
    if (okData.server_confirmed) {
      logger.debug(`[Recovery] task=${taskId} 已确认，跳过`)
      return false
    }
    const status = okData.status || ''
    if (status === 'completed') {
      // 仅重试状态迁移，result_path / log_path 由任务脚本通过 PATCH /fields 管理
      logger.info(`[Recovery] task=${taskId} 重试 complete 回调`)
      // 重启后令牌注册表是空的：用 ok.json 里记下的令牌临时登记，重试才不是「无令牌写」。
      // 只认本实例自己的记录（请求头里的 worker_id 取自当前配置，别人的令牌拿来也过不了）。
      const seq = okData.lease_seq
      const registered = Number.isInteger(seq) && okData.worker_id === settings.WORKER_ID && fencing.leaseSeqOf(taskId) === null
      if (registered) fencing.register(taskId, seq)
      let success
      try {
        success = await this.callback.completeTask(taskId)
      } finally {
        if (registered) fencing.unregister(taskId)
      }
      if (!success) { // 回调失败：查询服务端实际状态
        const st = await this.callback.getTaskStatus(taskId)
        if (!st.isNotFound && !st.isError && st.data && st.data.status === 'completed') {
          logger.info(`[Recovery] task=${taskId} 服务端已是 COMPLETED，视为已确认`)
          success = true
        } else if (st.isNotFound) {
          logger.warning(`[Recovery] task=${taskId} 服务端 404，标记 not_found`)
          this._writeNotFoundOkJson(taskDir, taskId, okData)
          return true
        }
        // isError → 网络故障，下次启动再试
      }
      if (success) {
        okData.server_confirmed = true
        okData.server_confirmed_at = new Date().toISOString()
        writeJsonAtomic(okPath, okData)
        return true
      }
      logger.warning(`[Recovery] task=${taskId} 重试 complete 回调失败，下次启动再试`)
      return false
    }
    if (status === 'cancelled') { // 取消状态但未确认，标记确认
      okData.server_confirmed = true
      writeJsonAtomic(okPath, okData)
      logger.info(`[Recovery] task=${taskId} 取消状态已确认`)
      return true
    }
    if (status === 'failed') {
      // 故意不在 recovery 重试 fail：重启后服务端可能已被补偿器重投，补打过期 fail 会覆盖正在执行的工作。
      // 失败补偿由服务端补偿器兜底；本地 ok.json 仅用于清理模块识别与审计诊断。
      logger.warning(`[Recovery] task=${taskId} 本地 failed 未确认，不主动重试 fail_task（由服务端补偿器兜底）`)
      return false
    }
    logger.warning(`[Recovery] task=${taskId} ok.json 状态未知: ${status}`)
    return false
  }

  /**
   * 处理无 ok.json 的目录（任务跑完必写 ok.json，没写就是没跑完）。
   * task_type 来源优先级：服务端响应 > 本地 meta.json。
   */
  async _handleWithoutOkJson(taskId, taskDir) {
    const result = await this.callback.getTaskStatus(taskId)
    if (result.isNotFound) {
      logger.info(`[Recovery] task=${taskId} 服务端 404，标记 not_found`)
      this._writeNotFoundOkJson(taskDir, taskId)
      return true
    }
    if (result.isError) {
      logger.warning(`[Recovery] task=${taskId} 查询失败(网络/5xx)，保留目录`)
      return false
    }
    const info = result.data || {}
    const serverStatus = info.status || ''

    // 主动归还租约：重启后几秒就能确知「这一轮没跑完」，归还比等一个完整租约周期快一个数量级。
    // fencing 保证安全：归还带 (worker_id, lease_seq)，任务已被别人领走则 lease_seq 不匹配直接被拒，
    // 物理上不可能覆盖别人的工作。被拒是正常结果（多半租约早已到期、任务被接管），不重试、不报错。
    if ((serverStatus === 'queued' || serverStatus === 'running')
      && info.lease_seq !== null && info.lease_seq !== undefined
      && info.worker_id && info.worker_id === settings.WORKER_ID) {
      fencing.register(taskId, Number(info.lease_seq))
      try {
        await this.callback.releaseLease(taskId)
      } finally {
        fencing.unregister(taskId)
      }
    }

    const taskType = info.task_type || this._readTaskTypeFromMeta(taskDir)
    const policy = this.plugin.softCleanPolicy(taskType)
    const keep = policy ? new Set(policy) : null

    if (serverStatus === 'completed') {
      // 已完成：保留目录，等专用清理统一处理，避免误删用户数据
      logger.info(`[Recovery] task=${taskId} 服务端 completed，保留目录等待专用清理脚本`)
      return true
    }
    const isFinalNotDone = serverStatus === 'failed' || serverStatus === 'cancelled'
    if (isFinalNotDone || ['running', 'queued', 'pending'].includes(serverStatus)) {
      // 插件要求保留的产物软清理保留（retry / 补偿器重投时复用）；其余整目录删
      const what = isFinalNotDone ? '终态' : '崩溃目录'
      if (keep) {
        logger.info(`[Recovery] task=${taskId} 服务端 ${serverStatus}(${taskType})，软清理${what}保留 ${JSON.stringify([...keep].sort())}`)
        this._softCleanCrashedDir(taskDir, keep)
      } else {
        logger.info(`[Recovery] task=${taskId} 服务端 ${serverStatus}(${taskType || '未知'})，清理${what}`)
        rmtree(taskDir)
      }
      return true
    }
    logger.warning(`[Recovery] task=${taskId} 未知服务端状态: ${serverStatus}`)
    return false
  }

  _readTaskTypeFromMeta(taskDir) {
    try { return readJson(path.join(taskDir, 'meta.json')).task_type || '' } catch (_) { return '' }
  }

  /** 崩溃目录软清理：只保留 keep 里的名字，其余（in/ out/ 元数据 ok.json 临时文件）一律删 */
  _softCleanCrashedDir(taskDir, keep) {
    if (!fs.existsSync(taskDir)) return
    for (const name of fs.readdirSync(taskDir)) {
      if (keep.has(name)) continue
      try {
        rmtree(path.join(taskDir, name))
      } catch (e) {
        logger.warning(`[Recovery] 软清理 ${path.join(taskDir, name)} 失败: ${e.message}`)
      }
    }
  }

  /** 写 not_found 状态的 ok.json，记录发现时间供过期清理判断 */
  _writeNotFoundOkJson(taskDir, taskId, baseData = null) {
    const okData = baseData || {}
    Object.assign(okData, {
      task_id: taskId,
      status: 'not_found',
      not_found_at: new Date().toISOString(),
      server_confirmed: false,
    })
    try { // 保留多阶段信息（如果有）
      const meta = readJson(path.join(taskDir, 'meta.json'))
      if (!('steps' in okData)) okData.steps = meta.steps || []
      if (!('sub_step' in okData)) okData.sub_step = meta.sub_step || ''
      if (!('end_step' in okData)) okData.end_step = meta.end_step || ''
    } catch (_) { /* 无 meta.json 或损坏 */ }
    try {
      writeJsonAtomic(path.join(taskDir, 'ok.json'), okData)
    } catch (e) {
      logger.error(`[Recovery] 写入 not_found ok.json 失败: ${taskId}: ${e.message}`)
    }
  }

  _isNotFoundExpired(notFoundAt) {
    const s = String(notFoundAt)
    const ms = Date.parse(/T/.test(s) && !/(Z|[+-]\d{2}:?\d{2})$/i.test(s) ? s + 'Z' : s) // 无时区按 UTC
    if (Number.isNaN(ms)) {
      logger.warning(`[Recovery] 解析 not_found_at 失败: ${notFoundAt}`)
      return false
    }
    return Math.floor((Date.now() - ms) / 86400000) >= NOT_FOUND_EXPIRE_DAYS
  }
}

module.exports = { WorkerRecovery, NOT_FOUND_EXPIRE_DAYS }
