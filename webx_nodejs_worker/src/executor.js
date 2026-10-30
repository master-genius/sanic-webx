'use strict'

/**
 * GenericTaskExecutor —— 通用任务执行器，与具体业务完全解耦。
 *
 * 职责：建任务目录 → 起心跳（先于下载）→ 下载输入 → 写 params/files/meta.json →
 * 经 cdpc 拉起子进程（detached，独立进程组）→ 增量/终态日志上传 → ok.json（先写后通知）→ complete/fail。
 * 与任务类型相关的少数规则（日志 key、输入筛选、环境变量）经 TaskKindPlugin 注入。
 *
 * 与 Python 版的有意差异：
 *   · 中断用 AbortSignal 表达：execute(taskId, payload, signal)，被 abort 抛 Interrupted
 *     （对应 CancelledError 分支：不写 ok.json、绝不上报 fail）。
 *   · 终止子进程：取消 / 超时 / 停机一律先 SIGTERM（同步发出），宽限期（WEBX_WORKER_KILL_GRACE_SECONDS，
 *     默认 3s）后 SIGKILL；子进程退出后再对整个进程组无条件 SIGKILL，清掉残留孙进程。
 *   · 子进程由 cdpc 管（procManager 单例），子进程 stdout / stderr 分别装配行后写同一份日志。
 *   · 输入文件名 / task_id / sub_step 做路径名归一化（Python 版 K12 / K23 未做）。
 *   · 脚本一律追加 `--task-id <ID>`（Python 版对 .sh / .js 漏了；环境变量里本来就有）。
 */

const fs = require('node:fs')
const path = require('node:path')
const E = require('./envnames')
const fencing = require('./fencing')
const { settings } = require('./config')
const { getLogger } = require('./log')
const { TaskKindPlugin } = require('./plugin')
const { buildCmdPrefix } = require('./channelLoader')
const { getManager } = require('./procManager')
const { LineAssembler } = require('./logWriter')
const os = require('node:os')
const { Interrupted, Event, sleep } = require('./aio')

const logger = getLogger('executor')

// 子进程退出后，等 drain 读完残余输出的兜底时长（秒）。测试可压短
const limits = { DRAIN_GRACE_SEC: 2.0 }
const SAFE_NAME = /^[A-Za-z0-9._-]+$/

function killGraceSeconds() {
  const v = Number(process.env.WEBX_WORKER_KILL_GRACE_SECONDS)
  return Number.isFinite(v) && v >= 0 ? v : 3.0
}

/** 对整个进程组发信号（detached ⇒ pgid == pid）；进程组已不存在是正常情况 */
function killGroup(pid, sig) {
  if (!pid) return
  try { process.kill(-pid, sig) } catch (_) { /* ESRCH / EPERM */ }
}

const isoNow = () => new Date().toISOString()

class GenericTaskExecutor {
  constructor(callback, logWriter, heartbeat, httpClient, workBaseDir, channelsConfig, condaRoot,
    ipcSockPath = null, plugin = null, scriptRoot = null) {
    this.callback = callback
    this.logWriter = logWriter
    this.heartbeat = heartbeat
    this.httpClient = httpClient
    this.workBaseDir = path.resolve(workBaseDir)
    fs.mkdirSync(this.workBaseDir, { recursive: true })
    this.channels = channelsConfig
    this.condaRoot = condaRoot
    this.plugin = plugin || new TaskKindPlugin()
    this.scriptRoot = scriptRoot ? path.resolve(scriptRoot) : process.cwd()
    this.ipcSockPath = ipcSockPath
    // 增量日志推送的去冗余状态（按 task_id 隔离）：
    //   streamedMarks：上次推送时的内容修订号，未变化则跳过（用修订号而非行数：进度行原地刷新时行数不变）
    //   streamedFields：已写过 log_path 字段的任务（URL 稳定，字段仅首次写入）
    this.streamedMarks = new Map()
    this.streamedFields = new Set()
    this.procSeq = 0
  }

  /** 把 promise 与 abort 信号竞态：被 abort 立即抛 Interrupted */
  _guard(signal, promise) {
    if (!signal) return promise
    if (signal.aborted) return Promise.reject(new Interrupted())
    return new Promise((resolve, reject) => {
      const onAbort = () => reject(new Interrupted())
      signal.addEventListener('abort', onAbort, { once: true })
      promise.then(
        (v) => { signal.removeEventListener('abort', onAbort); resolve(v) },
        (e) => { signal.removeEventListener('abort', onAbort); reject(e) },
      )
    })
  }

  async execute(taskId, payload, signal = null) {
    if (!SAFE_NAME.test(taskId) || taskId === '.' || taskId === '..') {
      throw new Error(`非法 task_id: ${JSON.stringify(taskId)}`)
    }
    const taskDir = path.join(this.workBaseDir, taskId)
    const inDir = path.join(taskDir, 'in')
    fs.mkdirSync(inDir, { recursive: true })
    fs.mkdirSync(path.join(taskDir, 'out'), { recursive: true })

    // 接收新任务（含 retry/redo/补偿重投）：删除旧 ok.json，避免上一轮的终态标记误导 recovery / 清理
    try { fs.rmSync(path.join(taskDir, 'ok.json'), { force: true }) } catch (e) {
      logger.warning(`清理旧 ok.json 失败 task_id=${taskId}: ${e.message}`)
    }

    const taskType = payload.task_type || 'unknown'
    const steps = payload.steps || []
    const subStep = payload.sub_step || ''
    const endStep = payload.end_step || ''
    const meta = { steps, subStep, endStep }

    // 上传日志的 key / 文件名一次算定：流式增量推送与末尾上传共用，口径一致、URL 稳定
    const [logKey, logUploadName] = this.plugin.logTarget(taskType, subStep, steps)

    logger.info(`[${taskType}] 开始处理 task_id=${taskId}` +
      (endStep ? ` steps=${JSON.stringify(steps)} sub_step=${subStep} end_step=${endStep}` : ''))

    try {
      // 0. 执行前检查服务端任务状态（区分 404 / cancelled / 网络故障）
      const result = await this._guard(signal, this.callback.getTaskStatus(taskId))
      if (result.isNotFound) {
        logger.warning(`任务不存在(task 404)，跳过执行: task_id=${taskId}`)
        this._writeOkJson(taskDir, taskId, 'not_found', false, { ...meta, notFoundAt: isoNow() })
        return
      }
      if (result.isOk && result.taskStatus === 'cancelled') {
        logger.warning(`任务已被取消，跳过执行: task_id=${taskId}`)
        this._writeOkJson(taskDir, taskId, 'cancelled', true, meta)
        return
      }
      // result.isError → 网络故障，不阻塞，继续执行

      // 1. 启动心跳 —— 必须在下载之前（下载期间租约持续被续；起跑前的 queued 视为存活，见 heartbeat）
      await this._guard(signal, this.heartbeat.register(taskId, {
        onBeat: () => this._streamLog(taskId, logKey, logUploadName),
      }))
      this.heartbeat.seedLease(taskId, this.callback.popAckLeaseRemainingMs(taskId))

      // 2. 下载输入文件
      await this._guard(signal, this._downloadInputs(taskId, payload, inDir, taskType))

      // 3. params.json / files.json / meta.json
      const params = this._resolveParams(payload)
      fs.writeFileSync(path.join(taskDir, 'params.json'), JSON.stringify(params, null, 2), 'utf8')

      const filesData = this._parseFiles(payload)
      if (filesData && filesData.length) {
        fs.writeFileSync(path.join(taskDir, 'files.json'), JSON.stringify(filesData, null, 2), 'utf8')
      }
      fs.writeFileSync(path.join(taskDir, 'meta.json'), JSON.stringify({
        task_id: taskId,
        name: payload.name || '',                 // 仅供任务脚本【只读】
        task_type: taskType,
        steps,
        sub_step: subStep,
        end_step: endStep,
        timeout_seconds: payload.timeout_seconds || 172800,
        created_at: isoNow(),
      }, null, 2), 'utf8')

      // 4. 构建并执行命令
      const cmd = this._buildCmd(taskType, taskId)
      const timeout = payload.timeout_seconds || 172800

      // run_env（机器级）→ 插件追加 → fencing 令牌（最后写入，不可被覆盖）
      const runEnv = this._resolveRunEnv(taskType)
      const ext = this.plugin.extendEnv(taskType, taskId, { ...runEnv }) || {}
      for (const [k, v] of Object.entries(ext)) runEnv[String(k)] = String(v)
      Object.assign(runEnv, fencing.envFor(taskId))

      const safeSub = String(subStep).replace(/[^A-Za-z0-9._-]/g, '_')
      const logFilename = subStep ? `run_${safeSub}.log` : 'run.log'
      const returncode = await this._runSubprocess(cmd, taskId, taskDir, logFilename, runEnv, timeout, signal)

      // 因服务端信号而终止（cancelled / not_found）
      if (returncode === -1) {
        const srvStatus = this.heartbeat.getServerStatus(taskId)
        if (srvStatus === 'not_found') {
          logger.info(`[${taskType}] 任务 404(not_found) task_id=${taskId}`)
          this._writeOkJson(taskDir, taskId, 'not_found', false, { ...meta, notFoundAt: isoNow() })
        } else {
          logger.info(`[${taskType}] 任务被取消 task_id=${taskId}`)
          this._writeOkJson(taskDir, taskId, 'cancelled', true, meta)
        }
        await this._collectLog(taskId)
        return
      }
      if (returncode !== 0) throw new Error(`任务脚本退出码: ${returncode}`)

      // 5. 上传步骤日志（在 collectLog 关闭 writer 之前）
      await this._guard(signal, this._uploadStepLog(taskId, logKey, logUploadName))

      // 6. 收集本地路径（仅 ok.json 诊断用）
      const outDir = path.join(taskDir, 'out')
      const localResultPath = fs.readdirSync(outDir).length ? outDir : ''
      const localLogPath = (await this._collectLog(taskId)) || ''

      // 7. ok.json 持久化协议：先写后通知
      this._writeOkJson(taskDir, taskId, 'completed', false, { ...meta, resultPath: localResultPath, logPath: localLogPath })

      // 8. 回调：仅触发状态迁移，不传业务数据
      let success
      try {
        success = await this._guard(signal, this.callback.completeTask(taskId))
      } catch (e) {
        if (e instanceof Interrupted) throw e
        // 产物与 ok.json(completed) 都已就绪，只是通知没送达：绝不能落进「任务失败」分支，保持未确认交给 recovery / 补偿器
        logger.error(`[${taskType}] 完成回调异常，保持 completed 未确认 task_id=${taskId}: ${e.message}`)
        success = false
      }
      if (success) {
        this._writeOkJson(taskDir, taskId, 'completed', true, { ...meta, resultPath: localResultPath, logPath: localLogPath })
      }
      logger.info(`[${taskType}] 任务完成 task_id=${taskId}`)
    } catch (e) {
      if (e instanceof Interrupted) {
        // 刻意不写 ok.json：这条分支只有「进程正在退出」能触达，被打断的一轮什么都没确认；
        // 写了 server_confirmed=true 的终态会让 recovery 整个跳过该目录（不归还租约、不清理）。
        logger.warning(`[${taskType}] 任务被中断（进程退出中）task_id=${taskId}`)
        throw e
      }
      logger.error(`[${taskType}] 任务失败 task_id=${taskId}: ${e.message}`, e)
      const errorMsg = this._buildErrorMessage(taskId, e.message)
      await this._uploadStepLog(taskId, logKey, logUploadName)
      await this._collectLog(taskId)
      // 先写 ok.json(failed)，再调 fail。fail 失败不在 recovery 重试，由补偿器兜底（避免过期失败信号覆盖重投后的工作）
      this._writeOkJson(taskDir, taskId, 'failed', false, { ...meta, errorMessage: errorMsg })
      if (await this.callback.failTask(taskId, { errorMessage: errorMsg })) this._markOkConfirmed(taskDir)
    } finally {
      await this.heartbeat.unregister(taskId)
      this.streamedMarks.delete(taskId)
      this.streamedFields.delete(taskId)
    }
  }

  /** 心跳节拍驱动的增量日志上传。绝不抛异常（日志推送失败不得影响心跳与取消） */
  async _streamLog(taskId, logKey, logFilename) {
    const mark = this.logWriter.getWriteCount(taskId)
    if (mark === 0 || mark === this.streamedMarks.get(taskId)) return
    const content = this.logWriter.getTruncatedContent(taskId)
    if (!content) return
    const url = await this.callback.uploadResultFile(taskId, logFilename, Buffer.from(content, 'utf8'), { timeout: 35.0 })
    if (!url) return // 失败不更新水位，下次心跳重试
    this.streamedMarks.set(taskId, mark)
    if (!this.streamedFields.has(taskId)) {
      // 写成功才置位：失败则下一跳自动重来
      if (await this.callback.updateTaskFields(taskId, { logPath: JSON.stringify({ [logKey]: url }), mode: 'merge' })) {
        this.streamedFields.add(taskId)
      }
    }
  }

  _writeOkJson(taskDir, taskId, status, serverConfirmed, o = {}) {
    const data = {
      task_id: taskId,
      status,
      completed_at: isoNow(),
      server_confirmed: serverConfirmed,
      steps: o.steps || [],
      sub_step: o.subStep || '',
      end_step: o.endStep || '',
      result_path: o.resultPath || '',
      log_path: o.logPath || '',
    }
    // 租约令牌随终态标记落盘：进程崩溃重启后注册表是空的，recovery 重试 complete 要靠它
    const seq = fencing.leaseSeqOf(taskId)
    if (seq !== null) {
      data.worker_id = settings.WORKER_ID
      data.lease_seq = seq
    }
    if (serverConfirmed) data.server_confirmed_at = isoNow()
    if (o.notFoundAt) data.not_found_at = o.notFoundAt
    if (o.errorMessage) data.error_message = o.errorMessage
    try {
      fs.writeFileSync(path.join(taskDir, 'ok.json'), JSON.stringify(data, null, 2), 'utf8')
    } catch (e) {
      logger.error(`写入 ok.json 失败 task_id=${taskId}: ${e.message}`)
    }
  }

  _markOkConfirmed(taskDir) {
    const p = path.join(taskDir, 'ok.json')
    if (!fs.existsSync(p)) return
    try {
      const data = JSON.parse(fs.readFileSync(p, 'utf8'))
      data.server_confirmed = true
      data.server_confirmed_at = isoNow()
      fs.writeFileSync(p, JSON.stringify(data, null, 2), 'utf8')
    } catch (e) {
      logger.warning(`标记 ok.json confirmed 失败 path=${p}: ${e.message}`)
    }
  }

  _resolveParams(payload) {
    const raw = payload.params_text === undefined ? '{}' : payload.params_text
    if (raw && typeof raw === 'object') return raw
    if (typeof raw === 'string') {
      try { return JSON.parse(raw) } catch (_) {
        logger.warning(`params_text 解析失败: ${JSON.stringify(raw)}`)
        return {}
      }
    }
    return {}
  }

  /** files_json（字符串或数组）→ 数组；未知格式返回 null */
  _parseFiles(payload) {
    const raw = payload.files_json || payload.files
    if (!raw) return null
    if (typeof raw === 'string') return JSON.parse(raw)
    if (Array.isArray(raw)) return raw
    logger.warning(`未知的 files 格式: ${typeof raw}`)
    return null
  }

  async _downloadInputs(taskId, payload, inDir, taskType = '') {
    const list = this._parseFiles(payload)
    if (!list) return
    if (!Array.isArray(list)) {
      logger.warning('files_json 应为数组格式')
      return
    }
    for (const f of this.plugin.selectInputs(taskType, list)) {
      const filename = f.name || ''
      const local = path.resolve(inDir, filename)
      // 路径穿越防护（in/ + name 不得逃出 in/）
      if (!local.startsWith(inDir + path.sep)) throw new Error(`非法输入文件名: ${JSON.stringify(filename)}`)
      fs.mkdirSync(path.dirname(local), { recursive: true })
      logger.debug(`下载 ${filename} -> ${local}`)
      if (!(await this.httpClient.downloadFile(taskId, filename, local))) {
        throw new Error(`下载文件失败: ${filename}`)
      }
    }
  }

  _resolveRunEnv(taskType) {
    const runEnv = (this.channels[taskType] || {}).run_env || {}
    return Object.fromEntries(Object.entries(runEnv).map(([k, v]) => [String(k), String(v)]))
  }

  _buildCmd(taskType, taskId = '') {
    if (!(taskType in this.channels)) throw new Error(`未配置任务类型 ${taskType} 的执行环境`)
    const config = this.channels[taskType]
    const script = config.script
    const scriptPath = path.resolve(this.scriptRoot, script) // 绝对路径原样保留
    if (!fs.existsSync(scriptPath)) throw new Error(`任务脚本不存在: ${scriptPath}`)

    let cmd
    if (script.endsWith('.sh')) cmd = ['bash', scriptPath]
    else if (/\.(js|cjs|mjs)$/.test(script)) cmd = [process.execPath, scriptPath]
    else {
      const prefix = buildCmdPrefix(config)
      // conda run 直接 exec 脚本文件需要 +x；显式加 python 避免权限问题
      cmd = prefix[0] === 'conda' ? [...prefix, 'python', scriptPath] : [...prefix, scriptPath]
    }
    if (taskId) cmd.push('--task-id', taskId)
    return cmd
  }

  /** 注入子进程的环境：连接信息 / 存储 / 工作目录 / 上传开关 / IPC，及 SDK 的 NODE_PATH */
  _buildChildEnv(extra) {
    const env = { ...process.env, ...extra }
    // 任务脚本里 require('webx_nodejs_worker/src/sdk')：NODE_PATH 指向包目录的父目录
    const pkgParent = path.resolve(__dirname, '..', '..')
    const np = (env.NODE_PATH || '').split(path.delimiter).filter(Boolean)
    if (!np.includes(pkgParent)) np.unshift(pkgParent)
    env.NODE_PATH = np.join(path.delimiter)

    env[E.ENV_INTERNAL_TOKEN] = this.callback.internalToken
    env[E.ENV_WEB_BASE_URL] = this.callback.baseUrl
    env[E.ENV_STORAGE_URL] = this.callback.storageUrl
    env[E.ENV_STORAGE_INTERNAL_URL] = this.callback.storageInternalUrl
    const st = settings.STORAGE_INTERNAL_TOKEN.trim()
    if (st) env[E.ENV_STORAGE_INTERNAL_TOKEN] = st
    env[E.ENV_WORK_DIR] = this.workBaseDir
    env[E.ENV_UPLOADER_ENABLED] = settings.UPLOADER_ENABLED ? 'true' : 'false'
    env[E.ENV_UPLOAD_ASYNC_THRESHOLD_MB] = String(settings.UPLOAD_ASYNC_THRESHOLD_MB)
    env[E.ENV_UPLOAD_REQUIRED_GLOBS] = settings.UPLOAD_REQUIRED_GLOBS
    if (this.ipcSockPath) env[E.ENV_IPC_SOCK] = this.ipcSockPath
    return env
  }

  /**
   * 运行子进程，stdout / stderr 写入截断日志。
   * @returns {Promise<number>} 退出码；-1 表示因取消信号而终止；被信号杀死返回 128+signum
   */
  async _runSubprocess(cmd, taskId, workDir, logFilename = 'run.log', env = {}, timeout = 7200, signal = null) {
    const logPath = path.join(workDir, logFilename)
    const childEnv = this._buildChildEnv(env)
    const cm = getManager()
    const name = `task_${taskId.replace(/[^A-Za-z0-9_]/g, '_')}_${++this.procSeq}`
    const grace = killGraceSeconds()

    logger.info(`启动子进程 task_id=${taskId} cmd=${cmd.join(' ')}`)

    let child = null
    let exitInfo = null
    const exited = new Event()
    const drains = []
    // 写日志串行化：保证行序；日志处理永不影响任务判定（异常吞掉只告警）
    let chain = Promise.resolve()
    const feeder = () => {
      const asm = new LineAssembler()
      const write = (items) => {
        chain = chain.then(async () => {
          try {
            for (const [ln, isPartial] of items) await this.logWriter.write(taskId, logPath, ln + '\n', { partial: isPartial })
          } catch (e) { logger.warning(`[${taskId}] 日志写入异常，丢弃本段日志（不影响任务）：${e.message}`) }
        })
      }
      return {
        feed: (chunk) => { try { write(asm.feed(chunk)) } catch (e) { asm.reset(); logger.warning(`[${taskId}] 日志装配异常：${e.message}`) } },
        close: () => { try { write(asm.close()) } catch (_) { /* 尽力而为 */ } },
      }
    }

    cm.run({
      name,
      command: cmd[0],
      args: cmd.slice(1),
      restart: 'none',
      stopTimeout: Math.max(5, Math.round(grace * 1000)),
      options: { cwd: workDir, env: childEnv, detached: true, stdio: ['ignore', 'pipe', 'pipe'] },
      callback: (ch) => {
        child = ch
        for (const stream of [ch.stdout, ch.stderr]) {
          if (!stream) continue
          const f = feeder()
          drains.push(new Promise((resolve) => {
            stream.on('data', f.feed)
            stream.on('error', () => {})
            stream.on('close', () => { f.close(); resolve() })
          }))
        }
        ch.on('exit', (code, sig) => { exitInfo = { code, sig }; exited.set() })
        ch.on('error', (err) => { exitInfo = exitInfo || { error: err }; exited.set() })
      },
    })

    const pid = () => (child ? child.pid : 0)
    const cancelEvent = this.heartbeat ? this.heartbeat.getCancelEvent(taskId) : null

    // 终止：同步发 SIGTERM（cdpc 整树 + 进程组），宽限后 SIGKILL；返回时子进程已退出（或已被 SIGKILL）
    const terminate = async () => {
      if (exited.isSet()) return
      try { cm.stop(name, Math.max(5, Math.round(grace * 1000))) } catch (_) { /* 已退出 */ }
      killGroup(pid(), 'SIGTERM')
      if (!(await exited.waitFor(grace + 0.5))) killGroup(pid(), 'SIGKILL')
      await exited.waitFor(2)
    }

    let timeoutTimer = null
    const racers = [exited.wait().then(() => 'exit')]
    if (cancelEvent) racers.push(cancelEvent.wait().then(() => 'cancel'))
    racers.push(new Promise((resolve) => { timeoutTimer = setTimeout(() => resolve('timeout'), timeout * 1000); timeoutTimer.unref() }))
    if (signal) {
      racers.push(new Promise((resolve) => {
        if (signal.aborted) resolve('abort')
        else signal.addEventListener('abort', () => resolve('abort'), { once: true })
      }))
    }

    let outcome
    try {
      outcome = await Promise.race(racers)
      clearTimeout(timeoutTimer)

      if (outcome === 'cancel' || outcome === 'timeout' || outcome === 'abort') {
        await terminate()
        killGroup(pid(), 'SIGKILL')
        await this._waitDrains(drains)
        if (outcome === 'abort') throw new Interrupted()
        if (outcome === 'cancel') {
          logger.warning(`[${taskId}] 收到取消信号（status=${this.heartbeat.getServerStatus(taskId)}），已终止子进程`)
          return -1
        }
        logger.error(`子进程超时 task_id=${taskId}`)
        throw new Error(`计算超时（>${timeout}s）`)
      }

      // 组长已退出：给 drain 一个短宽限读完残余输出，然后无条件清理进程组里遗留的子孙
      await this._waitDrains(drains)
      killGroup(pid(), 'SIGKILL')
      await this._waitDrains(drains)
    } finally {
      clearTimeout(timeoutTimer)
      // 一切 await 之前同步兜底：即便展开途中被打断，也不泄漏算力
      if (!exited.isSet()) killGroup(pid(), 'SIGKILL')
      for (const s of child ? [child.stdout, child.stderr] : []) { try { s && s.destroy() } catch (_) { /* 已关闭 */ } }
      try { await chain } catch (_) { /* 日志链永不抛 */ }
      try { cm.remove(name) } catch (_) { /* 已被清理 */ }
    }

    if (exitInfo && exitInfo.error) throw new Error(`子进程启动失败: ${exitInfo.error.message}`)
    const { code, sig } = exitInfo || {}
    if (code !== 0) {
      let tail = ''
      try { tail = fs.readFileSync(logPath, 'utf8').slice(-500) } catch (_) { /* 无日志 */ }
      logger.error(`子进程退出码 ${code === null || code === undefined ? sig : code}，最后输出:\n${tail}`)
    }
    if (code === null || code === undefined) return 128 + (os.constants.signals[sig] || 0)
    return code
  }

  async _waitDrains(drains) {
    await Promise.race([Promise.all(drains), sleep(limits.DRAIN_GRACE_SEC)])
  }

  async _uploadStepLog(taskId, logKey, logFilename) {
    const content = this.logWriter.getTruncatedContent(taskId)
    if (!content) {
      logger.warning(`步骤日志为空，未上传: task_id=${taskId}, step=${logKey}`)
      return
    }
    try {
      const url = await this.callback.uploadResultFile(taskId, logFilename, Buffer.from(content, 'utf8'))
      if (!url) {
        logger.warning(`步骤日志上传未返回 URL，log_path 不会写入: task_id=${taskId}, step=${logKey}`)
        return
      }
      await this.callback.updateTaskFields(taskId, { logPath: JSON.stringify({ [logKey]: url }), mode: 'merge' })
    } catch (e) {
      logger.warning(`步骤日志上传失败 task_id=${taskId}: ${e.message}`)
    }
  }

  /** 收集截断日志存本地（仅诊断用）。返回保存路径，失败或无内容返回 '' */
  async _collectLog(taskId) {
    const content = this.logWriter.getTruncatedContent(taskId)
    try {
      if (!content) return ''
      const p = path.join(this.workBaseDir, taskId, 'run.truncated.log')
      fs.writeFileSync(p, content, 'utf8')
      return p
    } catch (e) {
      logger.warning(`保存截断日志失败 task_id=${taskId}: ${e.message}`)
      return ''
    } finally {
      await this.logWriter.closeTask(taskId)
    }
  }

  /** 进程级错误 + 日志尾部摘要合并成可读的 error_message */
  _buildErrorMessage(taskId, error, tailLines = 50) {
    const content = this.logWriter.getTruncatedContent(taskId)
    if (!content) return error
    const lines = content.replace(/\s+$/, '').split('\n')
    const tail = lines.length > tailLines ? lines.slice(-tailLines).join('\n') : content.replace(/\s+$/, '')
    return `${error}\n\n--- 进程输出尾部 ---\n${tail}`
  }
}

module.exports = { GenericTaskExecutor, limits, killGroup, killGraceSeconds }
