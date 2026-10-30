'use strict'

/**
 * uploader 进程的守护 —— 由 worker 父进程拉起、看住、退出时收走。
 *
 * 为什么由父进程 spawn 而不是独立 unit：生命周期必须与 worker 一致；配置只有一份。
 * ⚠️ 配置文件层（.env / 覆盖层）只存在于父进程内存，传不到子进程，所以按【真环境变量】显式注入。
 * 重启不需要恢复任何状态：uploader 的全部状态都在磁盘队列上，崩了重启接着扫目录。
 *
 * 与 Python 版的差异：
 *   · 拉起与重启交给 procManager 的 cdpc 单例（restart:'always' + restartDelay 固定间隔），
 *     不再自己写指数退避循环（K20 的「退避只增不减」随之不存在：固定间隔，无状态可增）。
 *   · 命令是 `process.execPath <bin/webx-worker.js> uploader`；子进程日志由它自己落盘（LOG_DIR 一并传入，K19）。
 *   · stop：先 disable（防止重启定时器把它再拉起来），cm.stop(name, 宽限) = SIGTERM 整树 + 宽限后 SIGKILL，
 *     然后轮询确认退出，最后 remove 注销。
 */

const path = require('node:path')
const E = require('./envnames')
const { settings, storageConfigured } = require('./config')
const { getManager } = require('./procManager')
const { sleep } = require('./aio')
const { getLogger } = require('./log')

const logger = getLogger('uploaderSupervisor')

const PROC_NAME = 'webx-uploader'
// 崩溃重启间隔（ms）：固定，兼顾「配错起来就死不至于每秒烧一个进程」与「修好后及时恢复」
const RESTART_DELAY_MS = 5000
// 退出宽限期（秒）：让在途分片写完并把 offset 落盘。不等传完，传不完下次启动接着传
const TERM_GRACE_SEC = 10.0

const UPLOADER_BIN = path.join(__dirname, '..', 'bin', 'webx-worker.js')

/** 构造子进程环境（cdpc 的 env 是追加语义，底子是 process.env）：用父进程【已定版】的配置显式覆盖 */
function childEnv() {
  return {
    [E.ENV_WORK_DIR]: settings.WORK_DIR,
    [E.ENV_WEB_BASE_URL]: settings.WEB_BASE_URL,
    [E.ENV_INTERNAL_TOKEN]: settings.INTERNAL_TOKEN,
    [E.ENV_STORAGE_INTERNAL_TOKEN]: settings.STORAGE_INTERNAL_TOKEN,
    [E.ENV_STORAGE_URL]: settings.STORAGE_URL,
    [E.ENV_STORAGE_INTERNAL_URL]: settings.STORAGE_INTERNAL_URL,
    WEBX_WORKER_UPLOADER_CONCURRENCY: String(settings.UPLOADER_CONCURRENCY),
    WEBX_WORKER_UPLOADER_SCAN_INTERVAL: String(settings.UPLOADER_SCAN_INTERVAL),
    WEBX_WORKER_LOG_DIR: settings.LOG_DIR || '',
    WEBX_WORKER_LOG_MAX_BYTES: String(settings.LOG_MAX_BYTES),
    WEBX_WORKER_LOG_MAX_LINES: String(settings.LOG_MAX_LINES),
  }
}

class UploaderSupervisor {
  /**
   * @param {object} [opts]  仅供测试：{name, command, args, restartDelay(ms)}，默认拉起真 uploader
   */
  constructor({ name = PROC_NAME, command = process.execPath, args = [UPLOADER_BIN, 'uploader'], restartDelay = RESTART_DELAY_MS } = {}) {
    this.name = name
    this._command = command
    this._args = args
    this._restartDelay = restartDelay
    this._started = false
  }

  start() {
    if (!settings.UPLOADER_ENABLED) {
      logger.info('[UploaderSup] 已禁用（WEBX_WORKER_UPLOADER_ENABLED=false），不拉起')
      return false
    }
    // 没有存储服务地址 → 拉起来也无处可传。与投递侧的闸对称；队列里已有的旧条目不会自己消失，故把积压说出来
    if (!storageConfigured()) {
      const backlog = pendingBacklogBytes()
      logger.error(
        `[UploaderSup] 未配置存储服务（${E.ENV_STORAGE_INTERNAL_URL} / ${E.ENV_STORAGE_URL}），不拉起 uploader —— ` +
        `产物只能上传到存储服务。大产物已自动改走同步上传；队列现有积压 ${(backlog / 1048576).toFixed(1)} MB，` +
        '需配好存储服务后重启 worker 方能补传。')
      return false
    }
    const chk = getManager().add({
      name: this.name,
      command: this._command,
      args: this._args,
      env: childEnv(),
      restart: 'always',
      restartDelay: this._restartDelay,
      options: { stdio: 'ignore' }, // 日志由子进程自己落盘
    })
    if (!chk) {
      logger.error('[UploaderSup] 拉起失败：cdpc 拒绝了进程配置')
      return false
    }
    this._started = true
    logger.info(`[UploaderSup] uploader 已交由 cdpc 看护 name=${this.name}`)
    return true
  }

  /** 当前子进程 pid；未运行返回 null */
  get pid() {
    const chk = this._started ? getManager().find(this.name) : null
    return chk && chk.child ? chk.child.pid : null
  }

  /** SIGTERM + 宽限期 + SIGKILL 兜底，并确认退出。幂等 */
  async stop(graceSec = TERM_GRACE_SEC) {
    if (!this._started) return
    this._started = false
    const cm = getManager()
    const chk = cm.find(this.name)
    if (!chk) return
    cm.disable(this.name) // 防止 restartDelay 窗口里的定时器把它再拉起来
    if (chk.child) {
      cm.stop(this.name, Math.round(graceSec * 1000)) // SIGTERM 整树，宽限后 SIGKILL
      const deadline = Date.now() + graceSec * 1000 + 2000
      while (chk.child && Date.now() < deadline) await sleep(0.05)
      if (chk.child) logger.warning('[UploaderSup] uploader 在宽限期 + 2s 后仍未确认退出')
    }
    // cdpc 的 stop 计时器只在到点时才收尾，进程提前退出后要自己清掉，免得拖住事件循环
    if (chk.stopTimer) { clearTimeout(chk.stopTimer); chk.stopTimer = null }
    cm.remove(this.name) // 若仍有残留，remove 会 SIGKILL
    logger.info('[UploaderSup] 已停止')
  }
}

// ─────────────────────────── 背压 ───────────────────────────

/** 当前队列积压字节数。读磁盘目录，无需与 uploader 通信；观测失败绝不能影响接任务 */
function pendingBacklogBytes() {
  try {
    return require('./sdk/uploadQueue').pendingBytes(settings.WORK_DIR)
  } catch (e) {
    logger.debug(`[UploaderSup] 读取积压失败: ${e && e.message}`)
    return 0
  }
}

/**
 * 积压是否已超过水位 —— 父进程接新任务前问一次。
 * 算得比传得快时队列会无限堆积（队列里的文件不能删），磁盘会被撑爆，这是唯一会拖垮机器的失败模式。
 * 注：与 Python 版一样，目前领取循环没有调用点（K9）。
 */
function isBackpressured() {
  const limitGb = settings.UPLOADER_PENDING_LIMIT_GB
  if (limitGb <= 0) return false
  const backlog = pendingBacklogBytes()
  const over = backlog > limitGb * 1024 ** 3
  if (over) {
    logger.warning(`[UploaderSup] 上传积压 ${(backlog / 1024 ** 3).toFixed(1)}GB 已超水位 ${limitGb}GB，暂缓接新任务`)
  }
  return over
}

module.exports = { UploaderSupervisor, childEnv, pendingBacklogBytes, isBackpressured, PROC_NAME, UPLOADER_BIN }
