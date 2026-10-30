'use strict'

/**
 * Worker 分发入口 —— 主动领取版：领取循环（lease.js）+ 周期清理循环。
 *
 * 没有本地队列、没有预取、没有需要保活的长寿连接；清理与分发无关，周期性起子进程清理过期任务目录。
 *
 * 与 Python 版的差异：
 *   · Python 靠「取消 run_worker 协程」触发收尾；Node 版有两条等价通道：第四个可选参数 `{signal}`，
 *     或返回的 promise 上的 `.stop()`（app.js 用它）。触发后：停领、归还在手租约、停清理、
 *     关闭 HTTP 客户端，然后 resolve。两者都不触发则一直运行。
 *   · 清理子进程命令：`process.execPath bin/webx-worker.js cleanup --work-dir <WORK_DIR> --quiet`
 *     （与 Python 的 `python -m webx_worker cleanup` 对应；bin 由主 agent 提供）。
 *   · 清理频率不再用 2s tick 计数器，直接按间隔 CLEANUP_INTERVAL_SEC（600s）定时，可被 abort 立即打断。
 *   · 停机时若清理子进程在跑，发 SIGTERM 收走，不留孤儿。
 */

const path = require('node:path')
const { spawn } = require('node:child_process')
const { settings } = require('./config')
const { getLogger } = require('./log')
const lease = require('./lease')

const logger = getLogger('broker')

/** 可调参数（测试里压短） */
const tuning = {
  CLEANUP_INTERVAL_SEC: 600, // 原 CLEANUP_TICKS(300) × TICK_SECONDS(2)
  // 清理子进程命令构造；测试里可替换
  cleanupCommand: () => [
    process.execPath,
    path.join(__dirname, '..', 'bin', 'webx-worker.js'),
    'cleanup', '--work-dir', settings.WORK_DIR, '--quiet',
  ],
}

/**
 * Worker 主循环：领取分发 + 后台清理。
 * @param {object} handlers     {channelName: handler} 路由表（保留参数以对齐签名；executor 自己持有路由）
 * @param laneManager           LaneManager（task_type → lane 映射，即能力声明）
 * @param executor              GenericTaskExecutor：execute(taskId, payload, signal)
 * @param {{signal?: AbortSignal}} [options]
 */
function runWorker(handlers, laneManager, executor, { signal } = {}) {
  const stopAc = new AbortController()
  const p = runWorkerImpl(laneManager, executor, signal, stopAc.signal)
  p.stop = () => stopAc.abort() // app.js 收尾时用：等价于外部 signal abort
  return p
}

async function runWorkerImpl(laneManager, executor, extSignal, stopSignal) {
  const dispatcher = new lease.LeaseDispatcher(laneManager, executor)
  const cleanupAc = new AbortController()
  const loopP = dispatcher.loop()
  const cleanupP = cleanupLoop(cleanupAc.signal)
  logger.info('[Worker] 已启动：主动领取分发 + 后台清理')

  let failure = null
  const stopped = new Promise((resolve) => {
    for (const sg of [extSignal, stopSignal]) {
      if (!sg) continue
      if (sg.aborted) return resolve()
      sg.addEventListener('abort', () => resolve(), { once: true })
    }
  })
  const loopFailed = loopP.then(() => new Promise(() => {}), (e) => { failure = e })

  try {
    await Promise.race([stopped, loopFailed])
    if ((extSignal && extSignal.aborted) || stopSignal.aborted) {
      // 这里不等在跑的任务：真正跑任务的 _runOne 登记在 lease.runningTasks，
      // 停机时由 app 的收尾三步取消（beginDrain → 捕获令牌 → cancelRunningTasks → 归还）
      logger.info('Worker 被中断：停止领新任务，归还在手租约后退出（不等在跑的任务）')
    }
  } finally {
    dispatcher.stop()
    cleanupAc.abort()
    // 归还在手租约：让任务立刻回池，而不是等租约自然到期。放 finally 里，正常与异常退出都覆盖
    try {
      await dispatcher.releaseAll()
    } catch (e) {
      logger.warning(`[lease] 归还在手租约时出错（将由租约到期兜底）：${e && e.message}`)
    }
    await Promise.allSettled([loopP, cleanupP])
    await lease.leaseClient.close()
    try { await require('./callback').callbackClient.close() } catch (_) { /* callback 未加载或无 close：忽略 */ }
    logger.info('[✓] Worker 已关闭')
  }
  if (failure) throw failure
}

// ─────────────────────────── 内部协程 ───────────────────────────

/** 周期清理：每 CLEANUP_INTERVAL_SEC 起一次清理子进程；signal abort 立即退出 */
async function cleanupLoop(signal) {
  logger.info(`[Cleanup] 启动，每 ${tuning.CLEANUP_INTERVAL_SEC}s 触发一次清理子进程`)
  for (;;) {
    await lease.sleepAbortable(tuning.CLEANUP_INTERVAL_SEC, signal)
    if (signal.aborted) return
    try {
      await launchCleanupSubprocess(signal)
    } catch (e) {
      logger.error(`[Cleanup] 子进程启动异常: ${e && e.message}`, e)
    }
  }
}

/** 起一次清理子进程（默认策略 failed=2d, completed=7d）；stdout 是 JSON 摘要，非零退出码记 warning */
function launchCleanupSubprocess(signal) {
  return new Promise((resolve, reject) => {
    const [cmd, ...args] = tuning.cleanupCommand()
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] })
    let out = ''
    let err = ''
    child.stdout.on('data', (d) => { out += d })
    child.stderr.on('data', (d) => { err += d })
    const kill = () => { try { child.kill('SIGTERM') } catch (_) { /* 已退出 */ } }
    if (signal) signal.addEventListener('abort', kill, { once: true })
    child.on('error', (e) => { if (signal) signal.removeEventListener('abort', kill); reject(e) })
    child.on('close', (code) => {
      if (signal) signal.removeEventListener('abort', kill)
      if (code !== 0) {
        logger.warning(`[Cleanup] 子进程退出码 ${code}, stderr: ${err.trim().slice(0, 500)}`)
        return resolve()
      }
      const summary = out.trim()
      try {
        const d = JSON.parse(summary)
        const errors = Array.isArray(d.errors) ? d.errors.length : 0
        logger.info(`[Cleanup] 子进程完成 scanned=${d.scanned || 0} purged=${d.purged || 0} ` +
          `skipped=${JSON.stringify(d.skipped || {})} errors=${errors}`)
      } catch (_) {
        logger.info(`[Cleanup] 子进程完成: ${summary.slice(0, 500)}`)
      }
      resolve()
    })
  })
}

module.exports = { runWorker, cleanupLoop, launchCleanupSubprocess, tuning }
