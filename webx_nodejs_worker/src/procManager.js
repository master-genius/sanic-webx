'use strict'

/**
 * 进程管理单例 —— 整个 worker 进程共用一个 cdpc 实例，管任务子进程与 uploader 子进程。
 *
 * 配置要点（已对照 cdpc 源码与实测）：
 *   · notExit:true     cdpc 的信号处理（SIGINT/SIGTERM/SIGABRT/SIGQUIT）立即 return，不替我们退出；
 *                      退出由 worker 自己的停机流程决定。cdpc 的 process 'exit' 钩子仍会兜底带走子树。
 *   · allowDetached    任务子进程需要 detached（独立进程组，pgid == pid），才能整组 killpg。
 *   · stateDir         detached 子进程的 pid 文件放进工作目录，不落到 /tmp。
 *   · SIGHUP           cdpc 在无人监听 SIGHUP 时会补一个「重载配置」的监听，这里先占位，避免误触发。
 */

const fs = require('node:fs')
const path = require('node:path')
const CDPC = require('cdpc')
const { settings } = require('./config')

let manager = null

function getManager() {
  if (manager) return manager
  if (process.listenerCount('SIGHUP') === 0) process.on('SIGHUP', () => {})
  const stateDir = path.join(settings.WORK_DIR, '.cdpc')
  fs.mkdirSync(stateDir, { recursive: true })
  // 清掉上一轮遗留的 pid 文件：cdpc 对同名 detached 服务会「接管」仍存活的旧进程且不调用 callback，
  // executor 将永远等不到退出事件。WORK_DIR 按约定由实例独占（K4），清理是安全的
  const pidsDir = path.join(stateDir, 'pids')
  try { for (const f of fs.readdirSync(pidsDir)) fs.rmSync(path.join(pidsDir, f), { force: true }) } catch (_) { /* 目录不存在 */ }
  manager = new CDPC({ debug: false, notExit: true, allowDetached: true, stateDir })
  return manager
}

/** 测试隔离用：丢弃单例（不停止其子进程，调用方自行收尾） */
function resetManager() { manager = null }

module.exports = { getManager, resetManager }
