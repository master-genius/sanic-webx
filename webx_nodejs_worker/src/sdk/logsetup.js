'use strict'

/**
 * 任务子进程的日志配置 —— 任务脚本共用的单一入口。对应 Python sdk/logsetup.py。
 *
 * 级别策略：内部实现细节一律 debug（量大、没有上下文，会在 worker 的「首 N + 尾 M」截断窗口里挤掉真内容）；
 * 失败才 info 以上。默认 INFO ⟹ debug 行根本不会产生；排查时把 WEBX_WORKER_TASK_LOG_LEVEL=DEBUG 注入子进程。
 * 「不进前端」由 worker 侧的 redactors 在交付出口整行剔除，与级别无关；级别护的是截断窗口。
 *
 * ⚠️ .env / 覆盖层里写这个变量没用：文件层只对 worker 主进程可见，子进程只认真环境变量。
 *    要走 channel 配置的 run_env，或直接设为真环境变量。
 */

const E = require('../envnames')
const log = require('../log')
const { envStr } = require('./util')

/** 子进程日志级别的环境变量名 */
const LOG_LEVEL_ENV = E.ENV_TASK_LOG_LEVEL

// Python logging 的别名兜底；log.js 只有 DEBUG / INFO / WARNING / ERROR 四级
const ALIASES = { WARN: 'WARNING', CRITICAL: 'ERROR', FATAL: 'ERROR' }

/**
 * 配置任务子进程的日志级别，返回生效的级别（log.LEVELS 的数值）。
 * 在脚本主入口、业务逻辑之前调用一次。无效值静默回落 INFO（日志配置不该成为任务失败的原因）。
 */
function configureTaskLogging() {
  const raw = envStr(LOG_LEVEL_ENV).toUpperCase()
  const name = ALIASES[raw] || raw
  const level = Object.prototype.hasOwnProperty.call(log.LEVELS, name) ? name : 'INFO'
  log.setLevel(level)
  return log.LEVELS[level]
}

module.exports = { configureTaskLogging, LOG_LEVEL_ENV }
