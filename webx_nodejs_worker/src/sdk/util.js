'use strict'

/**
 * SDK 内部小工具（仅 sdk/ 内部使用，不属于对外 API）。
 * 只依赖 Node 标准库：SDK 跑在任务子进程里，不能引入 config.js 单例。
 */

/**
 * 退避睡眠（秒）。⚠️ 故意【不】unref：它是被 await 的在途工作，
 * 任务子进程里往往没有别的存活句柄，unref 会让脚本在重试中途悄悄退出。
 */
const sleep = (seconds) => new Promise((resolve) => setTimeout(resolve, Math.max(0, seconds) * 1000))

/** 读环境变量并 trim；未设置返回 '' */
const envStr = (name) => String(process.env[name] || '').trim()

/** 可重试的异常：排除编程错误（TypeError 等），其余（连接失败 / 超时 / 协议错误）都算传输层故障 */
const isTransient = (e) => !(e instanceof TypeError || e instanceof RangeError ||
  e instanceof SyntaxError || e instanceof ReferenceError)

/** 响应体前 n 个字符，永不抛（gohttp 在流出错时会给出没有 text() 的 status=0 响应） */
function bodySnippet(res, n = 200) {
  try {
    if (res && typeof res.text === 'function') return String(res.text()).slice(0, n)
    if (res && res.data) return Buffer.from(res.data).toString('utf8', 0, n)
  } catch (_) { /* 忽略 */ }
  return ''
}

/** 当前时间（浮点秒），与 Python time.time() 同口径 */
const nowSec = () => Date.now() / 1000

module.exports = { sleep, envStr, isTransient, bodySnippet, nowSec }
