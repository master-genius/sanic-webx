'use strict'

/**
 * fencing 令牌注册表 —— 「我现在持有哪些任务、各自是第几次租约」。
 *
 * 令牌 = (worker_id, lease_seq)，随每个写请求发送（请求头首选，JSON 体 / 表单第二通道）。
 * 子进程拿不到本进程内存，所以 executor 必须把令牌注入子进程环境变量（envFor）。
 */

const E = require('./envnames')
const { settings } = require('./config')

const REASON_NOT_LESSEE = 'not_lessee'
const REASON_GONE = 'gone'
const REASON_ALREADY_FINAL = 'already_final'

const leases = new Map() // task_id -> lease_seq

const register = (taskId, leaseSeq) => { leases.set(String(taskId), Number(leaseSeq)) }
const unregister = (taskId) => { leases.delete(String(taskId)) }
const snapshot = () => [...leases.keys()]
const leaseSeqOf = (taskId) => (leases.has(String(taskId)) ? leases.get(String(taskId)) : null)

/** fencing header；无租约返回空对象（协议一律强制 fencing，无令牌只会得到明确的 403） */
function headersFor(taskId) {
  const seq = leaseSeqOf(taskId)
  if (seq === null) return {}
  return { [E.HEADER_WORKER_ID]: settings.WORKER_ID, [E.HEADER_LEASE_SEQ]: String(seq) }
}

/** 注入任务子进程的 fencing 环境变量 */
function envFor(taskId) {
  const seq = leaseSeqOf(taskId)
  if (seq === null) return {}
  return {
    [E.ENV_WORKER_ID]: settings.WORKER_ID,
    [E.ENV_LEASE_SEQ]: String(seq),
    [E.ENV_TASK_ID]: String(taskId),
  }
}

/** 响应体是否为 fencing 的终态拒绝，是则返回 reason（调用方据此丢弃而不是重试） */
function isTerminalReject(payload) {
  if (!payload || typeof payload !== 'object' || payload.ok !== false) return null
  const r = payload.reason
  return [REASON_NOT_LESSEE, REASON_GONE, REASON_ALREADY_FINAL].includes(r) ? String(r) : null
}

module.exports = {
  REASON_NOT_LESSEE, REASON_GONE, REASON_ALREADY_FINAL,
  register, unregister, snapshot, leaseSeqOf, headersFor, envFor, isTerminalReject,
  HEADER_WORKER_ID: E.HEADER_WORKER_ID, HEADER_LEASE_SEQ: E.HEADER_LEASE_SEQ,
}
