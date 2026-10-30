'use strict'

/**
 * 统一 HTTP 出口 —— 对 gohttp 的薄封装，供 callback / lease / http_file_client / uploader / SDK 共用。
 *
 * 语义对齐 Python 版用的 httpx（移植时逻辑不用改）：
 *   · 4xx / 5xx 正常返回（看 res.status / res.ok），不抛；
 *   · 连接失败（ECONNREFUSED / DNS / TLS）抛原错误（err.code 为 Node 错误码）；
 *   · 超时抛 HttpTimeoutError（gohttp 的 {timeout:true} 被归一化成异常，对应 httpx.TimeoutException）；
 *   · 取消（signal.abort()）抛 AbortError（err.name === 'AbortError'）。
 * 超时单位是【秒】（对外接口统一）；gohttp 的超时是「无数据」超时，不是总时长。
 *
 * 链路健康记账（linkHealth.js）通过 setObserver() 挂接成旁路观测：摘掉它不影响任何功能路径。
 */

const { hcli } = require('gohttp')

class HttpTimeoutError extends Error {
  constructor(url, seconds) {
    super(`request timed out after ${seconds}s: ${url}`)
    this.name = 'HttpTimeoutError'
    this.code = 'WEBX_TIMEOUT'
  }
}

let observer = null

/** 挂接观测器：{onResponse(res, url), onError(err, url)}；传 null 摘除 */
function setObserver(o) { observer = o }

function notify(kind, a, url) {
  if (!observer) return
  try { observer[kind](a, url) } catch (_) { /* 旁路观测不得影响功能 */ }
}

/**
 * @param {string} method
 * @param {string} url      完整 URL
 * @param {object} [opts]   {headers, query, body(对象→JSON), rawBody(Buffer|string), timeout(秒), signal}
 * @returns gohttp 响应：{status, ok, headers, data(Buffer), text(), json()}
 */
async function request(method, url, opts = {}) {
  const { timeout = 30, ...rest } = opts
  const o = { method: method.toUpperCase(), ...rest, timeout: Math.round(timeout * 1000) }
  let res
  try {
    res = await hcli.request(url, o)
  } catch (err) {
    if (err && err.name !== 'AbortError') notify('onError', err, url)
    throw err
  }
  if (res && res.timeout) {
    const err = new HttpTimeoutError(url, timeout)
    notify('onError', err, url)
    throw err
  }
  notify('onResponse', res, url)
  return res
}

/** multipart 上传（POST）：{files:{field: path}, form:{k:v}, headers, timeout(秒)} */
async function upload(url, { files, form, headers, timeout = 60, signal } = {}) {
  let res
  try {
    res = await hcli.upload(url, { files, form, headers, timeout: Math.round(timeout * 1000), signal })
  } catch (err) {
    if (err && err.name !== 'AbortError') notify('onError', err, url)
    throw err
  }
  if (res && res.timeout) {
    const err = new HttpTimeoutError(url, timeout)
    notify('onError', err, url)
    throw err
  }
  notify('onResponse', res, url)
  return res
}

/** 安全取 JSON：解析失败返回 fallback（不抛） */
function jsonOf(res, fallback = null) {
  try { return res.json() } catch (_) { return fallback }
}

const isTimeout = (e) => Boolean(e) && (e.name === 'HttpTimeoutError' || e.code === 'ETIMEDOUT' || e.code === 'WEBX_TIMEOUT')
const isAbort = (e) => Boolean(e) && (e.name === 'AbortError' || e.code === 'ABORT_ERR')

module.exports = { request, upload, jsonOf, setObserver, HttpTimeoutError, isTimeout, isAbort }
