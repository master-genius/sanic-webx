'use strict'

/**
 * Worker HTTP 文件下载客户端 —— 通过任务服务的 Internal API 下载任务的输入文件
 * （GET /internal/tasks/<id>/files 与 .../files/<name>，docs/worker-protocol.md §4.11）。
 *
 * 与 Python 版的差异：
 *   · 下载走 gohttp 的流式 download（httpClient.request 会把整个响应读进内存且封顶 100MB，不适合大输入文件）：
 *     先落到目标目录下的临时子目录，成功后 rename 为 localPath，失败 / 中断清理临时目录，不留半截文件。
 *     httpClient 没有 download 封装，所以这里直接引用 gohttp 的 hcli，并手动把结果喂给 linkHealth 的 observer，
 *     保持「所有出口都被观测」。
 *   · 同样挂 fencing 头（injectFencingHeaders）：协议 §4 规定内部端点「除特别说明外都需要 fencing」。
 *   · 每次请求现取配置，无客户端对象，close() 为空操作。
 */

const fs = require('node:fs')
const path = require('node:path')
const { hcli } = require('gohttp')
const E = require('./envnames')
const { settings } = require('./config')
const httpClient = require('./httpClient')
const { injectFencingHeaders } = require('./callback')
const { observer } = require('./linkHealth')
const { getLogger } = require('./log')

const logger = getLogger('http_file_client')

const DEFAULT_TIMEOUT_SEC = 30.0 // gohttp 语义：连续这么久没收到数据才算超时，不是总时长

/** 文件名百分号编码（含空格 / 中文 / 斜杠的名字原样拼进路径会 404 或被误解析）；对齐 Python quote(safe='') */
const quoteName = (name) => encodeURIComponent(name).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`)

class HttpFileClient {
  get baseUrl() { return settings.WEB_BASE_URL }

  get internalToken() { return settings.INTERNAL_TOKEN }

  async close() {}

  _headers(url) {
    return injectFencingHeaders(url, { [E.HEADER_INTERNAL_TOKEN]: this.internalToken })
  }

  /**
   * 下载文件到本地。
   * @returns 是否下载成功
   */
  async downloadFile(taskId, filename, localPath, timeout = DEFAULT_TIMEOUT_SEC) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}/files/${quoteName(filename)}`
    const dir = path.dirname(path.resolve(localPath))
    let tmpDir = null
    try {
      fs.mkdirSync(dir, { recursive: true })
      tmpDir = fs.mkdtempSync(path.join(dir, '.dl-'))
      const res = await hcli.download(url, {
        headers: this._headers(url), dir: tmpDir, timeout: Math.round(timeout * 1000), cleanPartial: true,
      })
      if (res && res.timeout) {
        const err = new httpClient.HttpTimeoutError(url, timeout)
        observer.onError(err, url)
        throw err
      }
      const names = fs.readdirSync(tmpDir)
      if (names.length !== 1) throw new Error(`下载结果异常：临时目录内有 ${names.length} 个文件`)
      fs.renameSync(path.join(tmpDir, names[0]), localPath)
      observer.onResponse({ status: 200 }, url)
      return true
    } catch (e) {
      // gohttp 对 4xx/5xx 抛 "Download failed: <status>"：还原成状态码，既给日志也给链路观测
      const m = /^Download failed: (\d{3})$/.exec((e && e.message) || '')
      if (m) {
        observer.onResponse({ status: Number(m[1]) }, url)
        logger.error(`下载文件失败: ${taskId}/${filename}, 状态码: ${m[1]}`)
      } else {
        if (!(e instanceof httpClient.HttpTimeoutError)) observer.onError(e, url)
        logger.error(`下载文件异常: ${taskId}/${filename}, 错误: ${e && e.message}`)
      }
      return false
    } finally {
      if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  }

  /** 列出任务的所有文件名 */
  async listFiles(taskId) {
    const url = `${this.baseUrl}/internal/tasks/${taskId}/files`
    try {
      const res = await httpClient.request('GET', url, { headers: this._headers(url), timeout: DEFAULT_TIMEOUT_SEC })
      if (res.status === 200) {
        const data = res.json()
        return (data && data.files) || []
      }
      logger.error(`列出文件失败: ${taskId}, 状态码: ${res.status}`)
      return []
    } catch (e) {
      logger.error(`列出文件异常: ${taskId}, 错误: ${e && e.message}`)
      return []
    }
  }
}

// 全局单例
const httpFileClient = new HttpFileClient()

module.exports = { HttpFileClient, httpFileClient, quoteName }
