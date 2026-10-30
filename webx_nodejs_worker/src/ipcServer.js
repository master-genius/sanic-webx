'use strict'

/**
 * IPC Server —— AF_UNIX socket server，接收子进程经 SDK ipc_client.send_report 发来的「汇报失败事件」，
 * 转交 PendingReporter 入队。
 *
 * 通道：node:net 监听 Unix 域套接字（路径由主入口决定，默认 {WORK_DIR}/ipc.sock，经 WEBX_WORKER_IPC_SOCK 传给子进程）；
 * 子进程懒连接长连接，按行（\n）发 JSON，服务端逐行解析 → reporter.enqueueNowait。
 *
 * 容错：每个连接独立处理，单连接异常不影响其他；非法 JSON / 非法结构只告警并丢弃该行；客户端断开正常结束。
 *
 * 与 Python 版的差异：
 *   · 超长行（> 16MB）丢弃该行并告警，连接保持（Python 是 readline 抛错后关连接，同连接后续事件随之丢失）。
 *   · 监听期间临时收紧 umask(0o177)，socket 文件创建时即为 0600，不留 chmod 之前的窗口；listening 后再 chmod 兜底。
 *   · close() 超过 3 秒仍未断开的连接直接销毁（Python 只是打印告警后放手）。
 */

const fs = require('node:fs')
const net = require('node:net')
const path = require('node:path')
const { getLogger } = require('./log')

const logger = getLogger('ipc_server')

const IPC_LINE_LIMIT = 16 * 1024 * 1024 // 单行 JSON 事件的长度上限（字符）
const CLOSE_WAIT_SEC = 3.0

class IPCServer {
  /**
   * 构造参数顺序同 Python；也接受单个 options 对象 {sockPath, reporter}。
   * @param sockPath socket 文件路径（注意 Unix 套接字路径上限约 107 字节）
   * @param reporter 需提供 enqueueNowait(entry)
   */
  constructor(sockPath, reporter) {
    if (sockPath && typeof sockPath === 'object') ({ sockPath, reporter } = sockPath) // 也接受 {sockPath, reporter}
    this.sockPath = path.resolve(String(sockPath))
    this.reporter = reporter
    this._server = null
    this._conns = new Set()
    this.closeWaitSec = CLOSE_WAIT_SEC
  }

  /** 启动 IPC server，绑定 AF_UNIX socket */
  async start() {
    fs.mkdirSync(path.dirname(this.sockPath), { recursive: true })
    try { fs.unlinkSync(this.sockPath) } catch (e) { if (e.code !== 'ENOENT') throw e } // 清理残留 socket 文件

    const server = net.createServer((conn) => this._handleClient(conn))
    await new Promise((resolve, reject) => {
      server.once('error', reject)
      let oldMask = null
      try { oldMask = process.umask(0o177) } catch (_) { /* worker 线程不允许改 umask：靠下面的 chmod 兜底 */ }
      try {
        server.listen(this.sockPath, () => { server.off('error', reject); resolve() })
      } finally {
        if (oldMask !== null) process.umask(oldMask) // bind 在 listen() 内同步完成，umask 窗口只有这一小段
      }
    })
    server.on('error', (e) => logger.error(`[IPC] server 异常: ${e.message}`))
    try { fs.chmodSync(this.sockPath, 0o600) } catch (_) { /* 兜底，失败不致命 */ }
    this._server = server
    logger.info(`[IPC] Server 监听: ${this.sockPath}`)
  }

  /** 处理单个子进程连接，逐行解析 JSON 入队 */
  _handleClient(conn) {
    this._conns.add(conn)
    logger.debug('[IPC] 接受连接')
    conn.setEncoding('utf8') // 由流负责多字节字符跨 chunk 的拼接
    let buf = ''
    let skipping = false // 正在丢弃一条超长行，直到下一个换行

    const onLine = (line) => this._handleLine(line)

    conn.on('data', (chunk) => {
      let data = chunk
      if (skipping) {
        const nl = data.indexOf('\n')
        if (nl < 0) return
        skipping = false
        data = data.slice(nl + 1)
      }
      buf += data
      let nl
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl)
        buf = buf.slice(nl + 1)
        onLine(line)
      }
      if (buf.length > IPC_LINE_LIMIT) {
        logger.error(`[IPC] 单行超过 ${IPC_LINE_LIMIT} 字符，已丢弃该行（事件丢失）`)
        buf = ''
        skipping = true
      }
    })
    conn.on('end', () => { // 客户端关闭：末尾没有换行的残余也当作一行（对齐 readline 行为）
      if (buf.trim()) onLine(buf)
      buf = ''
    })
    conn.on('error', (e) => {
      if (e.code !== 'ECONNRESET' && e.code !== 'EPIPE') logger.error(`[IPC] 客户端处理异常: ${e.message}`)
    })
    conn.on('close', () => this._conns.delete(conn))
  }

  _handleLine(line) {
    if (!line.trim()) return
    let entry
    try {
      entry = JSON.parse(line)
    } catch (e) {
      logger.warning(`[IPC] 非法 JSON 行（已丢弃）: ${e.message}, raw=${JSON.stringify(line.slice(0, 200))}`)
      return
    }
    if (entry === null || typeof entry !== 'object' || Array.isArray(entry) || !entry.task_id) {
      logger.warning(`[IPC] 非法 entry 结构: ${JSON.stringify(entry)}`)
      return
    }
    try {
      this.reporter.enqueueNowait(entry)
    } catch (e) {
      logger.error(`[IPC] 入队失败: ${e.message}`)
    }
  }

  /**
   * 关闭 server 并清理 socket 文件。
   * 等已有连接断开最多 3 秒：避免子进程长连接未断开导致父进程退出阻塞；超时后销毁剩余连接。
   */
  async close() {
    const server = this._server
    this._server = null
    if (server) {
      const closed = new Promise((resolve) => server.close(() => resolve()))
      let timer
      const timedOut = new Promise((resolve) => { timer = setTimeout(() => resolve(true), this.closeWaitSec * 1000); timer.unref() })
      const to = await Promise.race([closed.then(() => false), timedOut])
      clearTimeout(timer)
      if (to) {
        logger.warning(`[IPC] 关闭等待超时（${this.closeWaitSec}s），强制断开 ${this._conns.size} 个连接`)
        for (const c of this._conns) c.destroy()
        await closed
      }
    }
    try { fs.unlinkSync(this.sockPath) } catch (e) { if (e.code !== 'ENOENT') logger.warning(`[IPC] 删除 socket 文件失败: ${e.message}`) }
    logger.info('[IPC] Server 已关闭')
  }
}

module.exports = { IPCServer, IPC_LINE_LIMIT }
