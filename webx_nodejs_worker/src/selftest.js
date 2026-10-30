'use strict'

/**
 * `node bin/webx-worker.js selftest` —— 在本机起一个最小假服务端，跑一遍 worker 的完整生命周期。
 *
 * 无需任何外部依赖（不需要真服务、数据库、网络）：
 *   1. 起 fakeServer.FakeTaskServer（协议参考实现：强制 fencing、拒绝旧路径）；
 *   2. 在临时主目录里放好示例 channel 配置（examples/channels.js + demo_task.js）；
 *   3. 以真实子进程拉起 worker（与 systemd 托管时同一个入口），并开启 uploader（阈值 0 MB，产物走异步上传）；
 *   4. 提交两个任务：一个成功、一个故意失败，逐项核对协议行为；
 *   5. 向 worker 发 SIGTERM，核对优雅停机（退出码 0）。
 *
 * 它验证的是这份代码对这份协议的遵守，不是你的真实服务端 —— 后者用 doctor。
 * 退出码：0 = 全部通过；1 = 有失败项。
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn } = require('node:child_process')
const { parseArgs } = require('node:util')
const { FakeTaskServer } = require('./fakeServer')

const EXAMPLES = path.resolve(__dirname, '..', 'examples')
const BIN = path.resolve(__dirname, '..', 'bin', 'webx-worker.js')

class Checks {
  constructor() { this.failed = 0; this.total = 0 }

  check(ok, what, detail = '') {
    this.total += 1
    if (!ok) this.failed += 1
    console.log(`  ${ok ? '[通过]' : '[失败]'} ${what}${!ok && detail ? ` —— ${detail}` : ''}`)
    return ok
  }
}

async function run(timeout = 90, keep = false) {
  const ck = new Checks()
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'webx-nodejs-worker-selftest-'))
  const home = path.join(tmp, 'home')
  const work = path.join(tmp, 'work')
  fs.mkdirSync(path.join(home, 'channel_config'), { recursive: true })
  fs.copyFileSync(path.join(EXAMPLES, 'channels.js'), path.join(home, 'channel_config', 'default.js'))
  fs.copyFileSync(path.join(EXAMPLES, 'demo_task.js'), path.join(home, 'channel_config', 'demo_task.js'))

  console.log(`webx_nodejs_worker selftest —— 临时目录 ${tmp}\n`)
  const server = await new FakeTaskServer().start()
  console.log(`  假服务端 ${server.url}`)
  const okId = server.addTask('demo', { params: { sleep: 0.5 } })
  const badId = server.addTask('demo', { params: { sleep: 0.2, fail: true } })

  const env = {
    ...process.env,
    WEBX_WORKER_WEB_BASE_URL: server.url, WEBX_WORKER_INTERNAL_TOKEN: server.token,
    WEBX_WORKER_STORAGE_URL: server.url, WEBX_WORKER_WORK_DIR: work,
    WEBX_WORKER_PREFIX: 'selftest', WEBX_WORKER_LEASE_HOLD_SECONDS: '1',
    WEBX_WORKER_UPLOADER_ENABLED: 'true', WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB: '0',
    WEBX_WORKER_UPLOADER_SCAN_INTERVAL: '0.5',
  }
  const proc = spawn(process.execPath, [BIN, 'run', '--home', home], { env, stdio: ['ignore', 'pipe', 'pipe'] })
  let out = ''
  proc.stdout.on('data', (d) => { out += d })
  proc.stderr.on('data', (d) => { out += d })
  const exited = new Promise((resolve) => proc.on('exit', (code, sig) => resolve(code === null ? -1 : code)))

  const okJson = (tid) => {
    try { return JSON.parse(fs.readFileSync(path.join(work, tid, 'ok.json'), 'utf8')) } catch (_) { return {} }
  }

  let rc
  try {
    // 等两个任务的本地标记都“服务端确认”再停机：complete/fail 到 worker 写下 server_confirmed 之间还有一个往返
    const done = await server.waitFor(() =>
      server.task(okId).status === 'completed' && server.task(badId).status === 'failed' &&
      'report_url' in ((server.task(okId).result_path || {}).outputs || {}) &&
      okJson(okId).server_confirmed && okJson(badId).server_confirmed, timeout)
    console.log()
    ck.check(done, '两个任务在时限内走完生命周期（一个 completed、一个 failed）',
      `ok=${server.task(okId).status} bad=${server.task(badId).status}`)

    proc.kill('SIGTERM')
    rc = await Promise.race([exited, new Promise((r) => setTimeout(() => r('timeout'), 30000))])
    if (rc === 'timeout') { proc.kill('SIGKILL'); rc = -9 }
    ck.check(rc === 0, '收到 SIGTERM 后优雅退出（退出码 0）', `rc=${rc}`)
  } finally {
    if (proc.exitCode === null) proc.kill('SIGKILL')
    await server.stop()
  }

  const tOk = server.task(okId)
  const tBad = server.task(badId)
  const wid = 'selftest-default'

  // —— 协议遵守度 ——
  const leases = server.callsTo('POST', '^/task/leases$')
  ck.check(leases.length > 0 && JSON.stringify(leases[0].json.slots) === '[["demo"]]',
    '领取请求 POST /task/leases 带能力声明（空 LANES → 单 Lane 装下全部 channel）',
    JSON.stringify(leases.length ? leases[0].json : null))
  const seq = server.calls.filter((c) => c.path.startsWith(`/internal/tasks/${okId}`))
    .map((c) => `${c.method} ${c.path}`).filter((s) => /\/(ack|status|complete)$/.test(s))
  ck.check(seq[0] === `POST /internal/tasks/${okId}/ack` && seq[1] === `PUT /internal/tasks/${okId}/status` &&
    seq[seq.length - 1] === `POST /internal/tasks/${okId}/complete`,
  '认领后顺序：ack → 上报 running → … → complete', JSON.stringify(seq))
  const writes = server.calls.filter((c) => ['POST', 'PUT', 'PATCH', 'DELETE'].includes(c.method) &&
    c.path.startsWith('/internal/tasks/') && !/\/(results|results\/uploads|deliveries)$/.test(c.path))
  ck.check(writes.length > 0 && writes.every((c) => c.headers['x-worker-id'] === wid && c.headers['x-lease-seq']),
    '所有任务写请求都带 fencing 令牌（X-Worker-Id / X-Lease-Seq）')
  ck.check(writes.every((c) => c.status !== 403), '没有任何写请求被 fencing 拒绝（子进程令牌注入正常）',
    JSON.stringify(writes.filter((c) => c.status === 403).slice(0, 3).map((c) => [c.method, c.path])))
  ck.check(!server.calls.some((c) => (c.status === 404 || c.status === 405) && !c.path.startsWith('/internal/tasks/probe')),
    '没有请求命中过期路径/错误方法')

  // —— 子进程 SDK ——
  ck.check(tOk.sub_step === '' && server.callsTo('PATCH', `/internal/tasks/${okId}/steps$`).length > 0,
    '子进程经 SDK 上报了步骤进度（PATCH /steps）')
  ck.check(server.stored(okId, 'result.txt') !== undefined && 'report_url' in ((tOk.result_path || {}).outputs || {}),
    '产物 result.txt 已上传到存储服务并写入 result_path（阈值 0MB → 经 uploader 异步上传并交付）')
  ck.check(server.callsTo('POST', '/deliveries$').every((c) => !('x-worker-id' in c.headers)),
    '交付端点不带 fencing（uploader 手里没有租约令牌）')
  ck.check(((tOk.log_path || {}).log || '').endsWith('/run.log') && server.stored(okId, 'run.log') !== undefined,
    '步骤日志已上传并写入 log_path')

  // —— 失败路径 ——
  ck.check(tBad.status === 'failed' && tBad.error_message.includes('退出码: 3'),
    '失败任务上报 fail，错误信息带子进程退出码', tBad.error_message.slice(0, 120))

  // —— 本地完成标记 ——
  ck.check(okJson(okId).status === 'completed' && okJson(okId).server_confirmed === true,
    '本地完成标记 ok.json：先写后确认（completed / server_confirmed=true）')
  ck.check(okJson(badId).status === 'failed' && okJson(badId).server_confirmed === true,
    '失败任务的 ok.json 同样在服务端确认后标记已确认')

  console.log(`\n${ck.total - ck.failed}/${ck.total} 项通过${ck.failed ? `，${ck.failed} 项失败` : ''}`)
  if (ck.failed) console.log(`\n── worker 输出（末 4000 字符）──\n${out.slice(-4000)}`)
  if (keep || ck.failed) console.log(`\n临时目录保留在 ${tmp}`)
  else fs.rmSync(tmp, { recursive: true, force: true })
  return ck.failed ? 1 : 0
}

async function main(argv) {
  const { values } = parseArgs({
    args: argv, strict: false,
    options: { timeout: { type: 'string', default: '90' }, keep: { type: 'boolean', default: false } },
  })
  return run(Number(values.timeout) || 90, values.keep === true)
}

module.exports = { main, run }
