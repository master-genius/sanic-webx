'use strict'

// 真进程 e2e：假服务端 + 真 worker 子进程 + 真信号。覆盖 executor 的取消 / 超时 / 租约被吊销 / 停机归还 / 孙进程清理。
const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn, spawnSync } = require('node:child_process')
const { FakeTaskServer } = require('../src/fakeServer')

const BIN = path.resolve(__dirname, '..', 'bin', 'webx-worker.js')
const SLEEP_MARK = '3141' // 孙进程 `sleep 3141`，用来检测是否被清理
const SLEEP_RE = 'sleep 314[1]' // 正则写法：pgrep / pkill 的参数不会匹配到自身命令行

const SCRIPT = `'use strict'
const { spawn } = require('node:child_process')
const fs = require('node:fs')
const p = JSON.parse(fs.readFileSync('params.json', 'utf8'))
if (p.grandchild) spawn('sleep', ['${SLEEP_MARK}'], { stdio: 'ignore' }).unref()
console.log('started')
setTimeout(() => process.exit(0), (p.sleep || 0) * 1000)
`

async function setup(t, taskParams, taskOpts = {}) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-e2e-'))
  const home = path.join(tmp, 'home')
  const work = path.join(tmp, 'work')
  fs.mkdirSync(path.join(home, 'channel_config'), { recursive: true })
  fs.writeFileSync(path.join(home, 'channel_config', 'long.js'), SCRIPT)
  fs.writeFileSync(path.join(home, 'channel_config', 'default.js'), "module.exports={CHANNELS:{long:{script:'long.js'}}}")
  const server = await new FakeTaskServer({ holdSeconds: 0.2 }).start()
  const tid = server.addTask('long', { params: taskParams, ...taskOpts })
  const proc = spawn(process.execPath, [BIN, 'run', '--home', home], {
    env: {
      ...process.env, WEBX_WORKER_WEB_BASE_URL: server.url, WEBX_WORKER_INTERNAL_TOKEN: server.token,
      WEBX_WORKER_WORK_DIR: work, WEBX_WORKER_PREFIX: 'e2e', WEBX_WORKER_LEASE_HOLD_SECONDS: '1',
      WEBX_WORKER_UPLOADER_ENABLED: 'false', WEBX_WORKER_KILL_GRACE_SECONDS: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let out = ''
  proc.stdout.on('data', (d) => { out += d })
  proc.stderr.on('data', (d) => { out += d })
  const exited = new Promise((r) => proc.on('exit', (c) => r(c)))
  t.after(async () => {
    if (proc.exitCode === null) {
      proc.kill('SIGTERM') // 先优雅停机，让 worker 自己带走在跑的子进程
      await Promise.race([exited, new Promise((r) => setTimeout(r, 5000))])
      if (proc.exitCode === null) proc.kill('SIGKILL')
    }
    await server.stop()
    spawnSync('pkill', ['-f', SLEEP_RE])
    fs.rmSync(tmp, { recursive: true, force: true })
  })
  const ok = (id = tid) => { try { return JSON.parse(fs.readFileSync(path.join(work, id, 'ok.json'), 'utf8')) } catch (_) { return {} } }
  const running = () => server.task(tid).status === 'running'
  const grandchildAlive = () => spawnSync('pgrep', ['-f', SLEEP_RE]).status === 0
  return { server, tid, proc, exited, ok, running, grandchildAlive, getOut: () => out }
}

test('用户取消：心跳感知 → 终止子进程组（含孙进程）→ ok.json cancelled', async (t) => {
  const c = await setup(t, { sleep: 60, grandchild: true })
  assert.ok(await c.server.waitFor(() => c.running() && c.grandchildAlive(), 20), c.getOut())
  c.server.cancelTask(c.tid)
  assert.ok(await c.server.waitFor(() => c.ok().status === 'cancelled', 40), c.getOut())
  assert.ok(await c.server.waitFor(() => !c.grandchildAlive(), 10), '孙进程应被清理')
  assert.equal(c.server.callsTo('POST', '/fail$').length, 0)
})

test('租约被吊销：worker 终止旧一轮子进程、不上报 fail，任务回池后被重新领取', async (t) => {
  const c = await setup(t, { sleep: 60, grandchild: true })
  assert.ok(await c.server.waitFor(() => c.running() && c.grandchildAlive(), 20), c.getOut())
  c.server.revokeLease(c.tid) // 回 pending，lease_seq 1 → 2
  // 旧一轮被终止后，同一 worker 会重新领取：lease_seq 变成 3，且新一轮重新进入 running
  assert.ok(await c.server.waitFor(() => c.server.task(c.tid).lease_seq === 3 && c.running(), 40), c.getOut())
  assert.equal(c.server.callsTo('POST', '/fail$').length, 0)
  assert.ok(/收到取消信号.*已终止子进程/.test(c.getOut()), '旧一轮应因 revoked 被终止')
})

test('子进程超时：按失败上报', async (t) => {
  const c = await setup(t, { sleep: 60, grandchild: true }, { timeoutSeconds: 1 })
  assert.ok(await c.server.waitFor(() => c.server.task(c.tid).status === 'failed', 30), c.getOut())
  assert.match(c.server.task(c.tid).error_message, /计算超时/)
  assert.ok(await c.server.waitFor(() => !c.grandchildAlive(), 10))
})

test('SIGTERM 停机：归还租约（回 pending、lease_seq+1），被打断的一轮不写 ok.json、不上报 fail', async (t) => {
  const c = await setup(t, { sleep: 60, grandchild: true })
  assert.ok(await c.server.waitFor(() => c.running() && c.grandchildAlive(), 20), c.getOut())
  const seq0 = c.server.task(c.tid).lease_seq
  c.proc.kill('SIGTERM')
  assert.equal(await c.exited, 0, c.getOut())
  const t1 = c.server.task(c.tid)
  assert.equal(t1.status, 'pending')
  assert.equal(t1.lease_seq, seq0 + 1)
  assert.deepEqual(c.ok(), {})
  assert.equal(c.server.callsTo('POST', '/fail$').length, 0)
  assert.ok(await c.server.waitFor(() => !c.grandchildAlive(), 10), '停机后不应遗留孙进程')
})
