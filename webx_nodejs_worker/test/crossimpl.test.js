'use strict'

// 跨实现验证：Node worker 对 Python 版 FakeTaskServer（协议参考实现）跑完整生命周期。
// 没有 python3 时跳过。
const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const readline = require('node:readline')
const { spawn, spawnSync } = require('node:child_process')

const BIN = path.resolve(__dirname, '..', 'bin', 'webx-worker.js')
const EXAMPLES = path.resolve(__dirname, '..', 'examples')
const PY = path.join(__dirname, 'support', 'py_fake_server.py')
const hasPython = spawnSync('python3', ['-c', 'import sys']).status === 0

test('Node worker 对 Python FakeTaskServer：成功 / 失败任务走通，写请求全部带令牌且无 403/404/405', { skip: !hasPython }, async (t) => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-x-'))
  const home = path.join(tmp, 'home')
  fs.mkdirSync(path.join(home, 'channel_config'), { recursive: true })
  fs.copyFileSync(path.join(EXAMPLES, 'channels.js'), path.join(home, 'channel_config', 'default.js'))
  fs.copyFileSync(path.join(EXAMPLES, 'demo_task.js'), path.join(home, 'channel_config', 'demo_task.js'))

  const py = spawn('python3', ['-I', PY], { stdio: ['ignore', 'pipe', 'inherit'] })
  const lines = readline.createInterface({ input: py.stdout })
  const it = lines[Symbol.asyncIterator]()
  let worker = null
  t.after(async () => {
    if (worker && worker.exitCode === null) worker.kill('SIGTERM')
    py.kill('SIGKILL')
    await new Promise((r) => setTimeout(r, 300))
    fs.rmSync(tmp, { recursive: true, force: true })
  })

  const head = JSON.parse((await it.next()).value)
  worker = spawn(process.execPath, [BIN, 'run', '--home', home], {
    env: {
      ...process.env, WEBX_WORKER_WEB_BASE_URL: head.url, WEBX_WORKER_INTERNAL_TOKEN: head.token,
      WEBX_WORKER_STORAGE_URL: head.url, WEBX_WORKER_WORK_DIR: path.join(tmp, 'work'),
      WEBX_WORKER_PREFIX: 'cross', WEBX_WORKER_LEASE_HOLD_SECONDS: '1', WEBX_WORKER_KILL_GRACE_SECONDS: '1',
      WEBX_WORKER_UPLOADER_ENABLED: 'false',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let out = ''
  worker.stdout.on('data', (d) => { out += d })
  worker.stderr.on('data', (d) => { out += d })

  const final = JSON.parse((await it.next()).value)
  assert.equal(final.ok, 'completed', out.slice(-1500))
  assert.equal(final.bad, 'failed')
  assert.match(final.bad_error, /退出码: 3/)
  assert.deepEqual(final.rejected, [], '不应有被 403/404/405 的请求')
  assert.deepEqual(final.writes_without_token, [])
  assert.ok(String(final.log_path.log || '').endsWith('/run.log'), JSON.stringify(final.log_path))
})
