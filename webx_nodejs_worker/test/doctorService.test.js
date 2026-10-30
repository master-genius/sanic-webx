'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const http = require('node:http')
const os = require('node:os')
const path = require('node:path')

const cfg = require('../src/config')
const doctor = require('../src/doctor')
const service = require('../src/service')
const { FakeTaskServer } = require('../src/fakeServer')

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-ds-'))

/** 跑一个函数并吞掉/收集 console.log、console.error 输出 */
async function capture(fn) {
  const lines = []
  const origLog = console.log
  const origErr = console.error
  console.log = (...a) => lines.push(a.join(' '))
  console.error = (...a) => lines.push(a.join(' '))
  try {
    return { result: await fn(), out: lines.join('\n') }
  } finally {
    console.log = origLog
    console.error = origErr
  }
}

/** 造一份可用的 home：.env + channel_config/gpu0.js + 脚本 */
function makeHome(webBase, { token = 'test-token' } = {}) {
  const home = tmp()
  fs.mkdirSync(path.join(home, 'channel_config'))
  fs.writeFileSync(path.join(home, 'demo.js'), "console.log('hi')\n")
  fs.writeFileSync(path.join(home, 'channel_config', 'gpu0.js'),
    `module.exports = { CHANNELS: { demo: { script: ${JSON.stringify(path.join(home, 'demo.js'))} } } }\n`)
  fs.writeFileSync(path.join(home, '.env'), [
    `WEBX_WORKER_WEB_BASE_URL=${webBase}`,
    `WEBX_WORKER_INTERNAL_TOKEN=${token}`,
    `WEBX_WORKER_STORAGE_URL=${webBase}`,
    `WEBX_WORKER_WORK_DIR=${path.join(home, 'work')}`,
  ].join('\n'))
  return home
}

const runDoctor = (home, extra = []) => capture(() => doctor.main(['--home', home, '--config', 'gpu0', '--timeout', '3', ...extra]))

test.afterEach(() => cfg.resetFileLayers())

test('doctor：对 FakeTaskServer 全通过，退出码 0', async () => {
  const srv = await new FakeTaskServer({ holdSeconds: 0.1 }).start()
  try {
    const { result, out } = await runDoctor(makeHome(srv.url))
    assert.equal(result, 0, out)
    for (const label of ['领取通道', '回调通道', '存储服务', '令牌', '工作目录', 'channel demo', 'Lane 0']) {
      assert.match(out, new RegExp(`\\[通过\\] ${label}`), out)
    }
    assert.match(out, /失败 0 项/)
  } finally {
    await srv.stop()
  }
})

test('doctor：令牌错 -> 退出码 1，三条通道都报 401', async () => {
  const srv = await new FakeTaskServer({ holdSeconds: 0.1 }).start()
  try {
    const { result, out } = await runDoctor(makeHome(srv.url, { token: 'wrong' }))
    assert.equal(result, 1, out)
    assert.match(out, /\[失败\] 领取通道 —— 401/)
    assert.match(out, /\[失败\] 回调通道 —— 401/)
    assert.match(out, /\[失败\] 存储服务 —— 401/)
  } finally {
    await srv.stop()
  }
})

test('doctor：路由 404（反代未转发）-> 退出码 1，领取通道指出 404', async () => {
  const srv = http.createServer((req, res) => { res.writeHead(404); res.end('{}') })
  await new Promise((r) => srv.listen(0, '127.0.0.1', r))
  try {
    const url = `http://127.0.0.1:${srv.address().port}`
    const { result, out } = await runDoctor(makeHome(url), ['--no-storage'])
    assert.equal(result, 1, out)
    assert.match(out, /\[失败\] 领取通道 —— 404/)
    assert.match(out, /\[通过\] 回调通道/) // 哨兵任务 404 即「可达」
    assert.doesNotMatch(out, /存储服务/)
  } finally {
    srv.closeAllConnections()
    await new Promise((r) => srv.close(r))
  }
})

test('doctor：连接失败 -> 1；缺配置 / 坏脚本 / 非法 --timeout', async () => {
  // 监听后立刻关掉，拿到一个确定无人监听的端口
  const probe = http.createServer()
  await new Promise((r) => probe.listen(0, '127.0.0.1', r))
  const dead = `http://127.0.0.1:${probe.address().port}`
  await new Promise((r) => probe.close(r))

  const home = makeHome(dead)
  let r = await runDoctor(home, ['--no-storage'])
  assert.equal(r.result, 1)
  assert.match(r.out, /\[失败\] 领取通道 —— 连接失败/)

  fs.rmSync(path.join(home, 'demo.js'))
  r = await runDoctor(home, ['--no-storage'])
  assert.match(r.out, /\[失败\] channel demo —— 脚本不存在/)

  r = await capture(() => doctor.main(['--home', home, '--config', 'nope', '--no-storage', '--timeout', '3']))
  assert.equal(r.result, 1)
  assert.match(r.out, /\[失败\] channel 配置 —— 不存在/)

  r = await capture(() => doctor.main(['--timeout', 'abc']))
  assert.equal(r.result, 2)
})

test('service one：单元含 ExecStart / TimeoutStopSec=30 / PrivateTmp=true，且 ExecStart 指向 node 与 bin', async () => {
  const out = tmp()
  const home = tmp()
  const { result } = await capture(() => service.run(['one', '--home', home, '--out-dir', out, '--unit-name', 'my-worker.service',
    '--config', 'gpu0', '--user', 'svc']))
  assert.equal(result, 0)
  const text = fs.readFileSync(path.join(out, 'my-worker.service'), 'utf8')
  assert.match(text, new RegExp(`^ExecStart=${process.execPath.replace(/[/.]/g, '\\$&')} ${service.BIN_PATH.replace(/[/.]/g, '\\$&')} run --config gpu0 \\$WORKER_ARGS$`, 'm'))
  assert.match(text, /^TimeoutStopSec=30$/m)
  assert.match(text, /^PrivateTmp=true$/m)
  assert.match(text, /^User=svc$/m)
  assert.match(text, /^SyslogIdentifier=my-worker$/m)
  assert.doesNotMatch(text, /__[A-Z_]+__/) // 占位符全部替换
})

test('service multi：模板单元 %i / target / 覆盖层样例互不相同的工作目录 / 残留基础名提示', async () => {
  const out = tmp()
  const home = tmp()
  // 预置另一基础名的残留，应触发拆除指引
  fs.writeFileSync(path.join(out, 'old-name@.service'), '')
  const { result, out: log } = await capture(() => service.run(['multi', '--home', home, '--out-dir', out, '--unit-name', 'w',
    '--instances', 'gpu0,gpu1', '--user', 'svc']))
  assert.equal(result, 0)

  const unit = fs.readFileSync(path.join(out, 'w@.service'), 'utf8')
  assert.match(unit, /^ExecStart=\S+ \S+ run --config %i$/m)
  assert.match(unit, /^TimeoutStopSec=30$/m)
  assert.match(unit, /^PrivateTmp=true$/m)
  assert.match(unit, /^PartOf=w\.target$/m)
  assert.match(unit, /^WantedBy=w\.target$/m)
  assert.match(unit, /^SyslogIdentifier=w-%i$/m)
  assert.match(fs.readFileSync(path.join(out, 'w.target'), 'utf8'), /^Description=webx_worker instances \(w group target\)$/m)

  const wd = (n) => fs.readFileSync(path.join(home, 'channel_config', `${n}_env`), 'utf8').match(/^WEBX_WORKER_WORK_DIR=(.+)$/m)[1]
  assert.notEqual(wd('gpu0'), wd('gpu1'))
  assert.equal(wd('gpu0'), path.join(home, 'tmp', 'tasks', 'gpu0'))

  assert.match(log, /其他基础名的单元文件: old-name@\.service/)
  assert.match(log, /systemctl disable --now old-name 'old-name@\*'/)

  // 覆盖层已存在则跳过，不覆盖用户编辑
  fs.appendFileSync(path.join(home, 'channel_config', 'gpu0_env'), '# mine\n')
  await capture(() => service.run(['multi', '--home', home, '--out-dir', out, '--unit-name', 'w', '--instances', 'gpu0']))
  assert.match(fs.readFileSync(path.join(home, 'channel_config', 'gpu0_env'), 'utf8'), /# mine/)
})

test('service：非法单元名 / 模式 / 未知参数 -> 退出码 2；旧 conda 参数被接受并忽略', async () => {
  const out = tmp()
  const base = ['--home', out, '--out-dir', out]
  for (const argv of [['one', ...base, '--unit-name', 'a@b'], ['one', ...base, '--unit-name', 'a/b'], ['one', ...base, '--unit-name', 'a b'],
    ['one', ...base, '--unit-name', ' '], ['bogus'], [], ['one', ...base, '--no-such-flag']]) {
    assert.equal((await capture(() => service.run(argv))).result, 2, JSON.stringify(argv))
  }
  const r = await capture(() => service.run(['one', ...base, '--unit-name', 'x', '--conda-env', 'myenv']))
  assert.equal(r.result, 0)
  assert.match(r.out, /--conda-env 在 Node 版已不适用/)
})
