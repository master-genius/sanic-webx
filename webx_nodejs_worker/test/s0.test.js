'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { hcli } = require('gohttp')

const cfg = require('../src/config')
const fencing = require('../src/fencing')
const { LaneManager } = require('../src/laneManager')
const { TaskKindPlugin } = require('../src/plugin')
const loader = require('../src/channelLoader')
const { FakeTaskServer } = require('../src/fakeServer')

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-'))

test('parseEnvText：引号 / export / 行内注释 / 取最后一行', () => {
  const o = cfg.parseEnvText('# c\nexport A=1\nB="x # y"\nC=v  # note\nA=2\nbad\n')
  assert.deepEqual(o, { A: '2', B: 'x # y', C: 'v' })
})

test('配置分层：环境变量 > 覆盖层 > .env；worker_id 派生稳定', () => {
  const d = tmp()
  fs.writeFileSync(path.join(d, '.env'), 'WEBX_WORKER_WEB_BASE_URL=http://base\nWEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB=7\n')
  fs.writeFileSync(path.join(d, 'gpu0_env'), 'WEBX_WORKER_WEB_BASE_URL=http://overlay\n')
  cfg.loadBaseEnv(path.join(d, '.env'))
  assert.equal(cfg.settings.WEB_BASE_URL, 'http://base')
  cfg.applyEnvOverlay(cfg.defaultOverlayFor(path.join(d, 'gpu0.js')))
  assert.equal(cfg.settings.WEB_BASE_URL, 'http://overlay')
  assert.equal(cfg.settings.UPLOAD_ASYNC_THRESHOLD_MB, 7)
  process.env.WEBX_WORKER_WEB_BASE_URL = 'http://env'
  cfg.refreshSettings()
  assert.equal(cfg.settings.WEB_BASE_URL, 'http://env')
  delete process.env.WEBX_WORKER_WEB_BASE_URL
  cfg.settings.WORKER_ID = ''
  cfg.settings.WORKER_PREFIX = 'host.x y'
  assert.equal(cfg.deriveWorkerId('gpu 0'), 'host-x-y-gpu-0')
  cfg.resetFileLayers()
})

test('KILL_GRACE_SECONDS 走配置分层（.env 可设，非法值回默认）', () => {
  const d = tmp()
  fs.writeFileSync(path.join(d, '.env'), 'WEBX_WORKER_KILL_GRACE_SECONDS=7.5\n')
  cfg.loadBaseEnv(path.join(d, '.env'))
  assert.equal(cfg.settings.KILL_GRACE_SECONDS, 7.5)
  cfg.resetFileLayers()
  assert.equal(cfg.settings.KILL_GRACE_SECONDS, 3)
})

test('leaseFilters：空串=不限，0 有意义', () => {
  cfg.settings.LEASE_MAX_LEVEL = '0'
  cfg.settings.LEASE_MAX_COMPUTE_POINTS = ''
  assert.deepEqual(cfg.leaseFilters(), [0, null])
  cfg.resetFileLayers()
})

test('LaneManager：空 LANES 归入一条 Lane；漏写/拼错的类型不进能力声明', () => {
  const one = new LaneManager([], ['a', 'b'])
  assert.deepEqual(one.typesOf(0), ['a', 'b'])
  const two = new LaneManager([['a', 'typo'], ['b']], ['a', 'b'])
  assert.deepEqual(two.typesOf(0), ['a'])
  assert.equal(two.getLane('b'), 1)
  assert.equal(two.getLane('zzz'), 0)
})

test('fencing：header / env 注入与终态拒绝判别', () => {
  cfg.settings.WORKER_ID = 'w1'
  assert.deepEqual(fencing.headersFor('t1'), {})
  fencing.register('t1', 3)
  assert.deepEqual(fencing.headersFor('t1'), { 'X-Worker-Id': 'w1', 'X-Lease-Seq': '3' })
  assert.equal(fencing.envFor('t1').WEBX_WORKER_LEASE_SEQ, '3')
  fencing.unregister('t1')
  assert.equal(fencing.leaseSeqOf('t1'), null)
  assert.equal(fencing.isTerminalReject({ ok: false, reason: 'gone' }), 'gone')
  assert.equal(fencing.isTerminalReject({ ok: false, reason: 'bad_lease_seq' }), null)
  assert.equal(fencing.isTerminalReject({ ok: true }), null)
})

test('channelLoader：加载、默认插件、命令前缀', () => {
  const d = tmp()
  const p = loader.resolveConfigPath('gpu0', d)
  assert.equal(p, path.join(d, 'channel_config', 'gpu0.js'))
  assert.equal(loader.ensureConfigExists(p), false) // 刚生成模板
  assert.throws(() => loader.loadChannelConfig(p), /未定义 CHANNELS/)
  fs.writeFileSync(p, "module.exports={CHANNELS:{demo:{script:'x.js',run_env:{A:1}}},LANES:[['demo']]}")
  const c = loader.loadChannelConfig(p)
  assert.ok(c.PLUGIN instanceof TaskKindPlugin)
  assert.equal(c.SCRIPT_ROOT, path.dirname(p))
  assert.deepEqual(loader.buildCmdPrefix({}), ['python'])
  assert.deepEqual(loader.buildCmdPrefix({ env: 'conda', env_name: 'e' }).slice(-2), ['-n', 'e'])
  assert.throws(() => loader.buildCmdPrefix({ env: 'conda' }), /env_name/)
})

test('FakeTaskServer：领取 / fencing / 撤销心跳 / 分片上传', async () => {
  const s = await new FakeTaskServer({ holdSeconds: 0.2 }).start()
  try {
    const H = { 'X-Internal-Token': 'test-token' }
    const tid = s.addTask('demo', { files: { 'a b.txt': 'hi' } })
    // 空 slots 一定 400；错令牌 401
    assert.equal((await hcli.post(`${s.url}/task/leases`, { headers: { ...H, 'X-Worker-Id': 'w' }, body: { slots: [] } })).status, 400)
    assert.equal((await hcli.post(`${s.url}/task/leases`, { headers: { 'X-Worker-Id': 'w' }, body: { slots: [['demo']] } })).status, 401)
    const r = await hcli.post(`${s.url}/task/leases`, { headers: { ...H, 'X-Worker-Id': 'w' }, body: { slots: [['demo']] } })
    const task = r.json().tasks[0]
    assert.equal(task.id, tid)
    assert.equal(task.lease_seq, 1)
    // 缺令牌 → not_lessee；带令牌 ack 成功
    let a = await hcli.post(`${s.url}/internal/tasks/${tid}/ack`, { headers: H, body: {} })
    assert.deepEqual([a.status, a.json().reason], [403, 'not_lessee'])
    const F = { ...H, 'X-Worker-Id': 'w', 'X-Lease-Seq': '1' }
    a = await hcli.post(`${s.url}/internal/tasks/${tid}/ack`, { headers: F, body: {} })
    assert.equal(a.json().ok, true)
    // 输入文件（名字百分号编码）
    const f = await hcli.get(`${s.url}/internal/tasks/${tid}/files/${encodeURIComponent('a b.txt')}`, { headers: H })
    assert.equal(f.data.toString(), 'hi')
    // 旧路径 404 / 错方法 405
    assert.equal((await hcli.post(`${s.url}/internal/tasks/${tid}/heartbeat`, { headers: F, body: {} })).status, 404)
    assert.equal((await hcli.get(`${s.url}/internal/tasks/${tid}/ack`, { headers: F })).status, 405)
    // 撤销后心跳 200 + revoked
    s.revokeLease(tid)
    const hb = await hcli.post(`${s.url}/internal/tasks/${tid}/heartbeats`, { headers: F, body: {} })
    assert.deepEqual([hb.status, hb.json().status], [200, 'revoked'])
    // 分片上传：offset 不符 409 + 响应头
    const c = await hcli.post(`${s.url}/internal/tasks/${tid}/results/uploads`, { headers: H, body: { filename: 'big.bin', total: 6 } })
    assert.equal(c.status, 201)
    const uid = c.json().upload_id
    let p = await hcli.request(`${s.url}/internal/uploads/${uid}`, { method: 'PATCH', headers: { ...H, 'Upload-Offset': '3' }, rawBody: Buffer.from('abc') })
    assert.deepEqual([p.status, p.headers['upload-offset']], [409, '0'])
    p = await hcli.request(`${s.url}/internal/uploads/${uid}`, { method: 'PATCH', headers: { ...H, 'Upload-Offset': '0' }, rawBody: Buffer.from('abcdef') })
    assert.equal(p.status, 200)
    const done = await hcli.post(`${s.url}/internal/uploads/${uid}/complete`, { headers: H, body: {} })
    assert.equal(done.json().size, 6)
    assert.equal(s.stored(tid, 'big.bin').toString(), 'abcdef')
    // 整传（multipart）
    const fs2 = require('node:fs')
    const tf = path.join(tmp(), 'x.bin')
    fs2.writeFileSync(tf, 'zzz')
    const up = await hcli.upload(`${s.url}/internal/tasks/${tid}/results`, { headers: H, files: { file: tf }, form: { filename: 'x.log' } })
    assert.equal(up.status, 201)
    assert.equal(s.stored(tid, 'x.log').toString(), 'zzz')
  } finally {
    await s.stop()
  }
})
