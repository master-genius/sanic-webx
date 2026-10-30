'use strict'

// pendingReporter.js + ipcServer.js：按响应体 reason 丢弃 / 保留、令牌取自条目、fail_task 前置校验、
// 快照落盘与恢复、consumer 循环与停机、IPC 收事件

const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const http = require('node:http')
const net = require('node:net')
const os = require('node:os')
const path = require('node:path')

const { settings } = require('../src/config')
const fencing = require('../src/fencing')
const { WebCallbackClient } = require('../src/callback')
const pr = require('../src/pendingReporter')
const { IPCServer } = require('../src/ipcServer')
const { FakeTaskServer } = require('../src/fakeServer')
const aio = require('../src/aio')

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-pr-'))

let srv
let cb
let rep
let dir

test.before(async () => {
  srv = await new FakeTaskServer({ holdSeconds: 0.1 }).start()
  Object.assign(settings, { WEB_BASE_URL: srv.url, INTERNAL_TOKEN: 'test-token', WORKER_ID: 'w1' })
})
test.after(async () => { await srv.stop() })

test.beforeEach(() => {
  srv.calls.length = 0
  srv._rules.length = 0
  settings.WEB_BASE_URL = srv.url
  dir = tmp()
  cb = new WebCallbackClient()
  rep = new pr.PendingReporter(cb, dir, { pollIntervalSec: 0.1 })
})

/** 服务端上已被 w1 租下的 running 任务（lease_seq 默认 1） */
function leased(taskId, { seq = 1, status = 'running' } = {}) {
  const tid = srv.addTask('demo', { taskId })
  Object.assign(srv.tasks.get(tid), { worker_id: 'w1', lease_seq: seq, status })
  return tid
}
const entry = (tid, action, payload = {}, fencingTok = { worker_id: 'w1', lease_seq: 1 }) => {
  const e = { task_id: tid, action, payload, context: {}, ts: 1 }
  if (fencingTok) e.fencing = fencingTok
  return e
}

test('200 出队；动作到协议方法 / 路径的映射正确', async () => {
  const tid = leased('map0000000000001', { status: 'queued' })
  assert.equal(await rep._tryReport(entry(tid, 'update_status', { status: 'running' })), 'success')
  assert.equal(srv.task(tid).status, 'running')
  assert.equal(await rep._tryReport(entry(tid, 'update_step_status', { steps: ['a'], sub_step: 'a' })), 'success')
  assert.deepEqual(srv.task(tid).steps, ['a'])
  assert.equal(await rep._tryReport(entry(tid, 'update_task_fields', { log_path: '{"a":"u"}', _mode: 'merge' })), 'success')
  assert.deepEqual(srv.task(tid).log_path, { a: 'u' })
  assert.equal(srv.callsTo('PATCH', '/fields$')[0].query.mode, 'merge')
  assert.equal(await rep._tryReport(entry(tid, 'complete_task')), 'success')
  assert.equal(srv.task(tid).status, 'completed')
  // 令牌同时走 body 与请求头
  const c = srv.callsTo('POST', '/complete$')[0]
  assert.equal(c.json.worker_id, 'w1')
  assert.equal(c.headers['x-lease-seq'], '1')
})

test('400 / 404 业务拒绝：丢弃；未知 action：丢弃且不发请求', async () => {
  const tid = leased('biz0000000000001')
  for (const code of [400, 404]) {
    srv.inject('POST', '/complete$', code)
    assert.equal(await rep._tryReport(entry(tid, 'complete_task')), 'drop')
  }
  srv.calls.length = 0
  assert.equal(await rep._tryReport(entry(tid, 'bogus_action')), 'drop')
  assert.equal(srv.calls.length, 0)
})

test('判据是响应体 reason：fencing 终态拒绝丢弃，401 / 无终态 reason 的 403 / 5xx / 网络错保留', async () => {
  const tid = leased('cls0000000000001')
  for (const reason of ['not_lessee', 'gone', 'already_final']) {
    srv.inject('POST', '/complete$', 403, { body: { ok: false, reason } })
    assert.equal(await rep._tryReport(entry(tid, 'complete_task')), 'drop', reason)
  }
  for (const [code, body] of [
    [401, { error: 'unauthorized' }],
    [403, { ok: false, reason: 'missing_fencing_token' }],
    [403, { ok: false, reason: 'bad_lease_seq' }],
    [403, { error: 'forbidden' }], // 没有 ok:false / reason：不是 fencing 拒绝
    [502, { error: 'bad gateway' }],
  ]) {
    srv.inject('POST', '/complete$', code, { body })
    assert.equal(await rep._tryReport(entry(tid, 'complete_task')), 'retry', `${code} ${JSON.stringify(body)}`)
  }
  settings.WEB_BASE_URL = 'http://127.0.0.1:1'
  assert.equal(await rep._tryReport(entry(tid, 'complete_task')), 'retry', '网络错保留')
  // 不能因为是 401 就被当成 403：401 带 fencing 终态形状的 body 也保留
  settings.WEB_BASE_URL = srv.url
  srv.inject('POST', '/complete$', 401, { body: { ok: false, reason: 'gone' } })
  assert.equal(await rep._tryReport(entry(tid, 'complete_task')), 'retry')
})

test('真实过期令牌：经服务端 reason 判为 drop；无令牌的条目同样是永久拒绝', async () => {
  const tid = leased('stale00000000001', { seq: 5 })
  assert.equal(await rep._tryReport(entry(tid, 'complete_task', {}, { worker_id: 'w1', lease_seq: 1 })), 'drop')
  assert.equal(srv.task(tid).status, 'running', '旧轮次的上报没有改动任何东西')
  assert.equal(await rep._tryReport(entry(tid, 'complete_task', {}, null)), 'drop')
})

test('重放用条目里的令牌，不被同任务的活注册表令牌顶替', async () => {
  const tid = leased('hijack0000000001', { seq: 1 })
  fencing.register(tid, 2) // 本机之后又领到了该任务的新一轮
  try {
    assert.equal(await rep._tryReport(entry(tid, 'update_step_status', { sub_step: 'x' }, { worker_id: 'w1', lease_seq: 1 })), 'success')
    assert.equal(srv.callsTo('PATCH', '/steps$')[0].headers['x-lease-seq'], '1')
    // 反过来：旧条目（seq 0）不能借新一轮令牌通过校验
    assert.equal(await rep._tryReport(entry(tid, 'update_step_status', { sub_step: 'y' }, { worker_id: 'w1', lease_seq: 0 })), 'drop')
    assert.equal(srv.task(tid).sub_step, 'x')
  } finally { fencing.unregister(tid) }
})

test('重放不修改条目：merge 模式可重复重放（K8）', async () => {
  const tid = leased('k80000000000001')
  const e = entry(tid, 'update_task_fields', { log_path: '{"a":"u"}', _mode: 'merge' })
  await rep._tryReport(e)
  await rep._tryReport(e)
  assert.equal(e.payload._mode, 'merge')
  assert.deepEqual(srv.callsTo('PATCH', '/fields$').map((c) => c.query.mode), ['merge', 'merge'])
})

test('fail_task 前置校验：仅 server 为 running 才重放，其余（已变更 / 不存在 / 读不到）丢弃', async () => {
  const tid = leased('fail000000000001')
  srv.setStatus(tid, 'queued') // 被补偿器重投
  assert.equal(await rep._tryReport(entry(tid, 'fail_task', { error_message: 'x' })), 'drop')
  assert.equal(srv.callsTo('POST', '/fail$').length, 0, '不得覆盖重投后的工作')
  assert.equal(await rep._tryReport(entry('nope000000000000', 'fail_task')), 'drop')

  srv.inject('GET', '/internal/tasks/', 500)
  assert.equal(await rep._tryReport(entry(tid, 'fail_task')), 'drop', '读不到状态：保守丢弃')

  srv.setStatus(tid, 'running')
  assert.equal(await rep._tryReport(entry(tid, 'fail_task', { error_message: 'boom' })), 'success')
  assert.equal(srv.task(tid).status, 'failed')
})

test('失败条目进 retryPool，下一轮扫描送达', async () => {
  const tid = leased('pool000000000001')
  srv.inject('POST', '/complete$', 502)
  await rep._handleEntry(entry(tid, 'complete_task'))
  assert.equal(rep._retryPool.length, 1)
  assert.equal(rep.pendingCount, 1)
  await rep._sweepRetryPool()
  assert.equal(rep._retryPool.length, 0)
  assert.equal(srv.task(tid).status, 'completed')
})

test('runLoop：新事件立即送达，失败的在下一个轮询间隔重试，requestStop 后退出', async () => {
  const t1 = leased('loop000000000001')
  const t2 = leased('loop000000000002')
  srv.inject('POST', '/complete$', 502, { times: 1 }) // 只让第一次失败
  const loop = rep.runLoop()
  rep.enqueueNowait(entry(t1, 'complete_task'))
  assert.ok(await srv.waitFor(() => srv.task(t1).status === 'completed', 3), '失败后应由轮询补发')
  await rep.enqueue(entry(t2, 'complete_task'))
  assert.ok(await srv.waitFor(() => srv.task(t2).status === 'completed', 3))
  rep.requestStop()
  await aio.withTimeout(loop, 2)
  assert.equal(rep.pendingCount, 0)
})

test('requestStop 中断在途请求：条目回到 retryPool，随快照落盘', async () => {
  const hang = http.createServer(() => {}) // 永不应答
  await new Promise((r) => hang.listen(0, '127.0.0.1', r))
  settings.WEB_BASE_URL = `http://127.0.0.1:${hang.address().port}`
  try {
    const loop = rep.runLoop()
    rep.enqueueNowait(entry('hang000000000001', 'update_status', { status: 'running' }))
    await aio.sleep(0.1)
    rep.requestStop()
    await aio.withTimeout(loop, 2) // 不必等 30s 请求超时
    assert.equal(rep._retryPool.length, 1)
    assert.equal(rep.snapshotToFile(), 1)
  } finally {
    settings.WEB_BASE_URL = srv.url
    hang.closeAllConnections()
    await new Promise((r) => hang.close(r))
  }
})

test('快照往返：排除 fail_task，恢复后删除快照文件；坏行被跳过', async () => {
  const tid = leased('snap000000000001')
  rep.enqueueNowait(entry(tid, 'update_status', { status: 'running' }))
  rep.enqueueNowait(entry(tid, 'fail_task', { error_message: 'x' }))
  rep._retryPool.push(entry(tid, 'complete_task'))
  assert.equal(rep.snapshotToFile(), 2)
  const file = path.join(dir, pr.SNAPSHOT_FILENAME)
  assert.ok(fs.existsSync(file))
  assert.equal(fs.existsSync(`${file}.tmp`), false)
  fs.appendFileSync(file, 'not json\n\n[1,2]\n')

  const rep2 = new pr.PendingReporter(cb, dir)
  assert.equal(await rep2.restoreFromSnapshot(), 2)
  assert.deepEqual(rep2._queue.map((e) => e.action), ['update_status', 'complete_task'])
  assert.equal(fs.existsSync(file), false)
  assert.equal(rep2.snapshotToFile() , 2)
})

test('恢复也读各任务目录的降级文件，且无快照时同样会读（K7）', async () => {
  const td = path.join(dir, 'task0000000000a')
  fs.mkdirSync(td)
  fs.writeFileSync(path.join(td, pr.TASK_PENDING_FILENAME),
    `${JSON.stringify(entry('task0000000000a', 'update_step_status', { sub_step: 'a' }))}\nbad line\n`)
  fs.writeFileSync(path.join(dir, 'stray.txt'), 'x')
  assert.equal(fs.existsSync(path.join(dir, pr.SNAPSHOT_FILENAME)), false)
  assert.equal(await rep.restoreFromSnapshot(), 1)
  assert.equal(rep._queue[0].task_id, 'task0000000000a')
  assert.equal(fs.existsSync(path.join(td, pr.TASK_PENDING_FILENAME)), false)
  assert.equal(await rep.restoreFromSnapshot(), 0)
})

test('enqueueGlobal：未设置全局实例不抛；设置后入队', async () => {
  pr.setGlobalReporter(null)
  await pr.enqueueGlobal({ task_id: 't', action: 'complete_task' })
  pr.setGlobalReporter(rep)
  try {
    await pr.enqueueGlobal({ task_id: 't', action: 'complete_task' })
    assert.equal(pr.getGlobalReporter(), rep)
    assert.equal(rep._queue.length, 1)
  } finally { pr.setGlobalReporter(null) }
})

// ─── IPC server ───────────────────────────────────────────────────

const connect = (p) => new Promise((resolve, reject) => {
  const c = net.createConnection(p, () => resolve(c))
  c.once('error', reject)
})
const endClient = (c) => new Promise((resolve) => { c.once('close', resolve); c.end() })

test('IPC：收行事件入队；坏 JSON / 缺 task_id 丢弃；超过 64KiB 的长行完整送达；末行无换行也收', async () => {
  const got = []
  const sock = path.join(tmp(), 'ipc.sock')
  fs.writeFileSync(sock, 'stale') // 残留文件应被清掉
  const ipc = new IPCServer(sock, { enqueueNowait: (e) => got.push(e) })
  await ipc.start()
  try {
    assert.equal(fs.statSync(sock).mode & 0o777, 0o600)
    const big = 'x'.repeat(200 * 1024)
    const c = await connect(sock)
    c.write(`${JSON.stringify({ task_id: 'a', action: 'update_step_status', payload: {} })}\n`)
    c.write('{not json}\n')
    c.write(`${JSON.stringify({ action: 'no_task_id' })}\n[1]\n`)
    c.write(`${JSON.stringify({ task_id: 'b', payload: { big, 中: '文' } })}\n`)
    c.write(JSON.stringify({ task_id: 'c' })) // 末行无换行，EOF 时处理
    await endClient(c)
    assert.ok(await srv.waitFor(() => got.length === 3, 2), `实际收到 ${got.length}`)
    assert.deepEqual(got.map((e) => e.task_id), ['a', 'b', 'c'])
    assert.equal(got[1].payload.big.length, big.length)
    assert.equal(got[1].payload['中'], '文')
  } finally { await ipc.close() }
  assert.equal(fs.existsSync(sock), false, '关闭后 socket 文件被清理')
})

test('IPC → PendingReporter：子进程降级事件经 socket 送达服务端；close 强制断开不听话的连接', async () => {
  const tid = leased('ipc0000000000001')
  const sock = path.join(tmp(), 'ipc.sock')
  const ipc = new IPCServer({ sockPath: sock, reporter: rep })
  ipc.closeWaitSec = 0.2
  await ipc.start()
  const loop = rep.runLoop()
  const c = await connect(sock)
  c.write(`${JSON.stringify(entry(tid, 'update_step_status', { sub_step: 'viaipc' }))}\n`)
  assert.ok(await srv.waitFor(() => srv.task(tid).sub_step === 'viaipc', 3))
  // 客户端不关连接：close 等 closeWaitSec 后强制断开，而不是无限期挂着
  await aio.withTimeout(ipc.close(), 3)
  c.destroy()
  rep.requestStop()
  await loop
  assert.equal(fs.existsSync(sock), false)
})
