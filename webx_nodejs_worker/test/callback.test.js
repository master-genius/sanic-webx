'use strict'

// callback.js + httpFileClient.js：fencing 头、403 reason 分流、分层重试与转 PendingReporter、ack 租约时长、上传、下载

const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const { settings } = require('../src/config')
const fencing = require('../src/fencing')
const { WebCallbackClient, ServerResult, injectFencingHeaders } = require('../src/callback')
const { HttpFileClient, quoteName } = require('../src/httpFileClient')
const { setGlobalReporter } = require('../src/pendingReporter')
const { FakeTaskServer } = require('../src/fakeServer')
const aio = require('../src/aio')

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-cb-'))

let srv
let cb
let queued // 被转入 PendingReporter 的条目

test.before(async () => {
  srv = await new FakeTaskServer({ holdSeconds: 0.1 }).start()
  Object.assign(settings, {
    WEB_BASE_URL: srv.url, INTERNAL_TOKEN: 'test-token', WORKER_ID: 'w1',
    STORAGE_INTERNAL_URL: srv.url, STORAGE_INTERNAL_TOKEN: '', STORAGE_URL: 'https://pub.example.com/',
  })
  setGlobalReporter({ enqueue: async (e) => { queued.push(e) } })
})

test.after(async () => {
  setGlobalReporter(null)
  await srv.stop()
})

test.beforeEach(() => {
  queued = []
  srv.calls.length = 0
  srv._rules.length = 0
  cb = new WebCallbackClient()
  cb.backoffBaseSec = 0.005
  cb.fieldsRetryDelaySec = 0.01
  settings.WEB_BASE_URL = srv.url
  settings.INTERNAL_TOKEN = 'test-token'
})

/** 建一个已被 w1 租下的 running 任务，并登记令牌 */
function leased(taskId, { seq = 1, status = 'running' } = {}) {
  const tid = srv.addTask('demo', { taskId })
  Object.assign(srv.tasks.get(tid), { worker_id: 'w1', lease_seq: seq, status })
  fencing.register(tid, seq)
  return tid
}

test('fencing 头自动注入；显式头不被覆盖；无租约不加头', async () => {
  const tid = leased('fence00000000001', { seq: 3 })
  try {
    assert.equal(await cb.completeTask(tid), true)
    const h = srv.callsTo('POST', '/complete$')[0].headers
    assert.equal(h['x-worker-id'], 'w1')
    assert.equal(h['x-lease-seq'], '3')
    assert.equal(h['x-internal-token'], 'test-token')

    const hdr = injectFencingHeaders(`http://x/internal/tasks/${tid}/steps`, { 'x-lease-seq': '1' })
    assert.deepEqual(hdr, { 'x-lease-seq': '1', 'X-Worker-Id': 'w1' }, '显式头（不分大小写）不覆盖')
    assert.deepEqual(injectFencingHeaders('http://x/internal/tasks/nolease0000000/steps', {}), {})
    assert.deepEqual(injectFencingHeaders('http://x/task/leases', {}), {})
  } finally { fencing.unregister(tid) }
})

test('updateStatus / updateStepStatus（位置参数与 options 对象）/ 心跳', async () => {
  const tid = leased('basic00000000001', { status: 'queued' })
  try {
    assert.equal(await cb.updateStatus(tid, 'running'), true)
    assert.equal(srv.task(tid).status, 'running')
    assert.equal(await cb.updateStepStatus(tid, ['a', 'b'], 'a', 'running', 'hi'), true)
    assert.equal(await cb.updateStepStatus(tid, { subStatus: 'done' }), true)
    assert.deepEqual(srv.callsTo('PATCH', '/steps$')[1].json, { sub_status: 'done' })
    assert.equal(srv.task(tid).sub_step, 'a')

    let r = await cb.updateHeartbeat(tid)
    assert.ok(r instanceof ServerResult)
    assert.equal(r.isOk, true)
    assert.equal(r.heartbeatStatus, 'running')

    srv.revokeLease(tid) // 撤销是 200 + status=revoked，不是 403
    r = await cb.updateHeartbeat(tid)
    assert.equal(r.isOk, true)
    assert.equal(r.heartbeatStatus, 'revoked')
    srv.inject('POST', '/heartbeats$', 502)
    assert.equal((await cb.updateHeartbeat(tid)).isError, true)
  } finally { fencing.unregister(tid) }
})

test('403 reason 分流：not_lessee / gone / already_final 只试 1 次且丢弃，不入队', async () => {
  const stale = leased('rej000000000001', { seq: 2 })
  fencing.register(stale, 1) // 本地令牌过期 → 服务端判 not_lessee
  const gone = leased('rej000000000002')
  srv.softDelete(gone)
  const fin = leased('rej000000000003', { status: 'completed' })
  try {
    for (const tid of [stale, gone, fin]) {
      srv.calls.length = 0
      assert.equal(await cb.completeTask(tid), false)
      assert.equal(srv.callsTo('POST', '/complete$').length, 1, `${tid} 应只试 1 次`)
    }
    assert.deepEqual(queued, [], '终态拒绝不入队')
    assert.equal(cb._fenceRejected.size, 0, '闩锁已被消费')
  } finally { [stale, gone, fin].forEach((t) => fencing.unregister(t)) }
})

test('403 无终态 reason / 401：只试 1 次，入队（带 fencing 快照），不丢', async () => {
  const tid = leased('auth00000000001', { seq: 4 })
  try {
    srv.inject('POST', '/complete$', 403, { body: { ok: false, reason: 'missing_fencing_token' } })
    assert.equal(await cb.completeTask(tid), false)
    assert.equal(srv.callsTo('POST', '/complete$').length, 1)
    assert.equal(queued.length, 1)
    assert.deepEqual(
      { task_id: queued[0].task_id, action: queued[0].action, payload: queued[0].payload, fencing: queued[0].fencing },
      { task_id: tid, action: 'complete_task', payload: {}, fencing: { worker_id: 'w1', lease_seq: 4 } })
    assert.equal(typeof queued[0].ts, 'number')

    queued.length = 0
    srv.calls.length = 0
    srv.inject('PUT', '/status$', 401)
    assert.equal(await cb.updateStatus(tid, 'running'), false)
    assert.equal(srv.callsTo('PUT', '/status$').length, 1, '401 不重试')
    assert.equal(queued[0].action, 'update_status')
    assert.deepEqual(queued[0].payload, { status: 'running' })
  } finally { fencing.unregister(tid) }
})

test('400/404 业务拒绝：不重试不入队', async () => {
  const tid = leased('biz00000000001', { status: 'pending' }) // pending 上 complete → 400
  try {
    assert.equal(await cb.completeTask(tid), false)
    assert.equal(srv.callsTo('POST', '/complete$').length, 1)
    srv.inject('POST', '/fail$', 404)
    assert.equal(await cb.failTask(tid, 'boom'), false)
    assert.deepEqual(queued, [])
  } finally { fencing.unregister(tid) }
})

test('5xx：前台重试 5 次；中途恢复则成功；耗尽后入队', async () => {
  const tid = leased('retry0000000001')
  try {
    srv.inject('POST', '/complete$', 502, { times: 2 })
    assert.equal(await cb.completeTask(tid), true)
    assert.equal(srv.callsTo('POST', '/complete$').length, 3)
    assert.deepEqual(queued, [])

    srv.calls.length = 0
    srv.setStatus(tid, 'running')
    srv.inject('POST', '/fail$', 503, { times: 99 })
    assert.equal(await cb.failTask(tid, 'oops'), false)
    assert.equal(srv.callsTo('POST', '/fail$').length, 5)
    assert.equal(queued.length, 1)
    assert.deepEqual(queued[0].payload, { error_message: 'oops' })
    assert.equal(queued[0].action, 'fail_task')

    srv._rules.length = 0
    assert.equal(await cb.failTask(tid, { errorMessage: 'opt' }), true)
    assert.equal(srv.task(tid).error_message, 'opt')
  } finally { fencing.unregister(tid) }
})

test('网络错（连接被拒）同样重试 5 次后入队', async () => {
  const tid = leased('net000000000001')
  try {
    settings.WEB_BASE_URL = 'http://127.0.0.1:1'
    assert.equal(await cb.updateStatus(tid, 'running'), false)
    assert.equal(queued.length, 1)
    assert.equal(queued[0].action, 'update_status')
  } finally { fencing.unregister(tid) }
})

test('ackLease：记录剩余租约并按流逝时间扣减，pop 只能取一次', async () => {
  const tid = leased('ack000000000001', { status: 'queued' })
  try {
    assert.equal(await cb.ackLease(tid), true)
    await aio.sleep(0.05)
    const ms = cb.popAckLeaseRemainingMs(tid)
    assert.ok(ms < 300000 && ms > 299000, `实际 ${ms}`)
    assert.equal(cb.popAckLeaseRemainingMs(tid), null)

    fencing.register(tid, 99) // ack 被 403：返回 false、不入队、闩锁不残留
    assert.equal(await cb.ackLease(tid), false)
    assert.deepEqual(queued, [])
    assert.equal(cb._fenceRejected.size, 0)
    assert.equal(cb.popAckLeaseRemainingMs(tid), null)
  } finally { fencing.unregister(tid) }
})

test('releaseLease：显式 fencing 头生效；被拒不抛', async () => {
  const tid = leased('rel000000000001')
  try {
    const h = fencing.headersFor(tid)
    fencing.unregister(tid) // 模拟停机：注册表已被清空，只能靠显式头
    assert.equal(await cb.releaseLease(tid, h), true)
    assert.equal(srv.task(tid).status, 'pending')
    assert.equal(await cb.releaseLease(tid), false) // 无令牌 → 403
  } finally { fencing.unregister(tid) }
})

test('getTaskStatus：兼容 data 包装、404、错误', async () => {
  const tid = srv.addTask('demo', { taskId: 'get0000000000001' })
  let r = await cb.getTaskStatus(tid)
  assert.equal(r.taskStatus, 'pending')
  srv.wrapTaskInfoInData = true
  try { r = await cb.getTaskStatus(tid) } finally { srv.wrapTaskInfoInData = false }
  assert.equal(r.taskStatus, 'pending')
  assert.equal((await cb.getTaskStatus('nope00000000000')).isNotFound, true)
  srv.inject('GET', '/internal/tasks/', 500)
  r = await cb.getTaskStatus(tid)
  assert.equal(r.isError, true)
  assert.equal(r.isOk, false)
})

test('updateTaskFields：merge 查询串、options 对象、空载荷、只重试 1 次且不入队', async () => {
  const tid = leased('fld000000000001')
  try {
    assert.equal(await cb.updateTaskFields(tid, '{"s1":"u1"}', null, 'merge'), true)
    assert.equal(await cb.updateTaskFields(tid, { logPath: '{"s2":"u2"}', mode: 'merge' }), true)
    assert.deepEqual(srv.task(tid).log_path, { s1: 'u1', s2: 'u2' })
    assert.equal(srv.callsTo('PATCH', '/fields$')[0].query.mode, 'merge')

    srv.calls.length = 0
    assert.equal(await cb.updateTaskFields(tid), true)
    assert.equal(srv.calls.length, 0, '没有字段不发请求')

    srv.inject('PATCH', '/fields$', 502, { times: 1 })
    assert.equal(await cb.updateTaskFields(tid, '{"s3":"u3"}'), true)
    assert.equal(srv.callsTo('PATCH', '/fields$').length, 2)

    srv.calls.length = 0
    srv.inject('PATCH', '/fields$', 502, { times: 9 })
    assert.equal(await cb.updateTaskFields(tid, '{"s4":"u4"}'), false)
    assert.equal(srv.callsTo('PATCH', '/fields$').length, 2, '首发 + 重试 1 次')

    srv.calls.length = 0
    srv._rules.length = 0
    srv.inject('PATCH', '/fields$', 403, { body: { ok: false, reason: 'not_lessee' } })
    assert.equal(await cb.updateTaskFields(tid, '{"s5":"u5"}'), false)
    assert.equal(srv.callsTo('PATCH', '/fields$').length, 1, '确定性拒绝不重试')
    assert.deepEqual(queued, [], '字段更新永不入队')
  } finally { fencing.unregister(tid) }
})

test('uploadResultFile：multipart 内容 / 文件名保真，返回对外 URL；无存储地址返回 null', async () => {
  const tid = 'up0000000000001'
  const content = Buffer.from('第一行\nline2 "q"\r\n')
  const url = await cb.uploadResultFile(tid, 'run 步骤"1".log', content, { timeout: 5 })
  assert.equal(url, `https://pub.example.com/public/tasks/${tid}/results/run 步骤"1".log`)
  assert.deepEqual(srv.stored(tid, 'run 步骤"1".log'), content)
  const call = srv.callsTo('POST', '/results$')[0]
  assert.equal(call.headers['x-internal-token'], 'test-token')
  assert.equal(call.headers['x-worker-id'], undefined, '存储服务不做 fencing')

  assert.equal(await cb.uploadResultFile(tid, 'a.txt', 'str内容', 5), 'https://pub.example.com/public/tasks/' + tid + '/results/a.txt')
  assert.equal(srv.stored(tid, 'a.txt').toString(), 'str内容')

  const saved = [settings.STORAGE_INTERNAL_URL, settings.STORAGE_URL]
  settings.STORAGE_INTERNAL_URL = ''
  settings.STORAGE_URL = ''
  try { assert.equal(await cb.uploadResultFile(tid, 'b', 'x'), null) } finally {
    [settings.STORAGE_INTERNAL_URL, settings.STORAGE_URL] = saved
  }
  srv.inject('POST', '/results$', 500)
  assert.equal(await cb.uploadResultFile(tid, 'c', 'x'), null)
})

test('httpFileClient：百分号编码文件名下载落盘 / 失败不留残骸 / 列表', async () => {
  assert.equal(quoteName("a b/中'(x)*!.txt"), 'a%20b%2F%E4%B8%AD%27%28x%29%2A%21.txt')
  const files = { 'my file 中文.txt': 'hello', 'a/b.txt': 'slash', 'big.bin': 'x'.repeat(300000) }
  const tid = srv.addTask('demo', { taskId: 'dl0000000000001', files })
  const d = tmp()
  const hc = new HttpFileClient()
  assert.deepEqual((await hc.listFiles(tid)).sort(), Object.keys(files).sort())
  for (const [name, body] of Object.entries(files)) {
    const dst = path.join(d, 'in', `${name.replace('/', '_')}`)
    assert.equal(await hc.downloadFile(tid, name, dst), true, name)
    assert.equal(fs.readFileSync(dst, 'utf8'), body)
  }
  assert.equal(await hc.downloadFile(tid, 'missing.txt', path.join(d, 'in', 'missing.txt')), false)
  assert.equal(fs.existsSync(path.join(d, 'in', 'missing.txt')), false)
  assert.deepEqual(fs.readdirSync(path.join(d, 'in')).filter((n) => n.startsWith('.dl-')), [], '临时目录已清理')
  assert.deepEqual(await hc.listFiles('nope00000000000'), [])
})
