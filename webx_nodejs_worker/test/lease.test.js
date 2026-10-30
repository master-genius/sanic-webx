'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')

const { settings } = require('../src/config')
const fencing = require('../src/fencing')
const { Interrupted, sleep, monotonic } = require('../src/aio')
const { LaneManager } = require('../src/laneManager')
const { FakeTaskServer } = require('../src/fakeServer')
const lease = require('../src/lease')
const broker = require('../src/broker')

/** 起假服务端并把全局配置指过去 */
async function setup({ hold = 0.2, lanes = [['a'], ['b']], channels = ['a', 'b'] } = {}) {
  const s = await new FakeTaskServer({ holdSeconds: hold }).start()
  Object.assign(settings, {
    WORKER_ID: 'w1', INTERNAL_TOKEN: 'test-token', WEB_BASE_URL: s.url, TASK_SERVER_URL: '',
    LEASE_HOLD_SECONDS: 1, LEASE_MAX_LEVEL: '', LEASE_MAX_COMPUTE_POINTS: '',
  })
  Object.assign(lease.tuning, { ACK_FAIL_COOLDOWN_SEC: 0.05, NO_SLOT_SLEEP_SEC: 0.05, MIN_EMPTY_INTERVAL_SEC: 0.02, RUNNING_RETRY_SEC: 0.01 })
  lease._reset()
  return { s, lm: new LaneManager(lanes, channels) }
}

async function teardown(s, ...dispatchers) {
  for (const d of dispatchers) d.stop()
  await lease.leaseClient.close()
  await s.stop()
  lease._reset()
}

/** 可控的假 executor：每次 execute 记录入参，直到 release(taskId) 或 signal abort 才返回 */
function fakeExecutor({ onAbort = 'interrupt' } = {}) {
  const ex = { calls: [], gates: new Map() }
  ex.execute = (taskId, payload, signal) => new Promise((resolve, reject) => {
    ex.calls.push({ taskId, payload, signal })
    ex.gates.set(taskId, resolve)
    signal.addEventListener('abort', () => {
      ex.gates.delete(taskId)
      if (onAbort === 'interrupt') reject(new Interrupted())
      else resolve()
    }, { once: true })
  })
  ex.finish = (taskId) => { const g = ex.gates.get(taskId); if (g) { ex.gates.delete(taskId); g() } }
  return ex
}

const leaseCalls = (s) => s.callsTo('POST', '/task/leases$')

test('领取：请求体 slots、回包按 task_type 查 Lane、每个 Lane 一个槽位的背压', async () => {
  const { s, lm } = await setup()
  const ex = fakeExecutor()
  const d = new lease.LeaseDispatcher(lm, ex)
  try {
    const loopP = d.loop()
    await s.waitFor(() => leaseCalls(s).length >= 1, 3)
    // 两个 Lane 都空闲：slots 每 Lane 一个元素，且没有 n 参数；未配置筛选则不带 max_* 键
    const first = leaseCalls(s)[0].json
    assert.deepEqual(first.slots, [['a'], ['b']])
    assert.equal(first.worker_id, 'w1')
    assert.ok(!('max_level' in first) && !('max_compute_points' in first) && !('n' in first))
    assert.equal(leaseCalls(s)[0].headers['x-worker-id'], 'w1')

    // 只有 b 类任务：回包只有一个元素（服务端只返回认领成功的），必须按类型派给 Lane 1，不是按下标派给 Lane 0
    const tid = s.addTask('b', { params: { x: 1 } })
    await s.waitFor(() => ex.calls.length === 1, 3)
    assert.equal(ex.calls[0].taskId, tid)
    assert.equal(ex.calls[0].payload.task_type, 'b')
    assert.equal(ex.calls[0].payload.task_id, tid)
    assert.deepEqual(ex.calls[0].payload.steps, [])
    assert.equal(d._busy.get(1), true)
    assert.equal(d._busy.get(0), false)
    assert.equal(fencing.leaseSeqOf(tid), 1) // 令牌已登记
    assert.equal(s.task(tid).status, 'running') // ack + 上报 running 都做了

    // Lane 1 忙：之后的请求只带 Lane 0 的类型
    await s.waitFor(() => leaseCalls(s).some((c) => c.json.slots.length === 1), 3)
    const after = leaseCalls(s).filter((c) => c.t > leaseCalls(s)[0].t).pop().json
    assert.deepEqual(after.slots, [['a']])

    // 两个 Lane 都忙：不再发请求（背压）
    s.addTask('a')
    await s.waitFor(() => ex.calls.length === 2, 3)
    await sleep(0.15) // 让在途的最后一个请求落定
    const n = leaseCalls(s).length
    await sleep(0.5)
    assert.equal(leaseCalls(s).length, n, '满载时不应再发领取请求')

    // 释放一个 Lane：恢复领取，令牌注销
    ex.finish(tid)
    await s.waitFor(() => leaseCalls(s).length > n, 3)
    assert.equal(fencing.leaseSeqOf(tid), null)
    ex.finish(ex.calls[1].taskId)
    await d.releaseAll()
    d.stop()
    await loopP
  } finally {
    await teardown(s, d)
  }
})

test('领取：max_level / max_compute_points 配置后进请求顶层（0 有意义）', async () => {
  const { s, lm } = await setup()
  settings.LEASE_MAX_LEVEL = '0'
  settings.LEASE_MAX_COMPUTE_POINTS = '50'
  const d = new lease.LeaseDispatcher(lm, fakeExecutor())
  try {
    const loopP = d.loop()
    await s.waitFor(() => leaseCalls(s).length >= 1, 3)
    const body = leaseCalls(s)[0].json
    assert.equal(body.max_level, 0)
    assert.equal(body.max_compute_points, 50)
    d.stop()
    await loopP
  } finally {
    await teardown(s, d)
  }
})

test('drain：长轮询在途时开始停机，回包里的任务原样归还、不执行（K5）', async () => {
  const { s, lm } = await setup({ hold: 1 })
  const ex = fakeExecutor()
  const d = new lease.LeaseDispatcher(lm, ex)
  try {
    const loopP = d.loop()
    await s.waitFor(() => leaseCalls(s).length >= 1, 3)
    lease.beginDrain()
    const tid = s.addTask('a') // 唤醒挂起的请求 → 服务端认领并回包
    await loopP // drain 后循环自行结束
    assert.equal(ex.calls.length, 0, '排空期间领到的任务不能执行')
    assert.equal(s.task(tid).status, 'pending', '应已归还回池')
    assert.equal(s.task(tid).lease_seq, 2) // 认领 +1，归还回池 +1
    assert.equal(s.callsTo('DELETE', '/lease$').length, 1)
    assert.equal(fencing.leaseSeqOf(tid), null)
    assert.equal(lease.isDraining(), true)
  } finally {
    await teardown(s, d)
  }
})

test('停机三步：先捕获令牌 → 同步 abort → 并发归还；被中断的任务不上报 fail', async () => {
  const { s, lm } = await setup()
  const ex = fakeExecutor()
  const d = new lease.LeaseDispatcher(lm, ex)
  try {
    const loopP = d.loop()
    const t1 = s.addTask('a')
    const t2 = s.addTask('b')
    await s.waitFor(() => ex.calls.length === 2, 3)
    assert.equal(lease.runningTasks.size, 2)

    lease.beginDrain()
    const tokens = lease.heldFencingHeaders()
    assert.deepEqual(Object.keys(tokens).sort(), [t1, t2].sort())
    assert.equal(tokens[t1]['X-Lease-Seq'], '1')
    assert.equal(lease.cancelRunningTasks(), 2)
    assert.equal(lease.cancelRunningTasks(), 0, '重复调用不重复取消')
    assert.ok(ex.calls.every((c) => c.signal.aborted))

    // 取消后 _runOne 的 finally 会清空注册表：归还必须用预先捕获的令牌
    await s.waitFor(() => lease.runningTasks.size === 0, 3)
    assert.deepEqual(fencing.snapshot(), [])
    assert.equal(await lease.releaseHeldLeases(tokens), 2)
    assert.equal(s.task(t1).status, 'pending')
    assert.equal(s.task(t2).status, 'pending')
    assert.equal(s.callsTo('POST', '/fail$').length, 0)
    await loopP
  } finally {
    await teardown(s, d)
  }
})

test('ack 失败：放弃执行、不调 executor、Lane 冷却后释放', async () => {
  const { s, lm } = await setup()
  const ex = fakeExecutor()
  const d = new lease.LeaseDispatcher(lm, ex)
  try {
    s.inject('POST', '/ack$', 403, { times: 5 })
    const loopP = d.loop()
    const tid = s.addTask('a')
    await s.waitFor(() => s.callsTo('POST', '/ack$').length >= 1, 3)
    await s.waitFor(() => d._busy.get(0) === false && fencing.leaseSeqOf(tid) === null, 3)
    assert.equal(ex.calls.length, 0)
    assert.equal(s.callsTo('PUT', '/status$').length, 0, 'ack 失败后不应上报 running')
    d.stop()
    await loopP
  } finally {
    await teardown(s, d)
  }
})

test('executor 抛 Interrupted 只记 warning（不归还、不冷却）；其它异常才归还租约', async () => {
  const { s, lm } = await setup({ lanes: [['a']], channels: ['a'] })
  let mode = 'interrupted'
  const d = new lease.LeaseDispatcher(lm, {
    execute: async () => { throw (mode === 'interrupted' ? new Interrupted() : new Error('boom')) },
  })
  try {
    const loopP = d.loop()
    s.addTask('a', { taskId: 't-int' })
    await s.waitFor(() => d._busy.get(0) === false && s.task('t-int').status === 'running', 3)
    await sleep(0.1)
    assert.equal(s.callsTo('DELETE', '/lease$').length, 0)

    mode = 'boom'
    s.addTask('a', { taskId: 't-boom' })
    await s.waitFor(() => s.callsTo('DELETE', '/internal/tasks/t-boom/lease$').length === 1, 3)
    assert.equal(s.task('t-boom').status, 'pending') // 归还后回池
    d.stop()
    await loopP
  } finally {
    await teardown(s, d)
  }
})

test('K22：空领取有最小间隔，服务端 HOLD=0 也不会紧循环', async () => {
  const { s, lm } = await setup({ hold: 0 })
  lease.tuning.MIN_EMPTY_INTERVAL_SEC = 0.25
  const d = new lease.LeaseDispatcher(lm, fakeExecutor())
  try {
    const loopP = d.loop()
    await sleep(1.0)
    d.stop()
    await loopP
    const n = leaseCalls(s).length
    assert.ok(n >= 2 && n <= 6, `1s 内领取请求数应约 4，实际 ${n}`)
  } finally {
    await teardown(s, d)
  }
})

test('领取长连接可被 AbortSignal 取消；stop() 立即掐断挂起的请求', async () => {
  const { s, lm } = await setup({ hold: 5 })
  try {
    const ac = new AbortController()
    setTimeout(() => ac.abort(), 100)
    const t0 = monotonic()
    assert.equal(await lease.leaseClient.acquire([['a']], ac.signal), null)
    assert.ok(monotonic() - t0 < 2, '被取消后应立即返回')

    const d = new lease.LeaseDispatcher(lm, fakeExecutor())
    const loopP = d.loop()
    await s.waitFor(() => leaseCalls(s).length >= 2, 3)
    const t1 = monotonic()
    d.stop()
    await loopP
    assert.ok(monotonic() - t1 < 2, 'stop() 应掐断挂起的领取而不是等满 hold')
  } finally {
    await teardown(s)
  }
})

test('acquire：非 200 / 非 JSON 一律返回 null（退避），不抛', async () => {
  const { s } = await setup()
  try {
    s.inject('POST', '/task/leases$', 503, { times: 1 })
    assert.equal(await lease.leaseClient.acquire([['a']]), null)
    s.inject('POST', '/task/leases$', 401, { times: 1 })
    assert.equal(await lease.leaseClient.acquire([['a']]), null)
    s.inject('POST', '/task/leases$', 404, { times: 1 })
    assert.equal(await lease.leaseClient.acquire([['a']]), null)
    s.inject('POST', '/task/leases$', 200, { times: 1, body: '<html>proxy</html>' })
    assert.equal(await lease.leaseClient.acquire([['a']]), null)
    assert.deepEqual(await lease.leaseClient.acquire([['a']]), []) // 正常空返回
  } finally {
    await teardown(s)
  }
})

test('runWorker：stop() 触发收尾（停领 + 归还在手租约 + 关闭），cleanup 子进程命令可注入', async () => {
  const { s, lm } = await setup()
  const ex = fakeExecutor()
  // 清理循环：把间隔压短、命令换成一个立即打印 JSON 的 node -e
  broker.tuning.CLEANUP_INTERVAL_SEC = 0.1
  broker.tuning.cleanupCommand = () => [process.execPath, '-e', 'console.log(JSON.stringify({scanned:1,purged:0,skipped:{},errors:[]}))']
  const worker = broker.runWorker({}, lm, ex)
  try {
    const tid = s.addTask('a')
    await s.waitFor(() => ex.calls.length === 1, 3)
    assert.equal(typeof worker.stop, 'function')
    // 按 app.js 的顺序做停机三步，再停 worker
    lease.beginDrain()
    const tokens = lease.heldFencingHeaders()
    lease.cancelRunningTasks()
    await lease.releaseHeldLeases(tokens)
    worker.stop()
    await worker
    assert.equal(s.task(tid).status, 'pending')
    assert.equal(s.callsTo('POST', '/fail$').length, 0)
  } finally {
    await teardown(s)
  }
})
