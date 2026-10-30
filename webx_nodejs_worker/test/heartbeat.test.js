'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')

const { sleep } = require('../src/aio')
const { HeartbeatManager, HEARTBEAT_INTERVALS } = require('../src/heartbeat')

// 脚本化假 callback：script(n) 返回第 n 次心跳的 ServerResult 形态
const ok = (data) => ({ data, isNotFound: false, isError: false })
const notFound = () => ({ data: null, isNotFound: true, isError: false })
const netErr = () => ({ data: null, isNotFound: false, isError: true })
function stub(script) {
  const c = { calls: 0, async updateHeartbeat() { c.calls += 1; return script(c.calls) } }
  return c
}
// 时间参数压缩到毫秒级：节拍 10ms，自熔余量 0.1s
const mk = (client, extra = {}) => new HeartbeatManager(30, { intervals: [0.01], selfFenceMarginSec: 0.1, callbackClient: client, ...extra })

test('默认节拍序列 5,7,11,...,29', () => {
  assert.deepEqual(HEARTBEAT_INTERVALS, [5, 7, 11, 13, 17, 19, 23, 29])
})

test('撤销 (200 + revoked) 与 cancelled 触发取消 Event，并记录服务端状态', async () => {
  for (const status of ['revoked', 'cancelled']) {
    const hb = mk(stub((n) => (n < 3 ? ok({ status: 'running', lease_until: 1, lease_remaining_ms: 60000 }) : ok({ ok: false, status }))))
    await hb.register('t1')
    const ev = hb.getCancelEvent('t1')
    assert.equal(ev.isSet(), false)
    assert.equal(await ev.waitFor(2), true)
    assert.equal(hb.getServerStatus('t1'), status)
    await hb.unregister('t1')
  }
})

test('404 → not_found 并取消；未注册任务无 Event、状态默认 running', async () => {
  const c = stub(() => notFound())
  const hb = mk(c)
  assert.equal(hb.getCancelEvent('nope'), null)
  assert.equal(hb.getServerStatus('nope'), 'running')
  await hb.register('t1')
  assert.equal(await hb.getCancelEvent('t1').waitFor(2), true)
  assert.equal(hb.getServerStatus('t1'), 'not_found')
  await sleep(0.05)
  assert.equal(c.calls, 1, '取消后心跳循环应停止')
  await hb.unregister('t1')
})

test('起跑前 queued 视为存活；见过 running 之后 queued 即取消', async () => {
  // queued, queued, running, queued(→ 取消)
  const seq = ['queued', 'queued', 'running', 'queued']
  const c = stub((n) => ok({ status: seq[n - 1], lease_until: 1, lease_remaining_ms: 60000 }))
  const hb = mk(c)
  await hb.register('t1')
  const ev = hb.getCancelEvent('t1')
  await sleep(0.03) // 已过前两跳：queued 未取消
  assert.ok(c.calls >= 1)
  if (c.calls < 3) assert.equal(ev.isSet(), false)
  assert.equal(await ev.waitFor(2), true)
  assert.equal(c.calls, 4)
  assert.equal(hb.getServerStatus('t1'), 'queued')
  await hb.unregister('t1')
})

test('网络故障不取消、不触发 onBeat；恢复后继续', async () => {
  const beats = []
  const c = stub((n) => (n <= 3 ? netErr() : ok({ status: 'running', lease_until: 1, lease_remaining_ms: 60000 })))
  const hb = mk(c, { logStreamStartBeat: 1 })
  await hb.register('t1', async (id) => { beats.push(c.calls) })
  await sleep(0.05)
  assert.equal(hb.getCancelEvent('t1').isSet(), false)
  assert.ok(beats.length >= 1)
  assert.ok(beats.every((n) => n >= 4), `前 3 跳网络故障不应推日志：${beats}`)
  await hb.unregister('t1')
})

test('onBeat：自第 N 跳起、单任务串行并合并积压、异常被吞', async () => {
  let running = 0
  let maxRunning = 0
  let calls = 0
  const c = stub(() => ok({ status: 'running', lease_until: 1, lease_remaining_ms: 60000 }))
  const hb = mk(c, { logStreamStartBeat: 3 })
  await hb.register('t1', async () => {
    calls += 1
    running += 1
    maxRunning = Math.max(maxRunning, running)
    if (calls === 1) { await sleep(0.06); running -= 1; throw new Error('boom') } // 慢 + 抛错
    await sleep(0.005)
    running -= 1
  })
  await sleep(0.03)
  assert.ok(c.calls >= 3)
  await sleep(0.2)
  assert.equal(maxRunning, 1, '不得并发起第二个上传')
  assert.ok(calls >= 3, '积压合并后仍应继续补发，且异常不影响心跳')
  assert.ok(calls < c.calls, '在途期间的多次节拍应合并，而不是一跳一发')
  assert.equal(hb.getCancelEvent('t1').isSet(), false)
  await hb.unregister('t1')
})

test('自熔：ack 预置的剩余租约耗尽且续租一直失败 → lease_expired', async () => {
  const hb = mk(stub(() => netErr()))
  await hb.register('t1')
  hb.seedLease('t1', 300) // 300ms - 余量 100ms = 200ms 后熔断
  const ev = hb.getCancelEvent('t1')
  await sleep(0.1)
  assert.equal(ev.isSet(), false, '余量内不应熔断')
  assert.equal(await ev.waitFor(2), true)
  assert.equal(hb.getServerStatus('t1'), 'lease_expired')
  await hb.unregister('t1')
})

test('seedLease 可先于 register；非正数忽略', async () => {
  const hb = mk(stub(() => netErr()))
  hb.seedLease('t1', 250)
  hb.seedLease('t2', 0)
  hb.seedLease('t3', null)
  await hb.register('t1')
  await hb.register('t2')
  assert.equal(await hb.getCancelEvent('t1').waitFor(2), true)
  await sleep(0.05)
  assert.equal(hb.getCancelEvent('t2').isSet(), false, '无基准的任务不应自熔')
  await hb.shutdownAll()
})

test('自熔优先使用 lease_remaining_ms，不受 lease_until 绝对时刻（时钟偏移）影响', async () => {
  // lease_until 是远古时刻（若按墙钟推算早已过期），remaining 却充足 → 不熔断
  let n = 0
  const hb = mk(stub(() => { n += 1; return n === 1 ? ok({ status: 'running', lease_until: 1, lease_remaining_ms: 60000 }) : netErr() }))
  await hb.register('t1')
  await sleep(0.15)
  assert.equal(hb.getCancelEvent('t1').isSet(), false)
  await hb.unregister('t1')
})

test('旧服务端只给 lease_until：回退按墙钟推算，到期自熔', async () => {
  let n = 0
  const hb = mk(stub(() => { n += 1; return n === 1 ? ok({ status: 'running', lease_until: Date.now() + 300 }) : netErr() }))
  await hb.register('t1')
  assert.equal(await hb.getCancelEvent('t1').waitFor(2), true)
  assert.equal(hb.getServerStatus('t1'), 'lease_expired')
  await hb.unregister('t1')
})

test('unregister：停止心跳；重复注册得到全新 Event 与状态', async () => {
  const c = stub(() => ok({ status: 'running', lease_until: 1, lease_remaining_ms: 60000 }))
  const hb = mk(c)
  await hb.register('t1')
  const ev1 = hb.getCancelEvent('t1')
  await sleep(0.03)
  await hb.unregister('t1')
  const after = c.calls
  await sleep(0.05)
  assert.ok(c.calls <= after + 1, '注销后不应再有心跳（至多一个在途请求）')
  assert.equal(hb.getCancelEvent('t1'), null)
  await hb.register('t1')
  assert.notEqual(hb.getCancelEvent('t1'), ev1)
  await hb.shutdownAll()
  assert.equal(hb.getCancelEvent('t1'), null)
})

test('register 的第二个参数也可传 { onBeat } 选项对象', async () => {
  let n = 0
  const hb = mk(stub(() => ok({ status: 'running', lease_until: 1, lease_remaining_ms: 60000 })), { logStreamStartBeat: 1 })
  await hb.register('t1', { onBeat: async () => { n += 1 } })
  await sleep(0.05)
  assert.ok(n >= 1)
  await hb.shutdownAll()
})
