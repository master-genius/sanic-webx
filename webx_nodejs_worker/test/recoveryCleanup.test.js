'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const { settings } = require('../src/config')
const fencing = require('../src/fencing')
const { TaskKindPlugin } = require('../src/plugin')
const { WorkerRecovery } = require('../src/recovery')
const { TaskDirCleaner, parseDuration, buildPolicyFromArgs, DEFAULT_POLICY, main: cleanupMain } = require('../src/cleanup')

settings.WORKER_ID = 'w1'

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-rc-'))
const writeJson = (p, o) => { fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(p, JSON.stringify(o)) }
const readJson = (p) => JSON.parse(fs.readFileSync(p, 'utf8'))
const taskDir = (root, id) => { const d = path.join(root, id); fs.mkdirSync(d, { recursive: true }); return d }
const dayAgo = (n) => new Date(Date.now() - n * 86400000).toISOString()

const res = (data) => ({ data, isNotFound: false, isError: false })
const NF = { data: null, isNotFound: true, isError: false }
const ERR = { data: null, isNotFound: false, isError: true }

/** 假 callback：记录调用以及调用瞬间 fencing 注册表里的令牌 */
function fakeCallback({ complete = true, status = ERR } = {}) {
  const c = {
    calls: [],
    async completeTask(id) { c.calls.push(['complete', id, fencing.leaseSeqOf(id)]); return typeof complete === 'function' ? complete(id) : complete },
    async getTaskStatus(id) { c.calls.push(['status', id]); return typeof status === 'function' ? status(id) : status },
    async releaseLease(id) { c.calls.push(['release', id, fencing.leaseSeqOf(id)]); return true },
  }
  return c
}
const callsOf = (c, kind) => c.calls.filter((x) => x[0] === kind)

// ======================= recovery =======================

test('recovery：confirmed=true 跳过；failed 未确认不重试；cancelled 未确认标记已确认', async () => {
  const root = tmp()
  writeJson(path.join(taskDir(root, 'c1'), 'ok.json'), { status: 'completed', server_confirmed: true })
  writeJson(path.join(taskDir(root, 'f1'), 'ok.json'), { status: 'failed', server_confirmed: false })
  writeJson(path.join(taskDir(root, 'x1'), 'ok.json'), { status: 'cancelled', server_confirmed: false })
  const cb = fakeCallback()
  await new WorkerRecovery(cb, root).recover()
  assert.deepEqual(cb.calls, [], '这三类都不应访问服务端')
  assert.equal(readJson(path.join(root, 'c1', 'ok.json')).server_confirmed, true)
  assert.equal(readJson(path.join(root, 'f1', 'ok.json')).server_confirmed, false)
  assert.equal(readJson(path.join(root, 'x1', 'ok.json')).server_confirmed, true)
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：completed 未确认 → 带 ok.json 里的令牌重试 complete，成功后标记已确认并注销令牌', async () => {
  const root = tmp()
  const okPath = path.join(taskDir(root, 't1'), 'ok.json')
  writeJson(okPath, { status: 'completed', server_confirmed: false, worker_id: 'w1', lease_seq: 3 })
  const cb = fakeCallback({ complete: true })
  await new WorkerRecovery(cb, root).recover()
  assert.deepEqual(cb.calls, [['complete', 't1', 3]])
  assert.equal(fencing.leaseSeqOf('t1'), null, '用完必须注销')
  const ok = readJson(okPath)
  assert.equal(ok.server_confirmed, true)
  assert.ok(ok.server_confirmed_at)
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：别的 worker 写的令牌不登记；complete 失败但服务端已 completed 视为已确认', async () => {
  const root = tmp()
  const okPath = path.join(taskDir(root, 't1'), 'ok.json')
  writeJson(okPath, { status: 'completed', server_confirmed: false, worker_id: 'other', lease_seq: 9 })
  const cb = fakeCallback({ complete: false, status: res({ status: 'completed' }) })
  await new WorkerRecovery(cb, root).recover()
  assert.deepEqual(callsOf(cb, 'complete'), [['complete', 't1', null]])
  assert.equal(readJson(okPath).server_confirmed, true)
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：complete 失败后服务端 404 → not_found；网络故障 → 保持未确认', async () => {
  const root = tmp()
  const p404 = path.join(taskDir(root, 'a404'), 'ok.json')
  const pErr = path.join(taskDir(root, 'berr'), 'ok.json')
  writeJson(p404, { status: 'completed', server_confirmed: false })
  writeJson(pErr, { status: 'completed', server_confirmed: false })
  const cb = fakeCallback({ complete: false, status: (id) => (id === 'a404' ? NF : ERR) })
  await new WorkerRecovery(cb, root).recover()
  const a = readJson(p404)
  assert.equal(a.status, 'not_found')
  assert.equal(a.server_confirmed, false)
  assert.ok(a.not_found_at)
  assert.equal(readJson(pErr).status, 'completed')
  assert.equal(readJson(pErr).server_confirmed, false)
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：无 ok.json → 服务端 404 写 not_found 标记并保留目录（保留 meta.json 的步骤信息）', async () => {
  const root = tmp()
  const d = taskDir(root, 't1')
  writeJson(path.join(d, 'meta.json'), { steps: ['a', 'b'], sub_step: 'b', end_step: 'b' })
  await new WorkerRecovery(fakeCallback({ status: NF }), root).recover()
  const ok = readJson(path.join(d, 'ok.json'))
  assert.equal(ok.status, 'not_found')
  assert.deepEqual(ok.steps, ['a', 'b'])
  assert.equal(ok.sub_step, 'b')
  assert.ok(fs.existsSync(path.join(d, 'meta.json')))
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：无 ok.json → 网络故障 / 服务端 completed 保留目录且不写标记', async () => {
  const root = tmp()
  taskDir(root, 'net')
  taskDir(root, 'done')
  const cb = fakeCallback({ status: (id) => (id === 'net' ? ERR : res({ status: 'completed' })) })
  await new WorkerRecovery(cb, root).recover()
  for (const id of ['net', 'done']) {
    assert.ok(fs.existsSync(path.join(root, id)))
    assert.ok(!fs.existsSync(path.join(root, id, 'ok.json')))
  }
  assert.deepEqual(callsOf(cb, 'release'), [])
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：无 ok.json 且 queued/running 租户是本机 → 带令牌归还租约，再整目录清理', async () => {
  const root = tmp()
  fs.writeFileSync(path.join(taskDir(root, 'q1'), 'junk'), 'x')
  fs.writeFileSync(path.join(taskDir(root, 'r1'), 'junk'), 'x')
  fs.writeFileSync(path.join(taskDir(root, 'q2'), 'junk'), 'x')
  const cb = fakeCallback({
    status: (id) => ({
      q1: res({ status: 'queued', worker_id: 'w1', lease_seq: 5 }),
      r1: res({ status: 'running', worker_id: 'w1', lease_seq: 7 }),
      q2: res({ status: 'queued', worker_id: 'someone-else', lease_seq: 1 }), // 租户不是本机：不归还
    }[id]),
  })
  await new WorkerRecovery(cb, root).recover()
  assert.deepEqual(callsOf(cb, 'release'), [['release', 'q1', 5], ['release', 'r1', 7]])
  for (const id of ['q1', 'r1', 'q2']) assert.ok(!fs.existsSync(path.join(root, id)), `${id} 应被清理`)
  assert.deepEqual(fencing.snapshot(), [], '令牌用完必须注销')
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：插件 softCleanPolicy 返回名字集合 → 只保留这些；task_type 取服务端优先、meta.json 兜底', async () => {
  const root = tmp()
  for (const id of ['s1', 's2']) {
    const d = taskDir(root, id)
    fs.mkdirSync(path.join(d, 'in'))
    fs.writeFileSync(path.join(d, 'keep.bin'), 'k')
    fs.writeFileSync(path.join(d, 'params.json'), '{}')
  }
  writeJson(path.join(root, 's2', 'meta.json'), { task_type: 'heavy' })
  const seen = []
  const plugin = new TaskKindPlugin()
  plugin.softCleanPolicy = (t) => { seen.push(t); return t === 'heavy' || t === 'srv' ? new Set(['keep.bin', 'meta.json']) : null }
  const cb = fakeCallback({ status: (id) => res(id === 's1' ? { status: 'failed', task_type: 'srv' } : { status: 'running' }) })
  await new WorkerRecovery(cb, root, plugin).recover()
  assert.deepEqual(seen.sort(), ['heavy', 'srv'])
  assert.deepEqual(fs.readdirSync(path.join(root, 's1')), ['keep.bin'])
  assert.deepEqual(fs.readdirSync(path.join(root, 's2')).sort(), ['keep.bin', 'meta.json'])
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：点号开头的基础设施目录不当任务；not_found 标记超 7 天清理、未超期保留；坏 ok.json 不拖垮整轮', async () => {
  const root = tmp()
  taskDir(root, '.uploads')
  writeJson(path.join(taskDir(root, 'old'), 'ok.json'), { status: 'not_found', not_found_at: dayAgo(8) })
  writeJson(path.join(taskDir(root, 'new'), 'ok.json'), { status: 'not_found', not_found_at: dayAgo(1) })
  fs.writeFileSync(path.join(taskDir(root, 'bad'), 'ok.json'), '{broken')
  writeJson(path.join(taskDir(root, 'z-ok'), 'ok.json'), { status: 'cancelled', server_confirmed: false }) // 排在坏目录之后仍应被处理
  const cb = fakeCallback()
  await new WorkerRecovery(cb, root).recover()
  assert.deepEqual(cb.calls, [])
  assert.ok(fs.existsSync(path.join(root, '.uploads')))
  assert.ok(!fs.existsSync(path.join(root, 'old')))
  assert.ok(fs.existsSync(path.join(root, 'new')))
  assert.equal(readJson(path.join(root, 'z-ok', 'ok.json')).server_confirmed, true)
  fs.rmSync(root, { recursive: true, force: true })
})

test('recovery：工作目录不存在 / 为空时直接返回', async () => {
  const cb = fakeCallback()
  await new WorkerRecovery(cb, path.join(os.tmpdir(), 'wnw-no-such-dir-xyz')).recover()
  const root = tmp()
  await new WorkerRecovery(cb, root).recover()
  assert.deepEqual(cb.calls, [])
  fs.rmSync(root, { recursive: true, force: true })
})

// ======================= cleanup =======================

test('parseDuration / buildPolicyFromArgs', () => {
  assert.equal(parseDuration('30d'), 30 * 86400)
  assert.equal(parseDuration('3h'), 3 * 3600)
  assert.equal(parseDuration('120m'), 7200)
  assert.equal(parseDuration('300s'), 300)
  assert.equal(parseDuration('30'), 30 * 86400) // 无后缀数字串按天
  assert.equal(parseDuration(90), 90) // 数字按秒
  assert.throws(() => parseDuration(''), /无效的时长/)
  assert.throws(() => parseDuration('abc'), /无效的时长/)
  assert.throws(() => parseDuration('xd'), /无效的时长/)
  assert.deepEqual(buildPolicyFromArgs([], []), { ...DEFAULT_POLICY })
  assert.deepEqual(buildPolicyFromArgs(['failed'], []), { failed: 2 * 86400 })
  assert.deepEqual(buildPolicyFromArgs(['failed', 'cancelled'], ['1d']), { failed: 86400, cancelled: 86400 })
  assert.deepEqual(buildPolicyFromArgs(['failed', 'completed'], ['1d', '2h']), { failed: 86400, completed: 7200 })
  assert.throws(() => buildPolicyFromArgs([], ['1d']), /配对/)
  assert.throws(() => buildPolicyFromArgs(['cancelled'], []), /必须显式/)
  assert.throws(() => buildPolicyFromArgs(['a', 'b', 'c'], ['1d', '2d']), /不匹配/)
})

test('cleanup：只清 confirmed 且超龄的 failed / completed', () => {
  const root = tmp()
  const ok = (id, o) => writeJson(path.join(taskDir(root, id), 'ok.json'), o)
  ok('c-old', { status: 'completed', server_confirmed: true, completed_at: dayAgo(8) }) // 清
  ok('c-fresh', { status: 'completed', server_confirmed: true, completed_at: dayAgo(1) })
  ok('f-old', { status: 'failed', server_confirmed: true, completed_at: dayAgo(3) }) // 清（failed 2 天）
  ok('f-unconf', { status: 'failed', server_confirmed: false, completed_at: dayAgo(30) }) // 未确认：留作诊断
  ok('x-old', { status: 'cancelled', server_confirmed: true, completed_at: dayAgo(30) }) // 不在默认策略
  ok('nf', { status: 'not_found', server_confirmed: false, completed_at: dayAgo(30) })
  ok('naive', { status: 'completed', server_confirmed: true, completed_at: new Date(Date.now() - 9 * 86400000).toISOString().replace('Z', '') }) // 无时区按 UTC → 清
  fs.writeFileSync(path.join(taskDir(root, 'bad'), 'ok.json'), 'nope')
  taskDir(root, 'running') // 无 ok.json：执行中
  writeJson(path.join(root, '.uploads', 'pending', 'unrelated__u0.json'), {})
  fs.writeFileSync(path.join(root, 'stray-file'), 'x')

  const r = new TaskDirCleaner(root).scanAndPurge()
  assert.deepEqual(r.purgedTaskIds.sort(), ['c-old', 'f-old', 'naive'])
  assert.equal(r.purged, 3)
  assert.equal(r.scanned, 9, '.uploads 与普通文件不算任务目录')
  assert.deepEqual(r.skipped, {
    fresh_completed: 1, unconfirmed_failed: 1, out_of_scope_cancelled: 1, out_of_scope_not_found: 1, bad_ok_json: 1, no_ok_json: 1,
  })
  assert.deepEqual(fs.readdirSync(root).sort(), ['.uploads', 'bad', 'c-fresh', 'f-unconf', 'nf', 'running', 'stray-file', 'x-old'])
  assert.deepEqual(r.toDict().purged_task_ids.sort(), ['c-old', 'f-old', 'naive'])
  fs.rmSync(root, { recursive: true, force: true })
})

test('cleanup：仍在上传队列（pending / inflight）里的任务跳过；dry-run 不删除；自定义策略', () => {
  const root = tmp()
  const old = { status: 'completed', server_confirmed: true, completed_at: dayAgo(10) }
  for (const id of ['p1', 'i1', 'free']) writeJson(path.join(taskDir(root, id), 'ok.json'), old)
  writeJson(path.join(root, '.uploads', 'pending', 'p1__abc.json'), {})
  writeJson(path.join(root, '.uploads', 'inflight', 'i1__def.json'), {})

  const dry = new TaskDirCleaner(root, true).scanAndPurge()
  assert.deepEqual(dry.purgedTaskIds, ['free'])
  assert.equal(dry.skipped.pending_upload_completed, 2)
  assert.ok(fs.existsSync(path.join(root, 'free')), 'dry-run 不得删除')

  const real = new TaskDirCleaner(root).scanAndPurge({ completed: '1d' })
  assert.deepEqual(real.purgedTaskIds, ['free'])
  assert.ok(!fs.existsSync(path.join(root, 'free')))
  assert.ok(fs.existsSync(path.join(root, 'p1')) && fs.existsSync(path.join(root, 'i1')))

  // 工作目录不存在：不抛，返回空结果
  assert.equal(new TaskDirCleaner(path.join(root, 'nope')).scanAndPurge().scanned, 0)
  fs.rmSync(root, { recursive: true, force: true })
})

test('cleanup CLI：参数错误返回非 0；--dry-run 正常返回 0', () => {
  const root = tmp()
  writeJson(path.join(taskDir(root, 'c-old'), 'ok.json'), { status: 'completed', server_confirmed: true, completed_at: dayAgo(30) })
  const write = process.stdout.write
  let captured = ''
  process.stdout.write = (s) => { captured += s; return true }
  let code
  try { code = cleanupMain(['--work-dir', root, '--dry-run', '-q']) } finally { process.stdout.write = write }
  assert.equal(code, 0)
  assert.deepEqual(JSON.parse(captured).purged_task_ids, ['c-old'])
  assert.ok(fs.existsSync(path.join(root, 'c-old')))
  const err = process.stderr.write
  process.stderr.write = () => true
  try { assert.equal(cleanupMain(['--work-dir', root, '--timeout', '1d']), 1) } finally { process.stderr.write = err }
  fs.rmSync(root, { recursive: true, force: true })
})
