'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const { settings } = require('../src/config')
const { Event, sleep } = require('../src/aio')
const { FakeTaskServer } = require('../src/fakeServer')
const q = require('../src/sdk/uploadQueue')
const up = require('../src/uploader')
const { UploaderSupervisor, childEnv } = require('../src/uploaderSupervisor')

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-up-'))

async function setup({ chunk = 1024 } = {}) {
  const s = await new FakeTaskServer({ holdSeconds: 0.1 }).start()
  const workDir = tmp()
  Object.assign(settings, {
    WORK_DIR: workDir, WEB_BASE_URL: s.url, STORAGE_URL: s.url, STORAGE_INTERNAL_URL: '',
    INTERNAL_TOKEN: 'test-token', STORAGE_INTERNAL_TOKEN: '', UPLOADER_ENABLED: true,
    UPLOADER_SCAN_INTERVAL: 0.5, UPLOADER_CONCURRENCY: 3,
  })
  up.tuning.CHUNK_SIZE = chunk
  up.tuning.RECHECK_EVERY_CHUNKS = 8
  q.ensureDirs(workDir)
  const u = new up.Uploader(workDir)
  return { s, workDir, u }
}

/** 造一个产物文件并投递；返回 {tid, file, name} */
function produce(s, workDir, { size, key = ['step1', 'out_url'], name = 'out.bin', tid = null, fill = 7 } = {}) {
  const taskId = tid || s.addTask('demo')
  if (s.tasks.get(taskId).status === 'pending') s.setStatus(taskId, 'running')
  const file = path.join(workDir, `${taskId}-${name}`)
  fs.writeFileSync(file, Buffer.alloc(size, fill))
  assert.equal(q.enqueue(workDir, taskId, file, name, key), true)
  return { tid: taskId, file, name }
}

/** 把 pending 里当前的条目逐个处理一遍 */
async function pass(u, workDir) {
  for (const e of q.iterPending(workDir)) await u.process(e)
}

const pendingNames = (workDir) => q.iterPending(workDir).map((e) => e.filename)
const inflightNames = (workDir) => fs.readdirSync(q.inflightDir(workDir))

test('整传：≤ 分片大小走 multipart，传完交付嵌套 result_path（不带 fencing），条目出队', async () => {
  const { s, workDir, u } = await setup()
  try {
    const { tid, name } = produce(s, workDir, { size: 500 })
    await pass(u, workDir)
    assert.deepEqual(s.stored(tid, name), Buffer.alloc(500, 7))
    assert.equal(s.callsTo('POST', '/results$').length, 1)
    assert.equal(s.callsTo('POST', '/results/uploads$').length, 0)
    // 交付：嵌套 key，URL 用对外前缀
    assert.deepEqual(s.task(tid).result_path, { step1: { out_url: `${s.url}/public/tasks/${tid}/results/${name}` } })
    const dl = s.callsTo('POST', '/deliveries$')[0]
    assert.equal(dl.headers['x-internal-token'], 'test-token')
    assert.equal(dl.headers['x-worker-id'], undefined, '交付刻意不带 fencing')
    assert.deepEqual(pendingNames(workDir), [])
    assert.deepEqual(inflightNames(workDir), [])
  } finally { await s.stop() }
})

test('分片：> 分片大小走会话，逐片带 Upload-Offset，内容完整', async () => {
  const { s, workDir, u } = await setup()
  try {
    const { tid, name, file } = produce(s, workDir, { size: 3000 })
    const body = Buffer.from(Array.from({ length: 3000 }, (_, i) => i % 251))
    fs.writeFileSync(file, body)
    await pass(u, workDir)
    assert.deepEqual(s.stored(tid, name), body)
    const patches = s.callsTo('PATCH', '/internal/uploads/')
    assert.deepEqual(patches.map((c) => c.headers['upload-offset']), ['0', '1024', '2048'])
    assert.equal(s.callsTo('POST', '/results$').length, 0)
    assert.equal(s.callsTo('POST', '/complete$').length, 1)
    assert.deepEqual(s.task(tid).result_path.step1.out_url.endsWith(name), true)
    assert.deepEqual(pendingNames(workDir), [])
  } finally { await s.stop() }
})

test('分片续传接力：条目里的 sessionId 有效则从服务端 offset 继续，不从头', async () => {
  const { s, workDir, u } = await setup()
  try {
    const { tid, name, file } = produce(s, workDir, { size: 3000 })
    const body = fs.readFileSync(file)
    // 先手工建会话并传好第一片，模拟「上次崩在半路」
    const c = await (await fetch(`${s.url}/internal/tasks/${tid}/results/uploads`, {
      method: 'POST', headers: { 'X-Internal-Token': 'test-token', 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: name, total: 3000 }),
    })).json()
    await fetch(`${s.url}/internal/uploads/${c.upload_id}`, {
      method: 'PATCH', headers: { 'X-Internal-Token': 'test-token', 'Upload-Offset': '0' }, body: body.subarray(0, 1024),
    })
    const [e] = q.iterPending(workDir)
    const claimed = q.claim(workDir, e)
    claimed.sessionId = c.upload_id
    q.update(workDir, claimed)
    const before = s.callsTo('PATCH', '/internal/uploads/').length
    assert.ok(await u.uploadResumable(claimed))
    const mine = s.callsTo('PATCH', '/internal/uploads/').slice(before)
    assert.deepEqual(mine.map((x) => x.headers['upload-offset']), ['1024', '2048'])
    assert.deepEqual(s.stored(tid, name), body)
    assert.equal(s.callsTo('POST', '/results/uploads$').length, 1, '不应再建新会话')
  } finally { await s.stop() }
})

test('409 以服务端响应头 Upload-Offset 重新对齐', async () => {
  const { s, workDir, u } = await setup()
  try {
    const { tid, name, file } = produce(s, workDir, { size: 3000 })
    const body = fs.readFileSync(file)
    // 第一次 PATCH 到达前，让服务端会话里「已经有」第一片（模拟上次发送其实成功了）
    let done = false
    s.hooks.push((rec) => {
      if (done || rec.method !== 'PATCH') return
      done = true
      const [[, sess]] = [...s.uploads]
      sess.data = body.subarray(0, 1024)
    })
    await pass(u, workDir)
    const offs = s.callsTo('PATCH', '/internal/uploads/').map((c) => c.headers['upload-offset'])
    assert.deepEqual(offs, ['0', '1024', '2048'], '0 被 409 后按服务端的 1024 继续')
    assert.deepEqual(s.stored(tid, name), body)
  } finally { await s.stop() }
})

test('K18：409 且缺 Upload-Offset 头时有计数上限，不会无限重发', async () => {
  const { s, workDir, u } = await setup()
  try {
    produce(s, workDir, { size: 3000 })
    // 反代吞头：409 无头无 offset；会话 GET 同样给不出 offset（头被吞）
    s.inject('PATCH', '/internal/uploads/', 409, { times: 1000, body: { error: 'offset mismatch' } })
    s.inject('GET', '/internal/uploads/', 200, { times: 1000, body: {} })
    await pass(u, workDir)
    const n = s.callsTo('PATCH', '/internal/uploads/').length
    assert.ok(n <= 1, `取不到 offset 应立即放弃本轮，实际重发 ${n} 次`)
    const [e] = q.iterPending(workDir)
    assert.equal(e.attempts, 1, '条目带 attempts 退回队列，由指数退避接管')
  } finally { await s.stop() }
})

test('K18：头缺失但 409 持续未能前进，超过上限后放弃本轮', async () => {
  const { s, workDir, u } = await setup()
  try {
    produce(s, workDir, { size: 3000 })
    // 头被吞，但会话 GET 每次都说 offset=0 → 重对齐到 0 → 再 409 …… 必须有上限
    s.inject('PATCH', '/internal/uploads/', 409, { times: 1000, body: { error: 'x' } })
    await pass(u, workDir)
    const n = s.callsTo('PATCH', '/internal/uploads/').length
    assert.equal(n, up.tuning.MAX_RESYNC_STRIKES + 1)
    assert.equal(q.iterPending(workDir)[0].attempts, 1)
  } finally { await s.stop() }
})

test('任务已取消 / 已失败：不传，丢弃条目；任务保留则正常传', async () => {
  const { s, workDir, u } = await setup()
  try {
    const a = produce(s, workDir, { size: 100, name: 'a.bin' })
    const b = produce(s, workDir, { size: 100, name: 'b.bin' })
    const c = produce(s, workDir, { size: 100, name: 'c.bin' })
    s.cancelTask(a.tid)
    s.setStatus(b.tid, 'failed')
    await pass(u, workDir)
    assert.deepEqual(pendingNames(workDir), [])
    assert.equal(s.stored(a.tid, a.name), undefined)
    assert.equal(s.stored(b.tid, b.name), undefined)
    assert.ok(s.stored(c.tid, c.name))
    assert.equal(s.callsTo('POST', '/results$').length, 1)
  } finally { await s.stop() }
})

test('权威检查：404 → 丢弃；401/403/5xx/网络问不到 → 保留并继续传（修好即自愈）', async () => {
  const { s, workDir, u } = await setup()
  try {
    const { tid } = produce(s, workDir, { size: 10 })
    const [e] = q.iterPending(workDir)
    assert.equal(await u.stillWanted(e), true)
    for (const code of [401, 403, 500, 502]) {
      s.inject('GET', `/internal/tasks/${tid}$`, code, { times: 1 })
      assert.equal(await u.stillWanted(e), true, `状态码 ${code} 应保留`)
    }
    s.inject('GET', `/internal/tasks/${tid}$`, 404, { times: 1 })
    assert.equal(await u.stillWanted(e), false)
    s.softDelete(tid) // 软删同样表现为 404
    assert.equal(await u.stillWanted(e), false)
    // 兼容包在 data 里的写法
    s.tasks.get(tid).is_delete = false
    s.wrapTaskInfoInData = true
    s.setStatus(tid, 'cancelled')
    assert.equal(await u.stillWanted(e), false)
  } finally { await s.stop() }
})

test('交付 404：带 error（任务没了）丢弃；无 error（路由缺失）保留，且下一轮只重做交付不重传', async () => {
  const { s, workDir, u } = await setup()
  try {
    const gone = produce(s, workDir, { size: 100, name: 'gone.bin' })
    const noRoute = produce(s, workDir, { size: 100, name: 'route.bin' })
    s.inject('POST', `/internal/tasks/${gone.tid}/deliveries$`, 404, { body: { error: 'task not found' } })
    s.inject('POST', `/internal/tasks/${noRoute.tid}/deliveries$`, 404, { body: { message: 'Not Found' } })
    await pass(u, workDir)

    assert.deepEqual(pendingNames(workDir), [`${noRoute.tid}__step1_out.json`])
    const [kept] = q.iterPending(workDir)
    assert.equal(kept.attempts, 1)
    assert.ok(kept.uploadedUrl.endsWith('route.bin'), '上传阶段的结果已落盘')
    assert.deepEqual(s.task(noRoute.tid).result_path, {})

    // 第二轮（越过退避窗口，直接处理）：只做交付，不再重传
    const uploads = s.callsTo('POST', '/results$').length
    await u.process(kept)
    assert.equal(s.callsTo('POST', '/results$').length, uploads)
    assert.deepEqual(pendingNames(workDir), [])
    assert.ok(s.task(noRoute.tid).result_path.step1.out_url.endsWith('route.bin'))
  } finally { await s.stop() }
})

test('上传被拒 → 退回队列 attempts+1，保留条目', async () => {
  const { s, workDir, u } = await setup()
  try {
    produce(s, workDir, { size: 100 })
    s.inject('POST', '/results$', 500, { times: 1 })
    await pass(u, workDir)
    const [e] = q.iterPending(workDir)
    assert.equal(e.attempts, 1)
    assert.equal(e.uploadedUrl, '')
    assert.deepEqual(inflightNames(workDir), [])
  } finally { await s.stop() }
})

test('本地文件已不在 → 丢弃', async () => {
  const { s, workDir, u } = await setup()
  try {
    const { file } = produce(s, workDir, { size: 10 })
    fs.rmSync(file)
    await pass(u, workDir)
    assert.deepEqual(pendingNames(workDir), [])
    assert.equal(s.callsTo('POST', '/results$').length, 0)
  } finally { await s.stop() }
})

test('K17：上传期间同名条目被 redo 覆盖，旧条目收尾不误删新条目', async () => {
  const { s, workDir, u } = await setup()
  try {
    const { tid, file, name } = produce(s, workDir, { size: 100, fill: 1 })
    let redone = false
    s.hooks.push((rec) => {
      if (redone || rec.method !== 'POST' || !/\/results$/.test(rec.path)) return
      redone = true
      fs.writeFileSync(file, Buffer.alloc(200, 2)) // redo：同一 (任务, key) 产出新内容并重新投递
      assert.equal(q.enqueue(workDir, tid, file, name, ['step1', 'out_url']), true)
    })
    await pass(u, workDir)
    assert.deepEqual(pendingNames(workDir), [`${tid}__step1_out.json`], '新一代条目必须还在')
    assert.deepEqual(inflightNames(workDir), [])
    // 下一轮把新一代传完
    await pass(u, workDir)
    assert.deepEqual(s.stored(tid, name), Buffer.alloc(200, 2))
    assert.deepEqual(pendingNames(workDir), [])
  } finally { await s.stop() }
})

test('退避：5s 起步指数翻倍封顶 300s；isDue 以上次失败时刻为基准', () => {
  assert.deepEqual([0, 1, 2, 3, 7, 20].map(up.retryDelay), [0, 5, 10, 20, 300, 300])
  const e = { attempts: 0, queuedAt: 1000 }
  assert.equal(up.isDue(e, 1000), true)
  e.attempts = 2
  assert.equal(up.isDue(e, 1009.9), false)
  assert.equal(up.isDue(e, 1010), true)
  assert.deepEqual(up.nest(['a', 'b', 'c'], 'u'), { a: { b: { c: 'u' } } })
})

test('run()：扫描目录自动上传并交付，stopping 置位后优雅退出', async () => {
  const { s, workDir } = await setup()
  try {
    const a = produce(s, workDir, { size: 100, name: 'a.bin' })
    const b = produce(s, workDir, { size: 2500, name: 'b.bin', key: ['step2', 'big_url'] })
    const stopping = new Event()
    const runP = up.run(workDir, { stopping })
    assert.ok(await s.waitFor(() => pendingNames(workDir).length === 0 && inflightNames(workDir).length === 0 &&
      s.task(b.tid).result_path.step2, 5))
    stopping.set()
    await runP
    assert.ok(s.stored(a.tid, a.name) && s.stored(b.tid, b.name))
    assert.equal(s.stored(b.tid, b.name).length, 2500)
  } finally { await s.stop() }
})

test('run()：没有存储地址拒绝启动', async () => {
  const { s, workDir } = await setup()
  try {
    Object.assign(settings, { STORAGE_URL: '', STORAGE_INTERNAL_URL: '' })
    await up.run(workDir, { stopping: new Event() }) // 直接返回，不进主循环
  } finally { await s.stop() }
})

// ─────────────────────────── supervisor ───────────────────────────

test('UploaderSupervisor：env 注入（含 LOG_DIR）、崩溃自动重启、stop 确认退出；无存储地址不拉起', async () => {
  const dir = tmp()
  settings.WORK_DIR = dir
  Object.assign(settings, {
    UPLOADER_ENABLED: true, STORAGE_URL: 'http://storage.example', STORAGE_INTERNAL_URL: '', LOG_DIR: path.join(dir, 'logs'),
    INTERNAL_TOKEN: 'tok-main', WEB_BASE_URL: 'http://web.example',
  })
  const env = childEnv()
  assert.equal(env.WEBX_WORKER_LOG_DIR, path.join(dir, 'logs'))
  assert.equal(env.WEBX_WORKER_INTERNAL_TOKEN, 'tok-main')
  assert.equal(env.WEBX_WORKER_WEB_BASE_URL, 'http://web.example')

  // 用一个 sleep 脚本代替真 uploader：把收到的环境变量和 pid 追加写进文件
  const out = path.join(dir, 'child.log')
  const script = `require('fs').appendFileSync(${JSON.stringify(out)}, JSON.stringify({pid: process.pid, log: process.env.WEBX_WORKER_LOG_DIR, tok: process.env.WEBX_WORKER_INTERNAL_TOKEN}) + '\\n');` +
    'process.on("SIGTERM", () => process.exit(0)); setInterval(() => {}, 1000)'
  const sup = new UploaderSupervisor({ name: 'test-uploader', command: process.execPath, args: ['-e', script], restartDelay: 100 })
  try {
    assert.equal(sup.start(), true)
    const lines = () => (fs.existsSync(out) ? fs.readFileSync(out, 'utf8').trim().split('\n').map((l) => JSON.parse(l)) : [])
    for (let i = 0; i < 100 && lines().length < 1; i++) await sleep(0.05)
    assert.equal(lines()[0].log, path.join(dir, 'logs'), 'K19：LOG_DIR 必须传到子进程')
    assert.equal(lines()[0].tok, 'tok-main')

    // 崩溃（SIGKILL）后被拉起新的进程
    const pid1 = sup.pid
    assert.ok(pid1)
    process.kill(pid1, 'SIGKILL')
    for (let i = 0; i < 100 && lines().length < 2; i++) await sleep(0.05)
    assert.equal(lines().length, 2)
    assert.notEqual(lines()[1].pid, pid1)

    // stop：确认退出，且之后不再被重启
    const pid2 = sup.pid
    await sup.stop(2)
    assert.equal(sup.pid, null)
    assert.throws(() => process.kill(pid2, 0), /ESRCH/)
    await sleep(0.3)
    assert.equal(lines().length, 2)
    await sup.stop(2) // 幂等
  } finally {
    await sup.stop(1)
  }

  // 没有存储地址：不拉起
  Object.assign(settings, { STORAGE_URL: '', STORAGE_INTERNAL_URL: '' })
  const sup2 = new UploaderSupervisor({ name: 'test-uploader-2', command: process.execPath, args: ['-e', ''] })
  assert.equal(sup2.start(), false)
  assert.equal(sup2.pid, null)
})
