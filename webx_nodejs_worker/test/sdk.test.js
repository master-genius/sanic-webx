'use strict'

const test = require('node:test')
const { afterEach } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const http = require('node:http')
const net = require('node:net')
const os = require('node:os')
const path = require('node:path')

const sdk = require('../src/sdk')
const { FakeTaskServer } = require('../src/fakeServer')

const { resultClient, ipcClient, uploadRoute, uploadQueue: q, logsetup } = sdk
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-sdk-'))
const readJsonl = (f) => fs.readFileSync(f, 'utf8').trim().split('\n').map((l) => JSON.parse(l))

// 每个用例前后保存 / 还原 WEBX_WORKER_* 环境变量，并把退避基数压到 10ms
const savedEnv = { ...process.env }
const tuning = { ...resultClient._tuning }
function resetEnv() {
  for (const k of Object.keys(process.env)) if (k.startsWith('WEBX_WORKER_')) delete process.env[k]
  for (const [k, v] of Object.entries(savedEnv)) if (k.startsWith('WEBX_WORKER_')) process.env[k] = v
}
afterEach(() => {
  resetEnv()
  Object.assign(resultClient._tuning, tuning)
  ipcClient._resetForTest()
  uploadRoute._resetForTest()
})

/** 起假服务端 + 一个持有租约的 running 任务，并注入 SDK 所需环境变量 */
async function withTask(fn, { env = {}, serverOpts = {} } = {}) {
  Object.assign(resultClient._tuning, { reportBackoffBase: 0.01, uploadBackoffBase: 0.01, streamBackoffBase: 0.01 })
  const s = await new FakeTaskServer({ holdSeconds: 0.1, ...serverOpts }).start()
  const workDir = tmp()
  try {
    const tid = s.addTask('demo')
    Object.assign(s.tasks.get(tid), { status: 'running', worker_id: 'w1', lease_seq: 1 })
    Object.assign(process.env, {
      WEBX_WORKER_ID: 'w1', WEBX_WORKER_LEASE_SEQ: '1', WEBX_WORKER_TASK_ID: tid,
      WEBX_WORKER_INTERNAL_TOKEN: 'test-token', WEBX_WORKER_WEB_BASE_URL: s.url,
      WEBX_WORKER_STORAGE_URL: 'https://pub.example.com', WEBX_WORKER_STORAGE_INTERNAL_URL: s.url,
      WEBX_WORKER_WORK_DIR: workDir, WEBX_WORKER_IPC_SOCK: path.join(workDir, 'nope.sock'),
      ...env,
    })
    await fn({ s, tid, workDir })
  } finally {
    ipcClient._resetForTest()
    await s.stop()
  }
}

test('fnmatchcase：* ? [seq] [!seq]，区分大小写，特殊字符按字面', () => {
  const m = uploadRoute.fnmatchcase
  assert.ok(m('a.tar.gz', '*.tar.gz'))
  assert.ok(m('a.tar.bz', '*.tar.[gb]z'))
  assert.ok(!m('a.tar.xz', '*.tar.[gb]z'))
  assert.ok(m('x1.log', 'x?.log') && !m('x12.log', 'x?.log'))
  assert.ok(m('a.out', '[!b]*') && !m('b.out', '[!b]*'))
  assert.ok(m('a]b', 'a[]]b'))          // ] 紧跟 [ 是字面量
  assert.ok(m('a[b', 'a[b'))            // 无闭合的 [ 是字面量
  assert.ok(!m('A.GZ', '*.gz'))         // 区分大小写
  assert.ok(m('a+b(1).txt', 'a+b(1).*') && !m('aab(1).txt', 'a+b(1).*'))
  assert.ok(!m('x', '[z-a]'))           // 非法范围：不匹配而不是抛出
})

test('updateStepStatus：fencing 头、请求体与成功响应', async () => {
  await withTask(async ({ s, tid }) => {
    const r = await sdk.updateStepStatus(tid, { subStep: 's1', subStatus: 'running', steps: ['a'], stepMessage: 'hi' })
    assert.equal(r.ok, true)
    const call = s.callsTo('PATCH', '/steps$')[0]
    assert.equal(call.headers['x-worker-id'], 'w1')
    assert.equal(call.headers['x-lease-seq'], '1')
    assert.equal(call.headers['x-internal-token'], 'test-token')
    assert.deepEqual(call.json, { steps: ['a'], sub_step: 's1', sub_status: 'running', step_message: 'hi' })
    assert.equal(s.task(tid).sub_status, 'running')
  })
})

test('403 按 reason 处理：不重试、不入队；already_final 是幂等成功', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    const pending = path.join(workDir, tid, 'pending_reports.jsonl')
    // 令牌 lease_seq 不对 → not_lessee：恰好 1 次请求，抛出，不降级写文件
    process.env.WEBX_WORKER_LEASE_SEQ = '9'
    await assert.rejects(sdk.updateStepStatus(tid, { subStatus: 'running' }),
      (e) => e.name === 'HttpError' && e.status === 403 && e.reason === 'not_lessee')
    assert.equal(s.callsTo('PATCH', '/steps$').length, 1)
    assert.ok(!fs.existsSync(pending))
    // 带了 worker_id 但 lease_seq 畸形 → bad_lease_seq，同样响亮抛出
    process.env.WEBX_WORKER_LEASE_SEQ = 'abc'
    await assert.rejects(sdk.updateTaskFields(tid, { resultPath: '{}' }), (e) => e.reason === 'bad_lease_seq')
    assert.ok(!fs.existsSync(pending))
    // 完全没有令牌（手动调试）→ not_lessee
    delete process.env.WEBX_WORKER_LEASE_SEQ
    await assert.rejects(sdk.updateStepStatus(tid, { subStatus: 'x' }), (e) => e.reason === 'not_lessee')
    // 同一租户但任务已终态 → already_final：幂等成功
    process.env.WEBX_WORKER_LEASE_SEQ = '1'
    s.setStatus(tid, 'completed')
    const r = await sdk.updateStepStatus(tid, { subStatus: 'success' })
    assert.equal(r.message, 'Already final')
    assert.ok(!fs.existsSync(pending))
  })
})

test('业务拒绝 404 直接抛、不入队；任务不存在 → gone', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    s.inject('PATCH', '/steps$', 400)
    await assert.rejects(sdk.updateStepStatus(tid, { subStatus: 'x' }), (e) => e.status === 400)
    await assert.rejects(sdk.updateStepStatus('nosuchtask', { subStatus: 'x' }), (e) => e.reason === 'gone')
    assert.ok(!fs.existsSync(path.join(workDir, tid, 'pending_reports.jsonl')))
  })
})

test('5xx 试满 5 次后转 IPC；IPC 不通降级写文件，令牌随条目快照', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    s.inject('PATCH', '/steps$', 500, { times: 5 })
    s.inject('PATCH', '/fields$', 503, { times: 5 })
    ipcClient.setTaskContext(tid, 'running', 'running', 's1', 'end', ['a'])
    const r = await sdk.updateStepStatus(tid, { subStatus: 'success', subStep: 's1' })
    assert.equal(r.message, 'Queued for async retry')
    assert.equal(s.callsTo('PATCH', '/steps$').length, 5)
    await sdk.updateTaskFields(tid, { resultPath: '{"a":1}', mode: 'merge' })
    assert.equal(s.callsTo('PATCH', '/fields$').length, 5)

    const rows = readJsonl(path.join(workDir, tid, 'pending_reports.jsonl'))
    assert.equal(rows.length, 2)
    assert.equal(rows[0].action, 'update_step_status')
    assert.deepEqual(rows[0].payload, { sub_status: 'success', sub_step: 's1' })
    assert.deepEqual(rows[0].fencing, { worker_id: 'w1', lease_seq: 1 })
    assert.equal(typeof rows[0].fencing.lease_seq, 'number')
    assert.deepEqual(rows[0].context, { status: 'running', sub_status: 'running', sub_step: 's1', end_step: 'end', steps_done: ['a'] })
    assert.equal(typeof rows[0].ts, 'number')
    assert.equal(rows[1].action, 'update_task_fields')
    assert.deepEqual(rows[1].payload, { result_path: '{"a":1}', _mode: 'merge' })
  })
})

test('sendReport：IPC 通时走 AF_UNIX 一行一 JSON，且不写降级文件；畸形令牌 = 无令牌', async () => {
  await withTask(async ({ tid, workDir }) => {
    const lines = []
    const sockPath = path.join(workDir, 'ipc.sock')
    const srv = net.createServer((c) => {
      let buf = ''
      c.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { lines.push(JSON.parse(buf.slice(0, i))); buf = buf.slice(i + 1) } })
    })
    await new Promise((r) => srv.listen(sockPath, r))
    try {
      process.env.WEBX_WORKER_IPC_SOCK = sockPath
      await ipcClient.sendReport('update_step_status', { sub_status: 'running' }, { taskId: tid })
      process.env.WEBX_WORKER_LEASE_SEQ = ''
      await ipcClient.sendReport('update_task_fields', { log_path: 'x' }, { taskId: tid })
      for (let i = 0; i < 50 && lines.length < 2; i++) await new Promise((r) => setTimeout(r, 10))
      assert.equal(lines.length, 2)
      assert.deepEqual(lines[0].fencing, { worker_id: 'w1', lease_seq: 1 })
      assert.ok(!('fencing' in lines[1]))
      assert.ok(!fs.existsSync(path.join(workDir, tid)))
      // 缺 task_id：不抛，事件丢弃
      await ipcClient.sendReport('update_step_status', {})
    } finally {
      ipcClient._resetForTest()
      await new Promise((r) => srv.close(r))
    }
  })
})

test('reportStepMessage：2 次即弃、不入队、永不抛；空串 subStep 合法，null 早退', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    await sdk.reportStepMessage(tid, '', 'Preparing')
    assert.deepEqual(s.callsTo('PATCH', '/steps$')[0].json, { sub_step: '', step_message: 'Preparing' })
    await sdk.reportStepMessage(tid, null, 'x')
    await sdk.reportStepMessage('', 's', 'x')
    assert.equal(s.callsTo('PATCH', '/steps$').length, 1)

    s.inject('PATCH', '/steps$', 500, { times: 9 })
    await sdk.reportStepMessage(tid, 's', 'will drop')
    assert.equal(s.callsTo('PATCH', '/steps$').length, 3) // 1 + 2 次尝试
    assert.ok(!fs.existsSync(path.join(workDir, tid, 'pending_reports.jsonl')))
    delete process.env.WEBX_WORKER_WEB_BASE_URL // 环境缺失也只是吞掉
    await sdk.reportStepMessage(tid, 's', 'x')
  })
})

test('uploadResult：整传、远端名不同于本地名、download_url 改用对外地址；5xx 重试；4xx 立即抛', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    const f = path.join(workDir, 'local-name.bin')
    fs.writeFileSync(f, 'payload')
    s.inject('POST', '/results$', 502, { times: 2 })
    const r = await sdk.uploadResult(tid, f, 'remote.bin')
    assert.equal(s.callsTo('POST', '/results$').length, 3)
    assert.equal(s.stored(tid, 'remote.bin').toString(), 'payload')
    assert.equal(r.download_url, `https://pub.example.com/public/tasks/${tid}/results/remote.bin`)

    s.inject('POST', '/results$', 413, { times: 1 })
    await assert.rejects(sdk.uploadResult(tid, f), (e) => e.status === 413)
    assert.equal(s.callsTo('POST', '/results$').length, 4)

    await assert.rejects(sdk.uploadResult(tid, f, 'a/b'), /非法产物文件名/)
    await assert.rejects(sdk.uploadResult(tid, path.join(workDir, 'missing')), /文件不存在/)
    delete process.env.WEBX_WORKER_STORAGE_URL
    delete process.env.WEBX_WORKER_STORAGE_INTERNAL_URL
    await assert.rejects(sdk.uploadResult(tid, f), /未配置存储服务地址/)
  })
})

test('deliverRequired / deliverOnly / deliver：按规则分流；队列条目只记路径', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    Object.assign(process.env, {
      WEBX_WORKER_UPLOADER_ENABLED: '1', WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB: '0',
      WEBX_WORKER_UPLOAD_REQUIRED_GLOBS: '*.pdb, model_*',
    })
    const mk = (n, c) => { const p = path.join(workDir, n); fs.writeFileSync(p, c); return p }
    const big = mk('big.dat', 'bigdata')

    // required：阈值为 0 也恒同步
    const u1 = await sdk.deliverRequired(tid, big, 'big.dat', ['outputs', 'big_url'])
    assert.match(u1, /\/results\/big\.dat$/)
    assert.equal(q.iterPending(workDir).length, 0)

    // only：≥阈值 → 进队列，返回 ''；条目内容符合约定
    assert.equal(await sdk.deliverOnly(tid, big, 'big.dat', ['outputs', 'big_url']), '')
    const ents = q.iterPending(workDir)
    assert.equal(ents.length, 1)
    assert.equal(ents[0].filename, `${tid}__outputs_big.json`)
    assert.equal(ents[0].localPath, fs.realpathSync(big))
    assert.deepEqual(ents[0].keyPath, ['outputs', 'big_url'])
    assert.equal(ents[0].size, 7)
    assert.match(ents[0].nonce, /^[0-9a-f]{16}$/)
    const raw = JSON.parse(fs.readFileSync(ents[0].path, 'utf8'))
    assert.deepEqual(Object.keys(raw).sort(), ['attempts', 'first_queued_at', 'key_path', 'local_path', 'nonce',
      'queued_at', 'remote_name', 'session_id', 'size', 'task_id', 'uploaded_url'])
    assert.equal(s.callsTo('POST', '/results$').length, 1) // 只有 required 那一次

    // deliver：model.pdb 命中规则 → 同步；other.dat 不命中 → 进队列
    assert.match(await sdk.deliver(tid, mk('model.pdb', 'x'), 'model.pdb', ['o', 'm_url']), /model\.pdb$/)
    assert.equal(await sdk.deliver(tid, mk('other.dat', 'x'), 'other.dat', ['o', 'x_url']), '')
    assert.deepEqual(q.iterPending(workDir).map((e) => e.remoteName).sort(), ['big.dat', 'other.dat'])

    // 阈值放大：only 小文件同步传；uploader 关闭：一律同步
    process.env.WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB = '1'
    assert.match(await sdk.deliverOnly(tid, mk('small.dat', 'x'), 'small.dat', ['o', 's_url']), /small\.dat$/)
    process.env.WEBX_WORKER_UPLOAD_ASYNC_THRESHOLD_MB = '0'
    delete process.env.WEBX_WORKER_UPLOADER_ENABLED
    assert.match(await sdk.deliverOnly(tid, mk('s2.dat', 'x'), 's2.dat', ['o', 's2_url']), /s2\.dat$/)

    // 文件不存在 → ''；非法远端名 → 响亮失败
    assert.equal(await sdk.deliverOnly(tid, path.join(workDir, 'nope'), 'nope', ['k']), '')
    await assert.rejects(sdk.deliverOnly(tid, big, '../x', ['k']), /非法产物文件名/)
  })
})

test('deliver：同步上传失败时降级进队列（返回 ""），未开 uploader 则不入队', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    const f = path.join(workDir, 'a.dat')
    fs.writeFileSync(f, 'x')
    s.inject('POST', '/results$', 400, { times: 2 })
    assert.equal(await sdk.deliverRequired(tid, f, 'a.dat', ['k_url']), '')
    assert.equal(q.iterPending(workDir).length, 0)
    process.env.WEBX_WORKER_UPLOADER_ENABLED = 'true'
    s.inject('POST', '/results$', 400, { times: 1 })
    assert.equal(await sdk.deliverRequired(tid, f, 'a.dat', ['k_url']), '')
    assert.equal(q.iterPending(workDir).length, 1)
  })
})

test('uploadQueue：claim / update / release 保留字段；drop 校验 nonce（K17）', () => {
  const wd = tmp()
  const f = path.join(wd, 'out.bin')
  fs.writeFileSync(f, 'abc')
  assert.equal(q.enqueue(wd, 't1', f, 'out.bin', ['step1', 'out_url']), true)
  assert.equal(q.enqueue(wd, 't1', path.join(wd, 'missing'), 'm', ['x']), false)
  assert.equal(q.entrySlug(['step1', 'out_url']), 'step1_out')
  assert.equal(q.entrySlug(['a b/c', 'url']), 'a-b-c_url')
  assert.equal(q.entrySlug(['..']), 'entry')
  assert.equal(q.pendingBytes(wd), 3)
  assert.deepEqual([...q.referencedPaths(wd)], [fs.realpathSync(f)])

  const a = q.claim(wd, q.iterPending(wd)[0])
  assert.ok(a.path.includes(`${path.sep}inflight${path.sep}`))
  assert.equal(q.claim(wd, a), null)             // 已被取走
  a.uploadedUrl = 'http://u'
  q.update(wd, a)
  q.release(wd, a)
  const back = q.iterPending(wd)[0]
  assert.equal(back.attempts, 1)
  assert.equal(back.uploadedUrl, 'http://u')     // 两阶段字段保留
  assert.equal(back.nonce, a.nonce)

  // K17：A 在传输中，用户 redo 同一步 → 同名新条目 B 覆盖 pending
  const a2 = q.claim(wd, back)
  q.enqueue(wd, 't1', f, 'out.bin', ['step1', 'out_url'])
  const b = q.iterPending(wd)[0]
  assert.notEqual(b.nonce, a2.nonce)
  q.release(wd, a2)                               // 不得用旧版本覆盖 B
  assert.equal(q.iterPending(wd)[0].nonce, b.nonce)
  assert.equal(fs.readdirSync(q.inflightDir(wd)).length, 0)
  const a3 = { ...a2, path: null, filename: a2.filename }
  q.drop(wd, a3)                                  // 旧条目 drop 不得误删 B
  assert.equal(q.iterPending(wd).length, 1)
  q.drop(wd, b)                                   // B 自己 drop 才删
  assert.equal(q.iterPending(wd).length, 0)
  q.drop(wd, b)                                   // 幂等

  // requeueInflight：pending 已有同名则不覆盖
  q.enqueue(wd, 't1', f, 'out.bin', ['k_url'])
  const c = q.claim(wd, q.iterPending(wd)[0])
  q.enqueue(wd, 't1', f, 'out.bin', ['k_url'])
  assert.equal(q.requeueInflight(wd), 0)
  assert.equal(fs.readdirSync(q.inflightDir(wd)).length, 0)
  assert.notEqual(q.iterPending(wd)[0].nonce, c.nonce)
  // 损坏条目被清理
  fs.writeFileSync(path.join(q.pendingDir(wd), 'bad__x.json'), '{oops')
  assert.equal(q.iterPending(wd).length, 1)
  assert.ok(!fs.existsSync(path.join(q.pendingDir(wd), 'bad__x.json')))
})

test('downloadArtifact / listArtifacts / downloadArtifactStream（假服务端：200 与 404）', async () => {
  await withTask(async ({ s, tid, workDir }) => {
    const src = path.join(workDir, 's.txt')
    fs.writeFileSync(src, 'hello')
    await sdk.uploadResult(tid, src, 'a b.txt')
    assert.deepEqual(await sdk.listArtifacts(tid), ['a b.txt'])
    const d1 = path.join(workDir, 'dl', 'd1.txt')
    assert.equal(await sdk.downloadArtifact(tid, 'a b.txt', d1), true)
    assert.equal(fs.readFileSync(d1, 'utf8'), 'hello')
    assert.equal(await sdk.downloadArtifact(tid, 'none.txt', path.join(workDir, 'dl', 'x')), false)
    const d2 = path.join(workDir, 'dl', 'd2.txt')
    assert.equal(await sdk.downloadArtifactStream(tid, 'a b.txt', d2), true)
    assert.equal(fs.readFileSync(d2, 'utf8'), 'hello')
    assert.ok(!fs.existsSync(`${d2}.part`))
    const d3 = path.join(workDir, 'dl', 'd3.txt')
    assert.equal(await sdk.downloadArtifactStream(tid, 'none.txt', d3), false)
    assert.ok(!fs.existsSync(`${d3}.part`))
    assert.deepEqual(await sdk.listArtifacts('nosuchtask'), []) // 目录不存在 = 确实为空
    s.inject('GET', '/results$', 500)
    assert.equal(await sdk.listArtifacts(tid), null) // 失败 = 不知道，而不是空
    assert.equal(sdk.resolveStorageUrl('/public/x'), `${s.url}/public/x`)
    assert.equal(sdk.resolveStorageUrl('http://h/y'), 'http://h/y')
  })
})

test('downloadArtifactStream：Range 断点续传 / 416 已完整 / 截断重试后保留 .part', async () => {
  const body = Buffer.from('0123456789'.repeat(100))
  const seen = []
  let mode = 'range'
  const srv = http.createServer((req, res) => {
    seen.push(req.headers.range || '')
    const m = /^bytes=(\d+)-$/.exec(req.headers.range || '')
    if (mode === 'truncate') {
      res.writeHead(200, { 'Content-Length': body.length })
      res.write(body.subarray(0, 100))
      return setTimeout(() => res.destroy(), 20)
    }
    if (m && parseInt(m[1], 10) >= body.length) {
      res.writeHead(416, { 'Content-Range': `bytes */${body.length}` })
      return res.end()
    }
    const start = m ? parseInt(m[1], 10) : 0
    res.writeHead(m ? 206 : 200, {
      'Content-Length': body.length - start,
      ...(m ? { 'Content-Range': `bytes ${start}-${body.length - 1}/${body.length}` } : {}),
    })
    res.end(body.subarray(start))
  })
  await new Promise((r) => srv.listen(0, '127.0.0.1', r))
  Object.assign(process.env, { WEBX_WORKER_STORAGE_INTERNAL_URL: `http://127.0.0.1:${srv.address().port}` })
  const dir = tmp()
  try {
    const f1 = path.join(dir, 'f1.bin')
    fs.writeFileSync(`${f1}.part`, body.subarray(0, 400))
    assert.equal(await sdk.downloadArtifactStream('t', 'f1.bin', f1), true)
    assert.deepEqual(seen, ['bytes=400-'])
    assert.ok(fs.readFileSync(f1).equals(body))
    assert.ok(!fs.existsSync(`${f1}.part`))

    const f2 = path.join(dir, 'f2.bin')
    fs.writeFileSync(`${f2}.part`, body)         // 本地已完整 → 416 且总长相等 → 收尾
    assert.equal(await sdk.downloadArtifactStream('t', 'f2.bin', f2), true)
    assert.ok(fs.readFileSync(f2).equals(body))

    mode = 'truncate'
    const f3 = path.join(dir, 'f3.bin')
    assert.equal(await sdk.downloadArtifactStream('t', 'f3.bin', f3, { maxAttempts: 2, backoffBase: 0 }), false)
    assert.ok(fs.existsSync(`${f3}.part`) && !fs.existsSync(f3)) // .part 保留供下次续传
  } finally {
    await new Promise((r) => { srv.close(r); srv.closeAllConnections() })
  }
})

test('configureTaskLogging：按环境变量取级别，无效值回落 INFO', () => {
  const { LEVELS } = require('../src/log')
  process.env.WEBX_WORKER_TASK_LOG_LEVEL = 'debug'
  assert.equal(logsetup.configureTaskLogging(), LEVELS.DEBUG)
  process.env.WEBX_WORKER_TASK_LOG_LEVEL = 'warn'
  assert.equal(logsetup.configureTaskLogging(), LEVELS.WARNING)
  process.env.WEBX_WORKER_TASK_LOG_LEVEL = 'bogus'
  assert.equal(logsetup.configureTaskLogging(), LEVELS.INFO)
  delete process.env.WEBX_WORKER_TASK_LOG_LEVEL
  assert.equal(logsetup.configureTaskLogging(), LEVELS.INFO)
})
