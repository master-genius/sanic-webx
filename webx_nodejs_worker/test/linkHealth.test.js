'use strict'

// linkHealth.js：分类函数、记账状态机（首报 / 限流 / 换类 / 恢复）、httpClient 观测挂接、启动自检、真实 TLS 错误码

const test = require('node:test')
const assert = require('node:assert/strict')
const { execFileSync } = require('node:child_process')
const fs = require('node:fs')
const https = require('node:https')
const os = require('node:os')
const path = require('node:path')

const { settings } = require('../src/config')
const log = require('../src/log')
const httpClient = require('../src/httpClient')
const lh = require('../src/linkHealth')
const { WebCallbackClient } = require('../src/callback')
const { FakeTaskServer } = require('../src/fakeServer')

/** 捕获日志行（同时抑制 stderr 输出） */
function capture() {
  const lines = []
  const off = log.addSink((l) => lines.push(l))
  return { lines, off }
}
const errWith = (props, msg = 'x') => Object.assign(new Error(msg), props)

let srv
let cb
test.before(async () => {
  srv = await new FakeTaskServer({ holdSeconds: 0.1 }).start()
  Object.assign(settings, { WEB_BASE_URL: srv.url, INTERNAL_TOKEN: 'test-token', WORKER_ID: 'w1' })
})
test.after(async () => { await srv.stop() })
test.beforeEach(() => {
  srv.calls.length = 0
  srv._rules.length = 0
  settings.WEB_BASE_URL = srv.url
  settings.INTERNAL_TOKEN = 'test-token'
  cb = new WebCallbackClient()
  lh.linkHealth.noteSuccess() // 清掉上一个用例遗留的单例状态
})

test('classifyStatus：只认 401，不认 403', () => {
  assert.equal(lh.classifyStatus(401), lh.KIND_AUTH)
  for (const s of [200, 400, 403, 404, 500, 502]) assert.equal(lh.classifyStatus(s), null, String(s))
})

test('classifyException：Node 的 TLS 错误码 / cause 链 / 文本兜底 → tls；瞬时故障 → null', () => {
  for (const code of ['UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'CERT_HAS_EXPIRED', 'DEPTH_ZERO_SELF_SIGNED_CERT',
    'SELF_SIGNED_CERT_IN_CHAIN', 'ERR_TLS_CERT_ALTNAME_INVALID', 'UNABLE_TO_GET_ISSUER_CERT_LOCALLY', 'ERR_SSL_WRONG_VERSION_NUMBER']) {
    assert.equal(lh.classifyException(errWith({ code })), lh.KIND_TLS, code)
  }
  assert.equal(lh.classifyException(new Error('wrapped', { cause: errWith({ code: 'CERT_HAS_EXPIRED' }) })), lh.KIND_TLS)
  const agg = Object.assign(new Error('agg'), { errors: [errWith({ code: 'ECONNREFUSED' }), errWith({ code: 'CERT_UNTRUSTED' })] })
  assert.equal(lh.classifyException(agg), lh.KIND_TLS)
  assert.equal(lh.classifyException(new Error('self-signed certificate in certificate chain')), lh.KIND_TLS)
  assert.equal(lh.classifyException(new Error('tlsv1 alert protocol version')), lh.KIND_TLS)

  for (const code of ['ECONNREFUSED', 'ECONNRESET', 'ENOTFOUND', 'EAI_AGAIN', 'ETIMEDOUT', 'EHOSTUNREACH']) {
    assert.equal(lh.classifyException(errWith({ code })), null, code)
  }
  assert.equal(lh.classifyException(new httpClient.HttpTimeoutError('https://ssl.example.com/x', 5)), null, 'URL 里的 ssl 不得误判')
  assert.equal(lh.classifyException(errWith({ code: 'ENOTFOUND' }, 'getaddrinfo ENOTFOUND ssl.example.com')), null)
  assert.equal(lh.classifyException(Object.assign(new Error('aborted'), { name: 'AbortError' })), null)
  assert.equal(lh.classifyException(new Error('boom')), null)
  assert.equal(lh.classifyException(null), null)
})

test('LinkHealth：首次立刻告警；持续期限流；换类重新计数；恢复收尾并归零；健康时成功静默', () => {
  const { lines, off } = capture()
  try {
    const h = new lh.LinkHealth(60)
    h.noteSuccess()
    assert.equal(lines.length, 0)

    h.noteFailure(lh.KIND_AUTH, 'HTTP 401 /a', { taskId: 'T1' })
    assert.equal(lines.length, 1)
    assert.match(lines[0], /ERROR.*持续失败（auth）.*累计 1 次/s)
    assert.match(lines[0], /样本 task=T1/)
    assert.match(lines[0], /X-Internal-Token/) // auth 的排查清单
    assert.equal(h.failing, true)

    h.noteFailure(lh.KIND_AUTH, 'HTTP 401 /a')
    h.noteFailure(lh.KIND_AUTH, 'HTTP 401 /a')
    assert.equal(lines.length, 1, '间隔内不刷屏')
    assert.equal(h.failureCount, 3)

    h._lastAlarmMono -= 61 // 把上次告警时刻拨回去 61 秒
    h.noteFailure(lh.KIND_AUTH, 'HTTP 401 /a')
    assert.equal(lines.length, 2, '到点继续报，不沉默')
    assert.match(lines[1], /累计 4 次/)

    h.noteFailure(lh.KIND_TLS, 'CERT_HAS_EXPIRED')
    assert.equal(lines.length, 3, '换类视为新一轮，立即报')
    assert.match(lines[2], /累计 1 次/)
    assert.match(lines[2], /fullchain/)

    h.noteSuccess()
    assert.equal(lines.length, 4)
    assert.match(lines[3], /INFO.*已恢复.*tls/s)
    assert.equal(h.failing, false)
    assert.equal(h.failureCount, 0)
    h.noteSuccess()
    assert.equal(lines.length, 4)
  } finally { off() }
})

test('httpClient 观测挂接：401 记故障；403 / 400 / 404 视为链路健康；连接被拒不记；恢复后收尾', async () => {
  const { lines, off } = capture()
  try {
    settings.INTERNAL_TOKEN = 'wrong'
    const r = await cb.completeTask('probe-task-00001') // 401：前台只试 1 次
    assert.equal(r, false)
    assert.equal(lh.linkHealth.failing, true)
    assert.ok(lines.some((l) => /持续失败（auth）/.test(l) && /HTTP 401/.test(l)))
    assert.equal(srv.callsTo('POST', '/complete$').length, 1)

    settings.INTERNAL_TOKEN = 'test-token' // 令牌修好：服务端给出 403（无租约）也算链路通
    await cb.request('GET', `${srv.url}/internal/tasks/nope000000000000`)
    assert.equal(lh.linkHealth.failing, false)
    assert.ok(lines.some((l) => /已恢复/.test(l)))

    lines.length = 0
    srv.addTask('demo', { taskId: 'obs0000000000001' })
    await cb.request('POST', `${srv.url}/internal/tasks/obs0000000000001/complete`, {}) // 403 not_lessee
    assert.equal(lh.linkHealth.failing, false, '403 不是鉴权故障')

    await assert.rejects(cb.request('GET', 'http://127.0.0.1:1/internal/tasks/x'))
    assert.equal(lh.linkHealth.failing, false, '连接被拒是瞬时故障')
    assert.equal(lines.filter((l) => /ERROR/.test(l)).length, 0)
  } finally { off() }
})

test('observer 可摘除：摘除后 401 不再记账', async () => {
  lh.uninstallObserver()
  try {
    settings.INTERNAL_TOKEN = 'wrong'
    await cb.request('GET', `${srv.url}/internal/tasks/x0000000000000`)
    assert.equal(lh.linkHealth.failing, false)
  } finally { lh.installObserver() }
})

test('checkWebLink：404 / 200 通；401 立即返回 auth 不重试；403 与 5xx 归 unreachable 且会重试；连接被拒 unreachable', async () => {
  assert.deepEqual(await lh.checkWebLink(cb, { attempts: 3, delaySec: 0.01 }), [true, null, ''])
  assert.equal(srv.callsTo('GET', `/internal/tasks/${lh.PROBE_TASK_ID}$`).length, 1)

  srv.addTask('demo', { taskId: lh.PROBE_TASK_ID }) // 极端情况下该 ID 真存在 → 200
  assert.equal((await lh.checkWebLink(cb))[0], true)

  srv.calls.length = 0
  settings.INTERNAL_TOKEN = 'wrong'
  let [ok, kind, detail] = await lh.checkWebLink(cb, { attempts: 3, delaySec: 0.01 })
  assert.deepEqual([ok, kind], [false, lh.KIND_AUTH])
  assert.match(detail, /HTTP 401/)
  assert.equal(srv.calls.length, 1, '确定性故障不重试')
  settings.INTERNAL_TOKEN = 'test-token'

  srv.calls.length = 0
  srv.inject('GET', '/internal/tasks/', 403, { times: 9 })
  ;[ok, kind, detail] = await lh.checkWebLink(cb, { attempts: 3, delaySec: 0.01 })
  assert.deepEqual([ok, kind], [false, lh.KIND_UNREACHABLE])
  assert.equal(srv.calls.length, 3, '403 不属于鉴权，按不可达重试')

  srv._rules.length = 0
  srv.inject('GET', '/internal/tasks/', 502, { times: 1 })
  assert.equal((await lh.checkWebLink(cb, { attempts: 3, delaySec: 0.01 }))[0], true, '启动竞态：重试后通')

  settings.WEB_BASE_URL = 'http://127.0.0.1:1'
  ;[ok, kind, detail] = await lh.checkWebLink(cb, { attempts: 2, delaySec: 0.01, timeout: 1 })
  assert.deepEqual([ok, kind], [false, lh.KIND_UNREACHABLE])
  assert.match(detail, /ECONNREFUSED/)
})

test('selfcheckAndReport：返回布尔并打横幅，横幅按类别给排查清单', async () => {
  const { lines, off } = capture()
  try {
    assert.equal(await lh.selfcheckAndReport(cb), true)
    assert.match(lines.at(-1), /INFO.*内部回调链路正常/)

    settings.INTERNAL_TOKEN = 'wrong'
    assert.equal(await lh.selfcheckAndReport(cb), false)
    const banner = lines.at(-1)
    assert.match(banner, /自检失败/)
    assert.match(banner, /X-Internal-Token/)
    assert.doesNotMatch(banner, /upstream/)
    lh.logLinkCheckBanner(false, lh.KIND_UNREACHABLE, 'x', 'http://b') // 不同类别给不同清单
    assert.match(lines.at(-1), /upstream/)
    assert.doesNotMatch(lines.at(-1), /X-Internal-Token/)
  } finally { off() }
})

test('真实 TLS：自签名证书服务端 → 请求报 Node 的证书错误码，被判为 tls 并触发告警；自检立即返回', async (t) => {
  let dir
  try {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-tls-'))
    execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=localhost',
      '-keyout', path.join(dir, 'k.pem'), '-out', path.join(dir, 'c.pem')], { stdio: 'ignore' })
  } catch (_) { return t.skip('没有 openssl，跳过') }

  const server = https.createServer({ key: fs.readFileSync(path.join(dir, 'k.pem')), cert: fs.readFileSync(path.join(dir, 'c.pem')) },
    (req, res) => { res.writeHead(404); res.end('{}') })
  await new Promise((r) => server.listen(0, '127.0.0.1', r))
  const { lines, off } = capture()
  try {
    settings.WEB_BASE_URL = `https://localhost:${server.address().port}`
    const err = await httpClient.request('GET', `${settings.WEB_BASE_URL}/internal/tasks/x`, { timeout: 3 }).then(() => null, (e) => e)
    assert.ok(err, '自签名证书必须被拒绝')
    assert.equal(lh.classifyException(err), lh.KIND_TLS, `code=${err.code} msg=${err.message}`)
    assert.equal(lh.linkHealth.failing, true)
    assert.ok(lines.some((l) => /持续失败（tls）/.test(l)))

    const [ok, kind] = await lh.checkWebLink(cb, { attempts: 3, delaySec: 0.01, timeout: 3 })
    assert.deepEqual([ok, kind], [false, lh.KIND_TLS])
  } finally {
    off()
    server.closeAllConnections()
    await new Promise((r) => server.close(r))
    fs.rmSync(dir, { recursive: true, force: true })
  }
})
