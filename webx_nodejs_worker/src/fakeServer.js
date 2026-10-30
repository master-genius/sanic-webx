'use strict'

/**
 * 最小假服务端 —— 按 docs/worker-protocol.md 实现的「任务服务 + 领取服务 + 存储服务」。
 * 测试与 selftest 共用这一份（避免两边漂移）。与 Python 版 fake_server.py 行为对齐。
 *
 * 它是【协议的参考实现】，不是生产服务：单进程、内存状态。但刻意做得「够严」：
 *   · 写端点一律强制 fencing（请求头优先，其次 JSON 体 / 表单字段）；
 *     缺令牌 / 令牌不符 / 终态分别回 403 {ok:false, reason: not_lessee / already_final}；
 *     DELETE /lease 缺令牌回 missing_fencing_token；任务不存在回 403 gone。
 *   · 旧路径一律 404，路径对方法错回 405。
 *   · 心跳被撤销回 200 {ok:false, status:'revoked'}（不是 403）。
 *   · 存储：创建回 201；分片 offset 不符回 409 并带 Upload-Offset 响应头。
 *   · 所有请求按到达顺序记入 calls。
 */

const http = require('node:http')

const TERMINAL = ['completed', 'failed', 'cancelled']
const TRANSITIONS = {
  draft: ['pending', 'draft'],
  pending: ['queued', 'running', 'failed', 'cancelled'],
  queued: ['running', 'failed', 'cancelled'],
  running: ['completed', 'failed', 'cancelled', 'queued'],
}

const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v)

function deepMerge(dst, src) {
  for (const [k, v] of Object.entries(src)) {
    if (isObj(v) && isObj(dst[k])) deepMerge(dst[k], v)
    else dst[k] = v
  }
  return dst
}

/** result_path / log_path 既可能是对象也可能是 JSON 字符串，两种都收 */
function asObj(v) {
  if (isObj(v)) return v
  if (typeof v === 'string' && v.trim()) {
    try { const o = JSON.parse(v); return isObj(o) ? o : {} } catch (_) { return {} }
  }
  return {}
}

/** 最小 multipart/form-data 解析：返回 {form, files} */
function parseMultipart(contentType, raw) {
  const m = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType)
  const form = {}
  const files = {}
  if (!m) return { form, files }
  const delim = Buffer.from('--' + (m[1] || m[2]))
  let pos = raw.indexOf(delim)
  while (pos >= 0) {
    pos += delim.length
    if (raw[pos] === 0x2d && raw[pos + 1] === 0x2d) break // 结束边界
    if (raw[pos] === 0x0d) pos += 2
    const next = raw.indexOf(delim, pos)
    if (next < 0) break
    const part = raw.subarray(pos, next - 2) // 去掉边界前的 \r\n
    const sep = part.indexOf('\r\n\r\n')
    if (sep >= 0) {
      const head = part.subarray(0, sep).toString('utf8')
      const body = part.subarray(sep + 4)
      const name = /name="([^"]*)"/.exec(head)
      const fname = /filename="([^"]*)"/.exec(head)
      if (name) {
        if (fname) files[name[1]] = [fname[1], Buffer.from(body)]
        else form[name[1]] = body.toString('utf8')
      }
    }
    pos = next
  }
  return { form, files }
}

class FakeTaskServer {
  constructor({
    token = 'test-token', storageToken = null, holdSeconds = 0.4, leaseSeconds = 60.0,
    heartbeatLeaseSeconds = 300.0, wrapTaskInfoInData = false, host = '127.0.0.1', port = 0,
  } = {}) {
    this.token = token
    this.storageToken = storageToken === null ? token : storageToken
    this.holdSeconds = holdSeconds
    this.leaseSeconds = leaseSeconds
    this.heartbeatLeaseSeconds = heartbeatLeaseSeconds
    this.wrapTaskInfoInData = wrapTaskInfoInData
    this._host = host
    this._port = port
    this.tasks = new Map()
    this.calls = []
    this.storage = new Map() // `${tid}\0${name}` -> Buffer
    this.uploads = new Map()
    this._seq = 0
    this._order = 0
    this._rules = []
    this.hooks = []
    this._closing = false
    this._waiters = new Set() // 挂起中的领取请求：被戳醒时调用
    this._httpd = null

    const T = '(?<tid>[^/]+)'
    const R = (s) => new RegExp(s)
    this._routes = [
      ['GET', R('^/health$'), this._hHealth],
      ['POST', R('^/task/leases$'), this._hLease],
      ['POST', R('^/task/pokes$'), this._hPoke],
      ['POST', R(`^/internal/tasks/${T}/ack$`), this._hAck],
      ['POST', R(`^/internal/tasks/${T}/heartbeats$`), this._hHeartbeat],
      ['PUT', R(`^/internal/tasks/${T}/status$`), this._hStatus],
      ['POST', R(`^/internal/tasks/${T}/complete$`), this._hComplete],
      ['POST', R(`^/internal/tasks/${T}/fail$`), this._hFail],
      ['DELETE', R(`^/internal/tasks/${T}/lease$`), this._hRelease],
      ['PATCH', R(`^/internal/tasks/${T}/steps$`), this._hSteps],
      ['PATCH', R(`^/internal/tasks/${T}/fields$`), this._hFields],
      ['POST', R(`^/internal/tasks/${T}/deliveries$`), this._hDeliveries],
      ['GET', R(`^/internal/tasks/${T}$`), this._hTaskInfo],
      ['GET', R(`^/internal/tasks/${T}/files$`), this._hFiles],
      ['GET', R(`^/internal/tasks/${T}/files/(?<name>.+)$`), this._hFile],
      // 存储服务（docs/storage-service.md）
      ['POST', R(`^/internal/tasks/${T}/results$`), this._hResultCreate],
      ['POST', R(`^/internal/tasks/${T}/results/uploads$`), this._hUploadCreate],
      ['GET', R('^/internal/uploads/(?<uid>[^/]+)$'), this._hUploadGet],
      ['PATCH', R('^/internal/uploads/(?<uid>[^/]+)$'), this._hUploadPatch],
      ['POST', R('^/internal/uploads/(?<uid>[^/]+)/complete$'), this._hUploadComplete],
      ['DELETE', R('^/internal/uploads/(?<uid>[^/]+)$'), this._hUploadDelete],
      ['GET', R(`^/public/tasks/${T}/results/(?<name>.+)$`), this._hPublicGet],
      ['GET', R(`^/public/tasks/${T}/results$`), this._hPublicList],
    ].map(([m, rx, fn]) => [m, rx, fn.bind(this)])
  }

  // ─────────────────────────── 生命周期 ───────────────────────────
  async start() {
    this._httpd = http.createServer((req, res) => this._handle(req, res))
    this._httpd.keepAliveTimeout = 5000
    await new Promise((resolve) => this._httpd.listen(this._port, this._host, resolve))
    this._port = this._httpd.address().port
    return this
  }

  async stop() {
    this._closing = true
    this._wakeAll()
    if (!this._httpd) return
    const srv = this._httpd
    this._httpd = null
    await new Promise((resolve) => { srv.close(() => resolve()); srv.closeAllConnections() })
  }

  get url() { return `http://${this._host}:${this._port}` }

  // ─────────────────────────── 测试/自检用的控制面 ───────────────────────────
  addTask(taskType, {
    taskId = null, params = {}, files = {}, timeoutSeconds = 3600, level = 0, computePoints = 0,
    steps = [], subStep = '', endStep = '', name = '',
  } = {}) {
    this._order += 1
    const tid = taskId || `task${String(this._order).padStart(11, '0')}`
    const fileContent = new Map(Object.entries(files).map(([n, v]) => [n, Buffer.from(v)]))
    this.tasks.set(tid, {
      id: tid, name: name || tid, task_type: taskType,
      files_json: JSON.stringify([...fileContent.keys()].map((n) => ({ name: n }))),
      files_content: fileContent,
      params_text: JSON.stringify(params),
      steps: [...steps], sub_step: subStep, end_step: endStep, sub_status: '', step_message: '',
      timeout_seconds: timeoutSeconds, allowed_retries: 0, level, compute_points: computePoints,
      lease_seq: 0, worker_id: '', status: 'pending', lease_until: 0, order: this._order,
      is_delete: false, result_path: {}, log_path: {}, error_message: '', start_time: 0.0,
    })
    this._wakeAll()
    return tid
  }

  task(tid) {
    const { files_content: _fc, ...rest } = this.tasks.get(tid)
    return JSON.parse(JSON.stringify(rest))
  }

  setStatus(tid, status) { this.tasks.get(tid).status = status }
  /** 模拟用户取消：下一跳心跳会得到 status=cancelled */
  cancelTask(tid) { this.tasks.get(tid).status = 'cancelled' }
  /** 模拟补偿器回收租约：回 pending，lease_seq+1，租户清空 */
  revokeLease(tid) {
    const t = this.tasks.get(tid)
    Object.assign(t, { status: 'pending', worker_id: '', lease_until: 0 })
    t.lease_seq += 1
    this._wakeAll()
  }

  softDelete(tid) { this.tasks.get(tid).is_delete = true }

  /** 故障注入：匹配的前 times 个请求直接回 status（不进业务逻辑，仍记入 calls） */
  inject(method, pathRe, status, { times = 1, body = null, headers = null } = {}) {
    this._rules.push({
      method: method.toUpperCase(), re: new RegExp(pathRe), status, times,
      body: body !== null ? body : { error: 'injected' }, headers: headers || {},
    })
  }

  callsTo(method, pathRe) {
    const rx = new RegExp(pathRe)
    return this.calls.filter((c) => c.method === method.toUpperCase() && rx.test(c.path))
  }

  async waitFor(predicate, timeout = 15.0, interval = 0.05) {
    const end = Date.now() + timeout * 1000
    while (Date.now() < end) {
      if (predicate()) return true
      await new Promise((r) => setTimeout(r, interval * 1000))
    }
    return Boolean(predicate())
  }

  _wakeAll() { for (const w of [...this._waiters]) w() }

  // ─────────────────────────── 请求处理 ───────────────────────────
  _handle(req, res) {
    const chunks = []
    req.on('data', (c) => chunks.push(c))
    req.on('end', () => {
      try { this._dispatch(req, res, Buffer.concat(chunks)) } catch (e) {
        this._write(res, 500, { error: `fake server bug: ${e && e.stack}` })
      }
    })
    req.on('error', () => {})
  }

  _dispatch(req, res, raw) {
    const url = new URL(req.url, 'http://x')
    const query = Object.fromEntries(url.searchParams)
    const headers = Object.fromEntries(Object.entries(req.headers).map(([k, v]) => [k.toLowerCase(), v]))
    const ctype = headers['content-type'] || ''
    let jbody = null
    let form = null
    let files = null
    if (raw.length && ctype.startsWith('application/json')) {
      try { jbody = JSON.parse(raw.toString('utf8')) } catch (_) { jbody = null }
    } else if (raw.length && ctype.startsWith('multipart/form-data')) {
      ({ form, files } = parseMultipart(ctype, raw))
    }
    const rec = {
      method: req.method, path: url.pathname, query, headers, json: jbody, form,
      size: raw.length, status: null, t: process.hrtime.bigint(),
    }
    this.calls.push(rec)
    for (const h of [...this.hooks]) h(rec)

    this._route(req.method, url.pathname, rec, raw, jbody, form, files, res).then(([status, body, extra]) => {
      rec.status = status
      this._write(res, status, body, extra)
    })
  }

  _write(res, status, body, extra) {
    const data = body === null || body === undefined ? Buffer.alloc(0)
      : Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body))
    const hdrs = { 'Content-Length': data.length }
    if (!extra || !('Content-Type' in extra)) hdrs['Content-Type'] = 'application/json'
    for (const [k, v] of Object.entries(extra || {})) hdrs[k] = String(v)
    try {
      res.writeHead(status, hdrs)
      res.end(data)
    } catch (_) { /* 客户端先断开：不是错误 */ }
  }

  async _route(method, path, rec, raw, jbody, form, files, res) {
    for (const r of this._rules) {
      if (r.times > 0 && r.method === method && r.re.test(path)) {
        r.times -= 1
        return [r.status, r.body, r.headers]
      }
    }
    let matchedOther = false
    for (const [m, rx, fn] of this._routes) {
      const mo = rx.exec(path)
      if (!mo) continue
      if (m !== method) { matchedOther = true; continue }
      if (path !== '/health') {
        const isStorage = path.startsWith('/public/') || path.includes('/results') || path.startsWith('/internal/uploads')
        const want = isStorage ? this.storageToken : this.token
        if (rec.headers['x-internal-token'] !== want && !path.startsWith('/public/')) {
          return [401, { error: 'unauthorized' }, null]
        }
      }
      return fn({ m: { ...mo.groups }, rec, raw, json: jbody, form, files, res })
    }
    return matchedOther ? [405, { error: 'method not allowed' }, null] : [404, { error: 'not found' }, null]
  }

  // ─────────────────────────── fencing ───────────────────────────
  /** 协议 §2：请求头首选，其次 JSON 体 / 表单字段；两者都带时以请求头为准 */
  _tokenOf(rec, jbody, form) {
    const h = rec.headers
    let wid = h['x-worker-id']
    let seq = h['x-lease-seq']
    if (wid === undefined && seq === undefined) {
      const src = isObj(jbody) ? jbody : (form || {})
      wid = src.worker_id
      seq = src.lease_seq
      seq = seq === undefined || seq === null ? undefined : String(seq)
    }
    return [wid || null, seq === '' ? null : (seq === undefined ? null : seq)]
  }

  /** 返回 [task, null] 或 [null, 403 响应三元组] */
  _fence(ctx, isRelease = false) {
    const t = this.tasks.get(ctx.m.tid)
    if (!t || t.is_delete) return [null, [403, { ok: false, reason: 'gone' }, null]]
    const [wid, seq] = this._tokenOf(ctx.rec, ctx.json, ctx.form)
    if (wid === null && seq === null) {
      return [null, [403, { ok: false, reason: isRelease ? 'missing_fencing_token' : 'not_lessee' }, null]]
    }
    if (wid !== null && (seq === null || !/^-?\d+$/.test(String(seq)))) {
      return [null, [403, { ok: false, reason: 'bad_lease_seq' }, null]]
    }
    if (wid !== t.worker_id || parseInt(seq, 10) !== t.lease_seq) {
      return [null, [403, { ok: false, reason: 'not_lessee' }, null]]
    }
    if (TERMINAL.includes(t.status)) return [null, [403, { ok: false, reason: 'already_final' }, null]]
    return [t, null]
  }

  // ─────────────────────────── 领取服务 ───────────────────────────
  _hHealth() { return [200, { ok: true, service: 'task_server', pokes: 0 }, null] }

  _hPoke() { this._wakeAll(); return [200, { ok: true }, null] }

  _claim(wid, slots, maxLevel, maxPoints) {
    const out = []
    for (const slot of slots) {
      const cands = [...this.tasks.values()].filter((t) =>
        t.status === 'pending' && !t.is_delete && slot.includes(t.task_type) &&
        (maxLevel === null || t.level <= maxLevel) &&
        (maxPoints === null || t.compute_points <= maxPoints))
      if (!cands.length) continue
      cands.sort((a, b) => (b.level - a.level) || (b.compute_points - a.compute_points) || (a.order - b.order))
      const t = cands[0]
      t.status = 'queued'
      t.lease_seq += 1
      t.worker_id = wid
      t.lease_until = Date.now() + this.leaseSeconds * 1000
      const o = {}
      for (const k of ['id', 'name', 'task_type', 'files_json', 'params_text', 'steps', 'sub_step',
        'end_step', 'timeout_seconds', 'allowed_retries', 'level', 'compute_points', 'lease_seq',
        'worker_id', 'status']) o[k] = t[k]
      out.push(o)
    }
    return out
  }

  async _hLease(ctx) {
    const body = isObj(ctx.json) ? ctx.json : {}
    const wid = ctx.rec.headers['x-worker-id'] || body.worker_id
    if (!wid) return [400, { error: 'worker_id required' }, null]
    const slots = body.slots
    if (!Array.isArray(slots) || !slots.length || slots.length > 16 ||
        slots.some((s) => !Array.isArray(s) || !s.length || s.length > 32 || s.some((x) => typeof x !== 'string'))) {
      return [400, { error: 'invalid slots' }, null]
    }
    const limits = []
    for (const k of ['max_level', 'max_compute_points']) {
      const v = body[k]
      if (v !== undefined && v !== null && (!Number.isInteger(v) || v < 0)) return [400, { error: `invalid ${k}` }, null]
      limits.push(v === undefined ? null : v)
    }
    const deadline = Date.now() + this.holdSeconds * 1000
    let gone = false
    ctx.res.on('close', () => { gone = true; this._wakeAll() }) // 客户端断开（如 worker 取消了挂起中的领取）
    for (;;) {
      if (!gone) {
        const got = this._claim(wid, slots, limits[0], limits[1])
        if (got.length) return [200, { tasks: got, count: got.length }, null]
      }
      const left = deadline - Date.now()
      if (gone || left <= 0 || this._closing) return [200, { tasks: [], count: 0 }, null]
      await new Promise((resolve) => {
        const done = () => { clearTimeout(timer); this._waiters.delete(done); resolve() }
        const timer = setTimeout(done, left)
        this._waiters.add(done)
      })
    }
  }

  // ─────────────────────────── 任务回调 ───────────────────────────
  _hAck(ctx) {
    const [t, err] = this._fence(ctx)
    if (err) return err
    t.lease_until = Date.now() + this.heartbeatLeaseSeconds * 1000
    return [200, { ok: true, lease_until: t.lease_until, lease_remaining_ms: Math.floor(this.heartbeatLeaseSeconds * 1000) }, null]
  }

  _hHeartbeat(ctx) {
    const t = this.tasks.get(ctx.m.tid)
    if (!t || t.is_delete) return [200, { ok: false, status: 'revoked', reason: 'gone' }, null]
    const [wid, seq] = this._tokenOf(ctx.rec, ctx.json, ctx.form)
    if (wid !== t.worker_id || seq === null || !/^\d+$/.test(String(seq)) || parseInt(seq, 10) !== t.lease_seq) {
      return [200, { ok: false, status: 'revoked', reason: 'not_lessee' }, null]
    }
    if (!TERMINAL.includes(t.status)) t.lease_until = Date.now() + this.heartbeatLeaseSeconds * 1000
    return [200, {
      ok: true, status: t.status, lease_until: t.lease_until,
      lease_remaining_ms: Math.floor(this.heartbeatLeaseSeconds * 1000),
    }, null]
  }

  _hStatus(ctx) {
    const [t, err] = this._fence(ctx)
    if (err) return err
    const body = isObj(ctx.json) ? ctx.json : {}
    const next = body.status
    if (!(next in TRANSITIONS) && !TERMINAL.includes(next)) return [400, { error: 'invalid status' }, null]
    if (next !== t.status && !(TRANSITIONS[t.status] || []).includes(next)) {
      return [400, { error: `invalid transition ${t.status} -> ${next}` }, null]
    }
    t.status = next
    if (next === 'running' && !t.start_time) t.start_time = Date.now() / 1000
    if (body.error_message) t.error_message = body.error_message
    return [200, { message: 'ok' }, null]
  }

  _hComplete(ctx) {
    const [t, err] = this._fence(ctx)
    if (err) return err
    if (t.status !== 'running') return [400, { error: `invalid transition ${t.status} -> completed` }, null]
    t.status = 'completed'
    return [200, { message: 'ok' }, null]
  }

  _hFail(ctx) {
    const [t, err] = this._fence(ctx)
    if (err) return err
    const body = isObj(ctx.json) ? ctx.json : {}
    t.status = 'failed'
    t.error_message = body.error_message || ''
    return [200, { message: 'ok' }, null]
  }

  _hRelease(ctx) {
    // 协议 §4.6：必须带 fencing（仅请求头，DELETE 无请求体）
    const [t, err] = this._fence(ctx, true)
    if (err) return err
    Object.assign(t, { status: 'pending', worker_id: '', lease_until: 0 })
    t.lease_seq += 1 // 任何路径回 pending 都必须 +1
    this._wakeAll()
    return [200, { ok: true }, null]
  }

  _hSteps(ctx) {
    const [t, err] = this._fence(ctx)
    if (err) return err
    const body = isObj(ctx.json) ? ctx.json : {}
    for (const k of ['steps', 'sub_step', 'sub_status', 'step_message']) if (k in body) t[k] = body[k]
    return [200, { ok: true, steps: t.steps, sub_step: t.sub_step, sub_status: t.sub_status }, null]
  }

  _hFields(ctx) {
    const mode = ctx.rec.query.mode || 'overwrite'
    const [t, err] = this._fence(ctx)
    if (err) return err
    if (!['overwrite', 'merge'].includes(mode)) return [400, { error: 'invalid mode' }, null]
    const body = isObj(ctx.json) ? ctx.json : {}
    for (const k of ['result_path', 'log_path']) {
      if (k in body) {
        const nv = asObj(body[k])
        if (mode === 'merge') deepMerge(t[k], nv)
        else t[k] = nv
      }
    }
    return [200, { ok: true }, null]
  }

  _hDeliveries(ctx) {
    // 协议 §4.9：刻意不做 fencing；任务不存在/已软删 → 404，其余 merge
    const t = this.tasks.get(ctx.m.tid)
    const body = isObj(ctx.json) ? ctx.json : {}
    if (!t || t.is_delete) return [404, { error: 'task not found' }, null]
    deepMerge(t.result_path, asObj(body.result_path))
    return [200, { ok: true }, null]
  }

  _hTaskInfo(ctx) {
    const t = this.tasks.get(ctx.m.tid)
    if (!t || t.is_delete) return [404, { error: 'task not found' }, null]
    const info = {}
    for (const k of ['id', 'status', 'lease_seq', 'worker_id', 'task_type', 'steps', 'sub_step', 'end_step']) info[k] = t[k]
    return [200, this.wrapTaskInfoInData ? { data: info } : info, null]
  }

  _hFiles(ctx) {
    const t = this.tasks.get(ctx.m.tid)
    if (!t) return [404, { error: 'task not found' }, null]
    return [200, { files: [...t.files_content.keys()] }, null]
  }

  _hFile(ctx) {
    const name = decodeURIComponent(ctx.m.name)
    const t = this.tasks.get(ctx.m.tid)
    if (!t || !t.files_content.has(name)) return [404, { error: 'file not found' }, null]
    return [200, t.files_content.get(name), { 'Content-Type': 'application/octet-stream' }]
  }

  // ─────────────────────────── 存储服务 ───────────────────────────
  _dl(tid, name) { return `/public/tasks/${tid}/results/${name}` }

  _hResultCreate(ctx) {
    const files = ctx.files || {}
    if (!('file' in files)) return [400, { error: 'file required' }, null]
    const [fname, data] = files.file
    const name = (ctx.form || {}).filename || fname
    const tid = ctx.m.tid
    this.storage.set(`${tid}\0${name}`, data)
    return [201, { task_id: tid, filename: name, download_url: this._dl(tid, name), size: data.length }, null]
  }

  _hUploadCreate(ctx) {
    const body = isObj(ctx.json) ? ctx.json : {}
    if (!body.filename) return [400, { error: 'filename required' }, null]
    this._seq += 1
    const uid = `up${String(this._seq).padStart(8, '0')}`
    this.uploads.set(uid, {
      task_id: ctx.m.tid, filename: body.filename, total: body.total === undefined ? null : body.total,
      data: Buffer.alloc(0),
    })
    return [201, { upload_id: uid, offset: 0 }, { 'Upload-Offset': 0 }]
  }

  _hUploadGet(ctx) {
    const u = this.uploads.get(ctx.m.uid)
    if (!u) return [404, { error: 'upload not found' }, null]
    return [200, { offset: u.data.length }, { 'Upload-Offset': u.data.length }]
  }

  _hUploadPatch(ctx) {
    const u = this.uploads.get(ctx.m.uid)
    if (!u) return [404, { error: 'upload not found' }, null]
    const have = u.data.length
    const want = ctx.rec.headers['upload-offset']
    if (want === undefined || !/^\d+$/.test(want) || parseInt(want, 10) !== have) {
      return [409, { error: 'offset mismatch', offset: have }, { 'Upload-Offset': have }] // 客户端据此重新对齐
    }
    u.data = Buffer.concat([u.data, ctx.raw])
    return [200, { offset: u.data.length }, { 'Upload-Offset': u.data.length }]
  }

  _hUploadComplete(ctx) {
    const u = this.uploads.get(ctx.m.uid)
    if (!u) return [404, { error: 'upload not found' }, null]
    if (u.total !== null && u.data.length !== u.total) {
      return [409, { error: 'incomplete', offset: u.data.length }, { 'Upload-Offset': u.data.length }]
    }
    this.storage.set(`${u.task_id}\0${u.filename}`, u.data)
    this.uploads.delete(ctx.m.uid)
    return [200, { task_id: u.task_id, filename: u.filename, download_url: this._dl(u.task_id, u.filename), size: u.data.length }, null]
  }

  _hUploadDelete(ctx) {
    this.uploads.delete(ctx.m.uid)
    return [200, { deleted: true }, null]
  }

  _hPublicGet(ctx) {
    const data = this.storage.get(`${ctx.m.tid}\0${decodeURIComponent(ctx.m.name)}`)
    if (!data) return [404, { error: 'not found' }, null]
    return [200, data, { 'Content-Type': 'application/octet-stream' }]
  }

  _hPublicList(ctx) {
    const names = [...this.storage.keys()].filter((k) => k.startsWith(`${ctx.m.tid}\0`)).map((k) => k.split('\0')[1]).sort()
    return [200, { files: names }, null]
  }

  /** 读取存储里的产物（测试断言用） */
  stored(tid, name) { return this.storage.get(`${tid}\0${name}`) }
}

module.exports = { FakeTaskServer, TERMINAL, deepMerge, parseMultipart }
