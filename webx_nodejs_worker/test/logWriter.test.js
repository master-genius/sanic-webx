'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const {
  LineAssembler, TruncatedLogWriter, ConcurrentLogWriter, buildRedactRoots, redactForFrontend, MAX_LINE_CHARS,
} = require('../src/logWriter')
const { HourlyRotatingFileHandler, setupLogging, teardownLogging } = require('../src/logConfig')
const { getLogger } = require('../src/log')

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wnw-lw-'))

// ---------- LineAssembler ----------

test('LineAssembler：\\n 定版，\\r 覆盖活动行', () => {
  const a = new LineAssembler()
  assert.deepEqual(a.feed(Buffer.from('hello\nwor')), [['hello', false], ['wor', true]])
  assert.deepEqual(a.feed(Buffer.from('ld\n')), [['world', false]])
  // 进度刷新：整段只产出最后一版，且是定版
  assert.deepEqual(a.feed(Buffer.from('10%\r20%\r30%\n')), [['30%', false]])
  // 跨 chunk 的活动行：每次给出当前快照
  assert.deepEqual(a.feed(Buffer.from('abc\rX')), [['Xbc', true]])
  assert.deepEqual(a.feed(Buffer.from('YZ\r')), [['XYZ', true]]) // 尾部 \r 后光标回行首，内容仍在
  assert.deepEqual(a.feed(Buffer.from('p')), [['pYZ', true]])
  assert.deepEqual(a.close(), [['pYZ', false]])
  assert.deepEqual(a.close(), [])
})

test('LineAssembler：\\r\\n 与空行；跨 chunk 的多字节 UTF-8 不乱码；坏字节不抛', () => {
  const a = new LineAssembler()
  // 注意：\r\n 按终端语义是「回行首 + 换行」，该行内容保持
  assert.deepEqual(a.feed(Buffer.from('a\r\n\nb\n')), [['a', false], ['', false], ['b', false]])
  const buf = Buffer.from('中文日志\n')
  const out = [...a.feed(buf.subarray(0, 4)), ...a.feed(buf.subarray(4))] // 第 2 个汉字被切开
  assert.deepEqual(out.filter(([, p]) => !p), [['中文日志', false]])
  assert.doesNotThrow(() => a.feed(Buffer.from([0xff, 0xfe, 0x0a])))
  a.reset()
  assert.equal(a.line, '')
})

test('LineAssembler：超长无换行行触顶强制定版', () => {
  const a = new LineAssembler()
  const ev = a.feed(Buffer.from('x'.repeat(MAX_LINE_CHARS + 10)))
  assert.equal(ev.length, 1)
  assert.equal(ev[0][1], false)
  assert.equal(a.line, '')
})

// ---------- TruncatedLogWriter ----------

test('TruncatedLogWriter：头 N + 尾 M 截断，总行数 ≤ N+M 时全量', () => {
  const w = new TruncatedLogWriter(2, 3)
  for (let i = 0; i < 5; i++) w.write(`l${i}`)
  assert.equal(w.getContent(), 'l0\nl1\nl2\nl3\nl4')
  for (let i = 5; i < 10; i++) w.write(`l${i}`)
  assert.equal(w.totalLines, 10)
  assert.equal(w.getContent(), 'l0\nl1\n\n... [省略 5 行] ...\n\nl7\nl8\nl9')
})

test('TruncatedLogWriter：活动行只占一个槽位、只计一次行数，但修订号每次递增', () => {
  const w = new TruncatedLogWriter(2, 3)
  w.write('start')
  w.write('10%', true)
  w.write('50%', true)
  w.write('100%', false) // 定版
  assert.equal(w.getContent(), 'start\n100%')
  assert.equal(w.totalLines, 2)
  assert.equal(w.writes, 4)
  w.write('after') // 新行进入尾部
  assert.equal(w.totalLines, 3)
  // 活动行落在尾部缓冲时同样原地覆盖
  w.write('p1', true)
  w.write('p2', true)
  w.write('p3', false)
  assert.equal(w.getContent(), 'start\n100%\nafter\np3')
})

test('TruncatedLogWriter：tailLines=0 时活动行覆盖不抛异常', () => {
  const w = new TruncatedLogWriter(1, 0)
  w.write('h')
  w.write('a', true)
  assert.doesNotThrow(() => { w.write('b', true); w.write('c', false); w.write('d') })
})

// ---------- 脱敏 ----------

test('redactForFrontend：基础设施日志整行剔除，URL / 内部路径遮蔽', () => {
  const text = [
    'normal line',
    'POST http://x/internal/tasks/abc/results ok',
    '结果文件上传 成功',
    'see https://example.com/a?b=1 for details',
    '',
    '',
    '',
    'end',
  ].join('\n')
  assert.equal(redactForFrontend(text), 'normal line\nsee [redacted] for details\n\nend')
})

test('redactForFrontend：路径脱敏（已知根 → 标签；家目录；兜底折叠；不误伤）', () => {
  const roots = [['/data/worker/tasks/t1', '[TASK_DIR]'], ['/data/worker', '[WORKER]'], ['/home/user', '[HOME]']]
  const out = redactForFrontend([
    'File "/data/worker/tasks/t1/run.py", line 3',
    'at /data/worker/executor.js',
    '/home/user/proj/a.py',
    '/home/user张三/x/y', // 已知根按路径段匹配，不能段中途替换
    '/home/alice/data',
    '/opt/tool/lib/python3.11/site.py',
    '/var/log', // 太短不折
    '/tmp/a/b/c', // 不在名单内原样保留
    'kJ/mol/nm and 2026/07/29 and /homework/a/b/c',
  ].join('\n'), roots)
  assert.equal(out, [
    'File "[TASK_DIR]/run.py", line 3',
    'at [WORKER]/executor.js',
    '[HOME]/proj/a.py',
    '[HOME]/x/y',
    '[HOME]/data',
    '…/python3.11/site.py',
    '/var/log',
    '/tmp/a/b/c',
    'kJ/mol/nm and 2026/07/29 and /homework/a/b/c',
  ].join('\n'))
  assert.ok(!out.split('\n')[3].includes('[HOME]张三'), '不得产生 [HOME]张三 这种段中途替换')
})

test('redactForFrontend：业务 redactors 先于内置规则；抛错的 redactor 被跳过', () => {
  // 业务规则依赖原文里的 URL 作锚点：若先遮蔽 URL，这个锚点就落空了
  const dropBanner = (t) => t.replace(/BANNER start\n[\s\S]*?see https:\/\/vendor\.example\/x\n/, '')
  const boom = () => { throw new Error('x') }
  const text = 'BANNER start\nsecret\nsee https://vendor.example/x\nkeep'
  assert.equal(redactForFrontend(text, [], [boom, dropBanner]), 'keep')
})

test('buildRedactRoots：按前缀长度倒序、同前缀保留首次标签、丢弃根目录', () => {
  const roots = buildRedactRoots({
    workDir: '/data/w', condaRoot: '/opt/conda', home: '/data/w', workerRoot: '/opt/conda/envs/x/worker', scriptRoot: '/',
    channels: { a: { env: 'conda', env_name: 'py' }, b: { env: 'uv', env_name: '/opt/uv/py' }, c: 'bad' },
  })
  const m = new Map(roots)
  assert.equal(m.get('/opt/conda/envs/py'), '[ENV]')
  assert.equal(m.get('/opt/uv/py'), '[ENV]')
  assert.equal(m.get('/data/w'), '[WORK_DIR]') // 先登记者胜出，不是 [HOME]
  assert.equal(m.get('/opt/conda'), '[CONDA]')
  assert.ok(!m.has('/') && !m.has(''))
  const lens = roots.map(([p]) => p.length)
  assert.deepEqual(lens, [...lens].sort((x, y) => y - x))
})

// ---------- ConcurrentLogWriter ----------

test('ConcurrentLogWriter：本地文件逐条追加、截断缓冲脱敏、计数与关闭', async () => {
  const dir = tmp()
  const logPath = path.join(dir, 't1', 'run.log')
  const w = new ConcurrentLogWriter({ headLines: 2, tailLines: 2, redactRoots: [['/nonexistent/static', '[S]']] })
  assert.equal(w.getTruncatedContent('t1'), null)
  assert.equal(w.getWriteCount('t1'), 0)

  await w.write('t1', logPath, `开始 ${logPath}\n`)
  await w.write('t1', logPath, 'POST /internal/tasks/t1/results\n') // 被剔除
  await w.write('t1', logPath, '10%', { partial: true })
  await w.write('t1', logPath, '90%\n', { partial: true })
  await w.write('t1', logPath, '100%\n') // 定版
  await w.write('t1', logPath, 'tail1\n')
  await w.write('t1', logPath, 'tail2\n')

  // 本地文件保留全部原文（含活动行的每个快照），缺换行的补上
  assert.equal(fs.readFileSync(logPath, 'utf8'),
    `开始 ${logPath}\nPOST /internal/tasks/t1/results\n10%\n90%\n100%\ntail1\ntail2\n`)
  assert.equal(w.getTotalLines('t1'), 5) // 活动行只计一次
  assert.equal(w.getWriteCount('t1'), 7) // 含活动行覆盖

  // 前端出口：任务目录被收敛成 [TASK_DIR]，基础设施日志被剔除，且做了头尾截断
  const content = w.getTruncatedContent('t1')
  assert.ok(content.startsWith('开始 [TASK_DIR]/run.log'), content)
  assert.ok(!content.includes('/internal/tasks'), content)
  assert.ok(content.includes('省略 1 行'), content)
  assert.ok(content.endsWith('tail1\ntail2'), content)

  const saved = w.saveTruncated('t1')
  assert.equal(saved, path.join(dir, 't1', 'run.truncated.log'))
  assert.ok(fs.existsSync(saved))

  await w.closeTask('t1')
  assert.equal(w.getTruncatedContent('t1'), null)
  assert.equal(w.getTotalLines('t1'), 0)
  fs.rmSync(dir, { recursive: true, force: true })
})

test('ConcurrentLogWriter：多任务互相隔离；redactors 引用后续追加同样生效', async () => {
  const dir = tmp()
  const redactors = []
  const w = new ConcurrentLogWriter({ redactors })
  await w.write('a', path.join(dir, 'a', 'run.log'), 'SECRET-a\n')
  await w.write('b', path.join(dir, 'b', 'run.log'), 'only b\n')
  redactors.push((t) => t.replace(/SECRET-\w+/g, 'XXX')) // 构造后才追加
  assert.equal(w.getTruncatedContent('a'), 'XXX')
  assert.equal(w.getTruncatedContent('b'), 'only b')
  await w.closeTask('a')
  await w.closeTask('b')
  fs.rmSync(dir, { recursive: true, force: true })
})

// ---------- logConfig ----------

test('HourlyRotatingFileHandler：文件名格式、同小时按行数滚动、跨小时换文件、同进程续写', () => {
  const dir = tmp()
  let now = new Date(2026, 5, 12, 8, 30).getTime()
  const h = new HourlyRotatingFileHandler(dir, { maxLines: 2, pid: 4242, now: () => now })
  for (let i = 0; i < 5; i++) h.writeText(`line${i}\n`)
  now = new Date(2026, 5, 12, 9, 1).getTime()
  h.writeText('next hour\n')
  h.close()
  const files = fs.readdirSync(dir).sort()
  assert.deepEqual(files, [
    'worker_task_2026-06-12-08_4242.1.log',
    'worker_task_2026-06-12-08_4242.2.log',
    'worker_task_2026-06-12-08_4242.log',
    'worker_task_2026-06-12-09_4242.log',
  ])
  assert.equal(fs.readFileSync(path.join(dir, 'worker_task_2026-06-12-08_4242.log'), 'utf8'), 'line0\nline1\n')
  assert.equal(fs.readFileSync(path.join(dir, 'worker_task_2026-06-12-08_4242.2.log'), 'utf8'), 'line4\n')

  // 重开：按已有文件内容接续最大 seq，计数器不丢（.2.log 已有 1 行，再写 1 行不滚，再写 1 行滚到 .3）
  now = new Date(2026, 5, 12, 8, 59).getTime()
  const h2 = new HourlyRotatingFileHandler(dir, { maxLines: 2, pid: 4242, now: () => now })
  h2.writeText('line5\n')
  h2.writeText('line6\n')
  h2.close()
  assert.equal(fs.readFileSync(path.join(dir, 'worker_task_2026-06-12-08_4242.2.log'), 'utf8'), 'line4\nline5\n')
  assert.equal(fs.readFileSync(path.join(dir, 'worker_task_2026-06-12-08_4242.3.log'), 'utf8'), 'line6\n')
  fs.rmSync(dir, { recursive: true, force: true })
})

test('HourlyRotatingFileHandler：按字节滚动；单条超大消息不空转', () => {
  const dir = tmp()
  const h = new HourlyRotatingFileHandler(dir, { maxBytes: 10, pid: 1, now: () => new Date(2026, 0, 1, 0).getTime() })
  h.writeText('x'.repeat(50) + '\n') // 首条超限但文件为空：照写，不产生空文件
  h.writeText('ab\n')
  h.close()
  assert.deepEqual(fs.readdirSync(dir).sort(), ['worker_task_2026-01-01-00_1.1.log', 'worker_task_2026-01-01-00_1.log'])
  fs.rmSync(dir, { recursive: true, force: true })
})

test('setupLogging：log.js 输出与 stdout/stderr 一并 tee 落盘；重入幂等；teardown 还原', () => {
  const dir = tmp()
  const origErr = process.stderr.write
  const origOut = process.stdout.write
  try {
    const eff = setupLogging({ logDir: dir, debug: true })
    assert.equal(eff, path.resolve(dir))
    getLogger('t').info('hello-info')
    getLogger('t').debug('hello-debug')
    process.stdout.write('raw-stdout\n')
    setupLogging({ logDir: dir, debug: true }) // 重入：不应 tee 套 tee
    getLogger('t').warning('once-only')
    teardownLogging()
    assert.equal(process.stderr.write, origErr)
    assert.equal(process.stdout.write, origOut)
    const files = fs.readdirSync(dir)
    assert.equal(files.length, 1)
    assert.match(files[0], new RegExp(`^worker_task_\\d{4}-\\d{2}-\\d{2}-\\d{2}_${process.pid}\\.log$`))
    const text = fs.readFileSync(path.join(dir, files[0]), 'utf8')
    assert.ok(text.includes('hello-info') && text.includes('hello-debug') && text.includes('raw-stdout'), text)
    assert.equal(text.split('once-only').length - 1, 1, '同一条日志只落盘一次')
    assert.equal(setupLogging({}), null) // 仅控制台
  } finally {
    teardownLogging()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})
