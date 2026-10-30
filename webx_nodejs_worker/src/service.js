'use strict'

/**
 * systemd 单元生成器 —— `webx-worker service one|multi ...`
 *
 * 部署期运行，生成文件交使用者自行安装（本工具不碰 /etc/systemd）：
 *
 *     service one   ...   单实例：<单元名>.service
 *     service multi ...   多实例：<单元名>@.service 模板单元 + <单元名>.target
 *
 * 设计要点（沿用 Python 版线上验证过的做法）：
 *  · 一 worker 一 unit，故障天然隔离；多实例用模板单元，实例名代入 `%i`。
 *  · 一名三用：%i == channel 配置名 == 环境覆盖层名（<home>/channel_config/%i_env）== worker_id 后缀。
 *  · unit 不承载业务配置：multi 的 ExecStart 只有 `run --config %i`，令牌 / 工作目录 / 日志目录一律进
 *    `.env` / `<名>_env`，所以手动启动与 systemd 托管行为完全等价。
 *  · 分组 target：`PartOf=<单元名>.target` + `WantedBy=<单元名>.target`，一次拉起 / 停止全部实例。
 *  · TimeoutStopSec=30（给 worker 的停机三步留时间）；PrivateTmp=true（日志目录别写 /tmp/...）。
 *  · 每个实例必须独占 WEBX_WORKER_WORK_DIR：`--instances` 会为每个实例写出带互不相同工作目录的覆盖层样例。
 *  · 同目录残留其它基础名的单元文件时打印拆除指引（改名不是迁移，旧单元的符号链接仍在）。
 *
 * 与 Python 版的差异：
 *  · ExecStart 为 `<node 绝对路径> <bin/webx-worker.js 绝对路径> run ...`；无 PYTHONPATH 补丁。
 *  · 删除 --conda-env / --conda-root / --env / --home-fallback 与 conda 启动脚本：Node 版本身不需要 conda。
 *    channel 里的 .py 任务仍可经 channel 配置使用 conda / uv —— 此时 systemd 环境没有 conda，需在
 *    单元里补 `Environment=PATH=...`（含 conda 的 bin）；旧参数仍被接受但只打印提示，便于部署脚本平滑迁移。
 *  · `--python` 改为 `--node`；`service one` 新增 `--config <名>`（写进 ExecStart）。
 *  · `run(argv)` 返回退出码（便于测试）；`main(argv)` 在失败时设置 process.exitCode。
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { parseArgs } = require('node:util')
const cfg = require('./config')

const DEFAULT_UNIT = 'webx-worker'
const BIN_PATH = path.resolve(__dirname, '..', 'bin', 'webx-worker.js')

/** 参数 / 用法错误：run() 捕获后打印并返回 2 */
class UsageError extends Error {}

// ────────────────────────── 单元名校验 / 残留检测 ──────────────────────────

/**
 * 校验并归一化 --unit-name。只做减法（剥后缀）不做加法。拦截四类必然出错、且错得不显眼的输入：
 * 带 .service/.target 后缀（会拼成 xxx.service@.service）、含 '@'、含 '/'、含空白 / 为空。
 */
function normalizeUnitName(raw) {
  let name = String(raw).trim()
  for (const suffix of ['.service', '.target']) {
    if (name.endsWith(suffix)) {
      name = name.slice(0, -suffix.length)
      console.log(`[service] --unit-name 已剥去后缀 ${suffix}：${JSON.stringify(raw)} → ${JSON.stringify(name)}`)
      break
    }
  }
  if (!name) throw new UsageError('--unit-name 不能为空')
  if (name.includes('@')) throw new UsageError(`--unit-name 不能含 '@'（它是模板单元的实例分隔符）: ${JSON.stringify(raw)}`)
  if (name.includes('/')) throw new UsageError(`--unit-name 不能含 '/'（systemd 单元名不是路径）: ${JSON.stringify(raw)}`)
  if (/\s/.test(name)) throw new UsageError(`--unit-name 不能含空白字符: ${JSON.stringify(raw)}`)
  return name
}

/** 输出目录里若残留【其他基础名】的单元文件，打印拆除指引，返回残留文件名 */
function warnStaleUnits(outDir, keep) {
  const stale = fs.readdirSync(outDir).filter((n) => /\.(service|target)$/.test(n) && !keep.has(n)).sort()
  if (!stale.length) return []
  // foo@.service / foo.service / foo.target → foo
  const oldNames = [...new Set(stale.map((n) => (n.includes('@') ? n.split('@')[0] : n.replace(/\.(service|target)$/, ''))))].sort()
  console.log(`\n[service] 输出目录检测到其他基础名的单元文件: ${stale.join(', ')}`)
  console.log('    改名不是迁移 —— 旧单元若已 enable，其符号链接仍在，')
  console.log('    新旧两套会同时拉起同一个 --config，共用工作目录必然互相破坏。')
  console.log('    如旧单元已安装，请先拆除（按实际存在的单元取用）：')
  for (const old of oldNames) {
    console.log(`      sudo systemctl disable --now ${old} '${old}@*'`)
    console.log(`      sudo rm -f /etc/systemd/system/${old}.service /etc/systemd/system/${old}@.service /etc/systemd/system/${old}.target`)
  }
  console.log('      sudo systemctl daemon-reload')
  return stale
}

function currentUser() {
  try { return os.userInfo().username } catch (_) { return process.env.USER || process.env.LOGNAME || 'root' }
}

// ────────────────────────── 模板 ──────────────────────────

// ⚠️ 单元名一律走 __UNIT__ 占位符，绝不能对渲染结果做全局字符串替换：
//    ExecStart 展开后既含路径也含单元名，全局替换会把可执行路径一并改坏。
const SERVICE_ONE_TEMPLATE = `[Unit]
# 【本文件由 \`webx-worker service one\` 生成】
Description=webx_worker (__UNIT__)
After=network.target

[Service]
Type=simple
User=__USER__
Group=__GROUP__
WorkingDirectory=__HOME__

# worker 业务配置（令牌、服务地址、工作目录、日志目录等）由 worker 自身读取
# <主目录>/.env，这里不再注入 EnvironmentFile。
Environment=WEBX_WORKER_HOME=__HOME__
# channel 里若有 conda / uv 的 .py 任务，systemd 环境没有它们：按需补上，如
#   Environment=PATH=/opt/conda/bin:/usr/local/bin:/usr/bin:/bin
# WORKER_ARGS：追加给 worker 的参数，留空即可。常用：--log <dir>、--debug。
#   ⚠️ 多参数必须用双引号包住「整个 VAR=值」：Environment="WORKER_ARGS=--debug --log logs"
#   ExecStart 用无花括号的 $WORKER_ARGS，systemd 替换后再按空白拆成多个 flag；切勿改成 \${WORKER_ARGS}。
Environment=WORKER_ARGS=
ExecStart=__EXEC__ $WORKER_ARGS

Restart=on-failure
RestartSec=5

# 优雅停止：默认 KillMode=control-group，SIGTERM 发给 worker 及其子进程；超时 SIGKILL 兜底。
TimeoutStopSec=30

# 安全加固
NoNewPrivileges=true
PrivateTmp=true

StandardOutput=journal
StandardError=journal
SyslogIdentifier=__UNIT__

[Install]
WantedBy=multi-user.target
`

const SERVICE_MULTI_TEMPLATE = `[Unit]
# 【本文件由 \`webx-worker service multi\` 生成】
# 模板单元：一份文件服务任意多个实例。实例名 %i 即 channel 配置名。
#   启动单个：sudo systemctl enable --now __UNIT__@gpu0
#   启动全部：sudo systemctl start __UNIT__.target
Description=webx_worker (__UNIT__, instance %i)
After=network.target

# PartOf 只向下传播 stop 与 restart（start 方向由 [Install] 的 WantedBy 提供）。
PartOf=__UNIT__.target

[Service]
Type=simple
User=__USER__
Group=__GROUP__
WorkingDirectory=__HOME__

# 业务配置由 worker 自身分两层读取，此处不注入 EnvironmentFile：
#   ① <主目录>/.env                    公共基础层（所有实例共享）
#   ② <主目录>/channel_config/%i_env   本实例覆盖层（可选，worker 按 --config 名自动探测）
# 覆盖层只需写「与公共层不同」的那几行，其余继续继承 .env。
#
# ⚠️ 多个实例绝不可共用同一个 WEBX_WORKER_WORK_DIR（ipc.sock 抢占 / 失败上报快照互相覆盖 /
#    recovery 扫描无归属过滤会删掉兄弟实例正在执行的任务目录）——
#    请在每个 %i_env 里给出**互不相同**的 WEBX_WORKER_WORK_DIR。
Environment=WEBX_WORKER_HOME=__HOME__
# channel 里若有 conda / uv 的 .py 任务，systemd 环境没有它们：按需补上，如
#   Environment=PATH=/opt/conda/bin:/usr/local/bin:/usr/bin:/bin
# ExecStart **刻意只有 run --config %i**：unit 是管理编排，不是配置容器。
# 手动 \`node bin/webx-worker.js run --config <名>\` 与 systemctl start __UNIT__@<名> 行为完全一致。
ExecStart=__EXEC__ --config %i

Restart=on-failure
RestartSec=5
TimeoutStopSec=30

NoNewPrivileges=true
# ⚠️ PrivateTmp 给每个实例独立的 /tmp 命名空间 —— 日志目录必须用相对路径或非 /tmp 的绝对路径
PrivateTmp=true

# SyslogIdentifier 带实例名，journalctl 可按实例过滤：journalctl -u __UNIT__@gpu0 -f
StandardOutput=journal
StandardError=journal
SyslogIdentifier=__UNIT__-%i

[Install]
# enable 时在 __UNIT__.target.wants/ 建符号链接 → target 获得 start 方向的控制
WantedBy=__UNIT__.target
`

const TARGET_TEMPLATE = `[Unit]
# 【本文件由 \`webx-worker service multi\` 生成】
# 实例组锚点：本身不启动任何进程，只作为「所有 worker 实例」的统一开关。
#   sudo systemctl start   __UNIT__.target   # 拉起全部已 enable 的实例
#   sudo systemctl stop    __UNIT__.target   # 停止全部（经各实例的 PartOf 传播）
#   sudo systemctl restart __UNIT__.target   # 重启全部
#   systemctl list-dependencies __UNIT__.target   # 查看当前挂了哪些实例
Description=webx_worker instances (__UNIT__ group target)
After=network.target

[Install]
WantedBy=multi-user.target
`

const overlayTemplate = (name, workDir) => `# ── worker 实例「${name}」环境覆盖层 ──────────────────────────
# 由 \`webx-worker service multi\` 生成，可自由编辑。
#
# 加载时机：worker 启动时按 --config 名自动探测本文件（channel_config/${name}_env），
#   存在即叠加在 <主目录>/.env 之上。语义是「继承 + 覆盖 + 新增」——
#   本文件没写的键继续沿用 .env 的值，只需写出与公共配置**不同**的那几行。
#
# 格式同 .env：KEY=VALUE，一行一个，# 开头为注释。
# 优先级：真实环境变量 > 本文件 > .env。
#
# ⚠️ 两条边界，务必先看：
#
#   ① 路径不做 ~ 展开。请一律写绝对路径。
#
#   ② 本文件**只对 worker 主进程的配置项生效**（src/config.js 的 settings 里定义的那些键）。
#      任务子进程读不到这里：线程数 / GPU 绑定这类子进程级参数请写在
#      channel_config/${name}.js 里各 channel 的 run_env。

# 【必填且每实例必须不同】任务工作目录，**绝对路径**。
# 为何不能用相对路径：本值会随 executor 注入子进程环境，而子进程 cwd 是各自的任务目录，
# 相对路径在那里会解析到错误位置。
#
# ⚠️ 绝不可把多个实例指向同一目录，也不可指向它们的**共同父目录**：
#    · 共用同一目录 → ipc.sock 被后启动者抢占、失败上报快照互相覆盖、recovery 会删掉
#      兄弟实例正在执行的任务目录
#    · 指向父目录 → recovery 把 WORK_DIR 下每个子目录名当作 task_id 去查服务端，
#      其它实例的目录会被当成任务、查到 404 后标记 not_found、7 天后删除
WEBX_WORKER_WORK_DIR=${workDir}

# 日志目录。相对路径按主目录解析（单元 WorkingDirectory 即主目录）。
# 多实例共用同一目录是安全的：文件名以 _{pid} 结尾，互不覆盖。
# ⚠️ 别写成绝对的 /tmp/...：单元里 PrivateTmp=true 会给每个实例独立的 /tmp 命名空间。
WEBX_WORKER_LOG_DIR=logs


# 其余可覆盖项示例（不需要就保持注释）：
# WEBX_WORKER_ID=${name}-box
`

// ────────────────────────── 渲染（纯函数，便于测试） ──────────────────────────

function fill(tpl, vars) {
  let out = tpl
  for (const [k, v] of Object.entries(vars)) out = out.split(`__${k}__`).join(v)
  return out
}

/** systemd 值里：% 是说明符前缀须写成 %%；含空白的路径要加双引号 */
const esc = (s) => String(s).replace(/%/g, '%%')
const quoteArg = (s) => (/[\s"'\\]/.test(s) ? `"${esc(s).replace(/(["\\])/g, '\\$1')}"` : esc(s))

/**
 * 渲染单实例 / 多实例单元。
 *   one:   ExecStart=<node> <bin> run [--config <名>] $WORKER_ARGS
 *   multi: ExecStart=<node> <bin> run --config %i
 * 注意 ExecStart 的 `--config %i` 在 multi 模板里追加，此处只给 `<node> <bin> run`（+ one 的 --config）。
 */
function renderUnit(template, { unit, user, group, home, node, bin = BIN_PATH, config = null }) {
  let exec = `${quoteArg(node)} ${quoteArg(bin)} run`
  if (config) exec += ` --config ${quoteArg(config)}`
  return fill(template, { UNIT: unit, USER: user, GROUP: group, HOME: esc(home), EXEC: exec })
}

// ────────────────────────── CLI ──────────────────────────

const ARG_OPTIONS = {
  'unit-name': { type: 'string', default: DEFAULT_UNIT },
  home: { type: 'string' },
  'out-dir': { type: 'string' },
  node: { type: 'string' },
  config: { type: 'string' },
  user: { type: 'string' },
  group: { type: 'string' },
  instances: { type: 'string' },
  // 以下为 Python 版遗留参数：接受但忽略（见文件头）
  python: { type: 'string' },
  'conda-env': { type: 'string' },
  'conda-root': { type: 'string' },
  env: { type: 'string' },
  'home-fallback': { type: 'string' },
}
const IGNORED_OPTIONS = ['python', 'conda-env', 'conda-root', 'env', 'home-fallback']

const expandHome = (p) => path.resolve(p.startsWith('~') ? path.join(os.homedir(), p.slice(1)) : p)

function parseCli(argv) {
  let parsed
  try {
    parsed = parseArgs({ args: argv, options: ARG_OPTIONS, allowPositionals: true, strict: true })
  } catch (err) {
    throw new UsageError(err.message)
  }
  const mode = parsed.positionals[0]
  if (!['one', 'multi'].includes(mode)) throw new UsageError('用法: service one|multi [--unit-name N] [--home DIR] [--out-dir DIR] [--node PATH] [--user U] [--group G] [--config N | --instances a,b]')
  if (parsed.positionals.length > 1) throw new UsageError(`多余的位置参数: ${parsed.positionals.slice(1).join(' ')}`)
  const v = parsed.values
  if (mode === 'multi' && v.config) throw new UsageError('--config 仅用于 service one（multi 用实例名 %i 作配置名）')
  if (mode === 'one' && v.instances) throw new UsageError('--instances 仅用于 service multi')
  return { mode, v }
}

/** @returns {number} 退出码：0 成功；2 参数错误 */
function run(argv = []) {
  let cli
  try {
    cli = parseCli(argv)
    const { mode, v } = cli
    for (const k of IGNORED_OPTIONS) {
      if (v[k] !== undefined) console.log(`[service] 提示：--${k} 在 Node 版已不适用，已忽略（无 conda 启动脚本；conda / uv 任务需自行在单元里补 PATH）`)
    }
    const home = v.home ? expandHome(v.home) : cfg.workerHome()
    const unit = normalizeUnitName(v['unit-name'])
    const user = v.user || currentUser()
    const outDir = v['out-dir'] ? expandHome(v['out-dir']) : path.join(home, 'deploy')
    fs.mkdirSync(outDir, { recursive: true })
    const renderKw = { unit, user, group: v.group || user, home, node: v.node ? path.resolve(v.node) : process.execPath }

    if (mode === 'one') {
      const file = path.join(outDir, `${unit}.service`)
      fs.writeFileSync(file, renderUnit(SERVICE_ONE_TEMPLATE, { ...renderKw, config: v.config || null }), 'utf8')
      console.log(`[service] 已生成单元: ${file}`)
      console.log(`[service] 部署：sudo cp ${file} /etc/systemd/system/${unit}.service && sudo systemctl daemon-reload && sudo systemctl enable --now ${unit}`)
      warnStaleUnits(outDir, new Set([path.basename(file), `${unit}@.service`, `${unit}.target`]))
      return 0
    }

    const serviceFile = path.join(outDir, `${unit}@.service`)
    const targetFile = path.join(outDir, `${unit}.target`)
    fs.writeFileSync(serviceFile, renderUnit(SERVICE_MULTI_TEMPLATE, renderKw), 'utf8')
    fs.writeFileSync(targetFile, fill(TARGET_TEMPLATE, { UNIT: unit }), 'utf8')

    const names = (v.instances || '').split(',').map((n) => n.trim()).filter(Boolean)
    const overlayDir = path.join(home, 'channel_config')
    for (const name of names) {
      fs.mkdirSync(overlayDir, { recursive: true })
      const p = path.join(overlayDir, `${name}_env`)
      if (fs.existsSync(p)) { console.log(`[service] 覆盖层已存在，跳过: ${p}`); continue }
      fs.writeFileSync(p, overlayTemplate(name, path.join(home, 'tmp', 'tasks', name)), 'utf8')
      console.log(`[service] 已生成覆盖层样例: ${p}`)
    }

    console.log(`[service] 已生成模板单元: ${serviceFile}`)
    console.log(`[service] 已生成组 target: ${targetFile}`)
    warnStaleUnits(outDir, new Set([path.basename(serviceFile), path.basename(targetFile), `${unit}.service`]))
    const list = names.length ? names : ['gpu0']
    console.log('\n部署步骤：')
    console.log(`  sudo cp ${serviceFile} ${targetFile} /etc/systemd/system/`)
    console.log('  sudo systemctl daemon-reload')
    console.log(`  sudo systemctl enable ${unit}.target`)
    for (const name of list) {
      console.log(`  sudo systemctl enable --now ${unit}@${name}      # 需要 channel_config/${name}.js 与（可选）channel_config/${name}_env`)
    }
    console.log(`\n  journalctl -u ${unit}@${list[0]} -f      # 跟踪单个实例`)
    console.log('\n手动测试（与 systemd 行为完全等价，因为 unit 不含任何业务配置）：')
    console.log(`  cd ${home} && node ${BIN_PATH} run --config ${list[0]}`)
    return 0
  } catch (err) {
    if (!(err instanceof UsageError)) throw err
    console.error(`[service] 参数错误: ${err.message}`)
    return 2
  }
}

/** bin 入口：失败时设置 process.exitCode（成功不动它） */
function main(argv = []) {
  const code = run(argv)
  if (code) process.exitCode = code
  return code
}

module.exports = { main, run, renderUnit, normalizeUnitName, warnStaleUnits, UsageError, DEFAULT_UNIT, BIN_PATH }
