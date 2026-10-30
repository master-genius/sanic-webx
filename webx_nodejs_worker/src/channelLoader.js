'use strict'

/**
 * Channel 配置加载器。
 *
 * channel 配置是一个 CommonJS 文件（被 require 加载），约定这些导出：
 *
 *   module.exports = {
 *     CHANNELS: {                       // 必填。键 = task_type = 本 worker 的能力声明
 *       demo: {
 *         script: 'demo_task.js',       // 相对 SCRIPT_ROOT（缺省 = 本文件所在目录），或绝对路径
 *         env: '',                      // 'conda' | 'uv' | ''（直接执行，仅对 .py 等非 js/sh 脚本生效）
 *         env_name: '',                 // conda 环境名 / uv 的 python 路径
 *         executor: 'python',           // env='' 时的解释器（.js 固定用当前 node，.sh 固定用 bash）
 *         run_env: { K: 'V' },          // 注入子进程的环境变量（机器级设备绑定等）
 *       },
 *     },
 *     LANES: [['a', 'b'], ['c']],       // 可选。空/缺省 = 所有 channel 归入一条 Lane
 *     CONDA_ROOT: '/opt/conda',         // 可选。缺省自动探测
 *     SCRIPT_ROOT: '/path/to/scripts',  // 可选。相对脚本路径的解析根
 *     PLUGIN: new MyPlugin(),           // 可选。TaskKindPlugin 实例（见 plugin.js）
 *   }
 *
 * 目录约定（锚定在 worker 主目录，不是包源码目录）：
 *   <home>/.env
 *   <home>/channel_config/default.js   ← 缺省配置
 *   <home>/channel_config/gpu0.js      ← --config gpu0
 *   <home>/channel_config/gpu0_env     ← gpu0 的实例覆盖层（可选）
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { workerHome } = require('./config')
const { TaskKindPlugin } = require('./plugin')

const DEFAULT_CONFIG_DIR = 'channel_config'
const DEFAULT_CONFIG_NAME = 'default.js'
const CONFIG_EXTS = ['.js', '.cjs']

/**
 * 解析配置文件路径：
 *   未指定 → <home>/channel_config/default.js；
 *   裸文件名 → <home>/channel_config/<name>（缺扩展名自动补 .js）；
 *   带路径 / 绝对路径 → 直接使用。
 */
function resolveConfigPath(customPath, home) {
  const configDir = path.join(home || workerHome(), DEFAULT_CONFIG_DIR)
  if (!customPath) return path.join(configDir, DEFAULT_CONFIG_NAME)
  if (path.basename(customPath) === customPath && !path.isAbsolute(customPath)) {
    const name = CONFIG_EXTS.includes(path.extname(customPath)) ? customPath : `${customPath}.js`
    return path.resolve(configDir, name)
  }
  return path.resolve(customPath)
}

const TEMPLATE = `'use strict'
// webx_nodejs_worker — Channel 配置文件（首次启动自动生成，请按需编辑）
//
// CHANNELS 的键就是 task_type —— 它同时是本 worker 的「能力声明」：
// 每次领取请求都会带上「我这个空槽能跑哪些 task_type」，服务端不存能力注册表。
//
// 每个 channel 的字段：
//   script     任务脚本。相对路径按 SCRIPT_ROOT（缺省 = 本文件所在目录）解析，也可写绝对路径。
//              .js/.cjs/.mjs 用当前 node，.sh 用 bash，其余走 env / executor
//   env        'conda' → conda run -n <env_name> python <script>
//              'uv'    → uv run [--python <env_name>] <script>
//              ''      → 直接执行（用 executor 指定的解释器，缺省 python）
//   env_name   conda 环境名 / uv 的 python 路径
//   executor   env='' 时的解释器路径
//   run_env    注入子进程的环境变量（字符串）——GPU/线程绑定这类「机器属性」放这里
//
// 任务超时由任务服务下发（timeout_seconds），不在此处配置。
module.exports = {
  CHANNELS: {
    // demo: { script: 'demo_task.js' },
  },

  // Lane 分组（可选）。每个子数组是一条 Lane = 一个槽位：组内任务串行，组间并行。
  // 空数组 = 所有 channel 归入【一条】Lane。并发的扩展轴是【进程】（多起 worker 实例）。
  LANES: [],

  // 业务接缝（可选）：见 src/plugin.js
  // PLUGIN: new (require('webx_nodejs_worker/src/plugin').TaskKindPlugin)(),
}
`

/** 配置不存在则写出最小模板。返回 true = 已存在可直接加载；false = 刚创建，需编辑后重启 */
function ensureConfigExists(configPath) {
  if (fs.existsSync(configPath)) return true
  fs.mkdirSync(path.dirname(configPath), { recursive: true })
  fs.writeFileSync(configPath, TEMPLATE, 'utf8')
  const bar = '='.repeat(60)
  console.log(`\n${bar}\n  Channel 配置文件已创建: ${configPath}\n  请编辑此文件，声明本 worker 能跑的任务类型（CHANNELS）后，\n  重新启动 Worker。\n${bar}\n`)
  return false
}

/** PLUGIN 的鸭子类型检查：配置文件 require 到的可能是另一份拷贝，instanceof 不可靠 */
function isPlugin(p) {
  return p instanceof TaskKindPlugin ||
    (p && ['logTarget', 'selectInputs', 'extendEnv', 'softCleanPolicy'].every((m) => typeof p[m] === 'function'))
}

/** 加载 channel 配置，返回 {CHANNELS, CONDA_ROOT, LANES, PLUGIN, SCRIPT_ROOT} */
function loadChannelConfig(configPath) {
  const full = path.resolve(configPath)
  delete require.cache[require.resolve(full)] // 允许同进程重复加载（测试 / doctor）
  const mod = require(full)

  const channels = mod.CHANNELS || {}
  if (!channels || Object.keys(channels).length === 0) {
    throw new Error(`配置文件中未定义 CHANNELS: ${full}`)
  }
  let plugin = mod.PLUGIN
  if (plugin == null) plugin = new TaskKindPlugin()
  else if (!isPlugin(plugin)) throw new TypeError(`PLUGIN 必须是 TaskKindPlugin 的实例: ${full}`)

  const scriptRoot = mod.SCRIPT_ROOT || path.dirname(full)
  return {
    CHANNELS: channels,
    CONDA_ROOT: mod.CONDA_ROOT || detectCondaRoot(),
    LANES: Array.isArray(mod.LANES) ? mod.LANES : [],
    PLUGIN: plugin,
    SCRIPT_ROOT: path.resolve(String(scriptRoot).replace(/^~/, os.homedir())),
  }
}

/**
 * 构建执行命令前缀（不含脚本路径）。
 *   env='conda' → ['conda','run','--no-capture-output','-n',env_name]
 *   env='uv'    → ['uv','run','--python',<env_name>] 或 ['uv','run']
 *   env=''      → [executor]（缺省 python）
 */
function buildCmdPrefix(channelConfig) {
  const envType = channelConfig.env || ''
  if (envType === 'conda') {
    if (!channelConfig.env_name) throw new Error("env='conda' 需要配置 'env_name' 字段")
    return ['conda', 'run', '--no-capture-output', '-n', channelConfig.env_name]
  }
  if (envType === 'uv') {
    return channelConfig.env_name ? ['uv', 'run', '--python', path.resolve(channelConfig.env_name)] : ['uv', 'run']
  }
  return [channelConfig.executor || 'python']
}

function which(bin) {
  const r = spawnSync('which', [bin], { encoding: 'utf8' })
  return r.status === 0 ? r.stdout.trim() : ''
}

/** 自动检测 conda 根：CONDA_PREFIX（非 envs 子目录）> CONDA_ROOT > PATH 中 conda 的上两级 > /opt/conda */
function detectCondaRoot() {
  const prefix = process.env.CONDA_PREFIX || ''
  if (prefix && !prefix.split(path.sep).includes('envs')) return prefix
  if (process.env.CONDA_ROOT) return process.env.CONDA_ROOT
  const bin = which('conda')
  if (bin) return path.dirname(path.dirname(bin))
  return '/opt/conda'
}

module.exports = {
  resolveConfigPath, ensureConfigExists, loadChannelConfig, buildCmdPrefix, detectCondaRoot,
  DEFAULT_CONFIG_DIR, DEFAULT_CONFIG_NAME,
}
