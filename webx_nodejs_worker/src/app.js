'use strict'

/**
 * Worker 主程序 —— 多 Lane 并发版（`node bin/webx-worker.js [run]`）
 *
 * 单进程运行，Lane 间并发、Lane 内串行；worker 主动向领取服务领取任务（有空槽才领，天然背压）。
 *
 * 启动顺序：
 *   1. LaneManager（空 LANES → 单 Lane）
 *   2. PendingReporter（失败上报的持久重放队列）+ IPC server（接子进程的失败事件）
 *   3. 启动恢复：灌回上次 snapshot，再扫工作目录的 ok.json 与服务端协商（Recovery）
 *   4. 拉起 uploader 子进程，启动领取分发循环 + 后台清理
 *   5. 收到 SIGTERM / SIGINT：begin_drain（同步停领）→ 同步捕获令牌 → 同步取消在跑任务 → 用捕获的令牌归还租约，
 *      然后停 uploader、落 PendingReporter 快照。第二次信号 = 强退。
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { parseArgs } = require('node:util')

const cfg = require('./config')
const { settings } = cfg
const { getLogger } = require('./log')
const { resolveConfigPath, ensureConfigExists, loadChannelConfig } = require('./channelLoader')
const { LaneManager } = require('./laneManager')
const { TaskKindPlugin } = require('./plugin')
const { callbackClient } = require('./callback')
const { httpFileClient } = require('./httpFileClient')
const { HeartbeatManager } = require('./heartbeat')
const { ConcurrentLogWriter, buildRedactRoots } = require('./logWriter')
const { setupLogging } = require('./logConfig')
const { PendingReporter, setGlobalReporter } = require('./pendingReporter')
const { IPCServer } = require('./ipcServer')
const { selfcheckAndReport } = require('./linkHealth')
const { WorkerRecovery } = require('./recovery')
const { UploaderSupervisor } = require('./uploaderSupervisor')
const { runWorker } = require('./broker')
const lease = require('./lease')
const { GenericTaskExecutor } = require('./executor')
const { Event } = require('./aio')

const logger = getLogger('app')

function buildExecutor(channels, condaRoot, ipcSockPath = null, plugin = null, scriptRoot = null) {
  plugin = plugin || new TaskKindPlugin()
  const logWriter = new ConcurrentLogWriter({
    headLines: settings.LOG_HEAD_LINES,
    tailLines: settings.LOG_TAIL_LINES,
    // 前端可见日志的路径脱敏根：WORK_DIR / conda_root / channels 此刻均已定版
    redactRoots: buildRedactRoots({ workDir: settings.WORK_DIR, condaRoot, channels, scriptRoot }),
    redactors: plugin.redactors, // 存引用：插件之后追加的函数同样生效
  })
  const hb = new HeartbeatManager(settings.HEARTBEAT_INTERVAL)
  return new GenericTaskExecutor(callbackClient, logWriter, hb, httpFileClient, settings.WORK_DIR,
    channels, condaRoot, ipcSockPath, plugin, scriptRoot)
}

/** channel -> handler 路由表，所有 channel 共享同一 executor；handler 签名 (taskId, payload, signal) */
function buildHandlers(executor, channels) {
  const handlers = {}
  for (const name of Object.keys(channels)) {
    handlers[name] = (taskId, payload, signal) => executor.execute(taskId, payload, signal)
    console.log(`[+] 注册 channel: ${name}`)
  }
  return handlers
}

const ARG_OPTIONS = {
  home: { type: 'string' },
  config: { type: 'string' },
  'ext-env': { type: 'string' },
  'work-dir': { type: 'string' },
  debug: { type: 'boolean', default: false },
  log: { type: 'string' },
}

/** 容忍未知参数（便于部署包装脚本统一透传）；未知项收集到 unknown，日志就绪后再告警 */
function parseCliArgs(argv) {
  const { values, tokens } = parseArgs({ args: argv, options: ARG_OPTIONS, strict: false, tokens: true })
  const unknown = tokens.filter((t) => t.kind === 'option' && !(t.name in ARG_OPTIONS)).map((t) => t.rawName)
  const str = (v) => (typeof v === 'string' ? v : undefined)
  return {
    home: str(values.home), config: str(values.config), extEnv: str(values['ext-env']),
    workDir: str(values['work-dir']), log: str(values.log), debug: values.debug === true, unknown,
  }
}

const expandHome = (p) => path.resolve(p.startsWith('~') ? path.join(os.homedir(), p.slice(1)) : p)

async function asyncMain(channels, lanes, condaRoot, plugin, scriptRoot) {
  // 兜底：未处理异常记入日志（后台服务排障刚需），不静默吞
  process.on('unhandledRejection', (e) => logger.error(`[Main] 未处理的 Promise 异常: ${e && e.stack ? e.stack : e}`))

  // 停止信号只置 Event，真正的收尾在主流程里做；第二次信号 = 强退（收尾卡住时仍可 Ctrl-C）
  const stopping = new Event()
  const onSignal = (signame) => {
    if (stopping.isSet()) {
      logger.warning(`[Main] 再次收到 ${signame}，放弃收尾直接退出`)
      process.exit(1)
    }
    logger.info(`[Main] 收到 ${signame}，开始收尾`)
    stopping.set()
  }
  process.on('SIGTERM', () => onSignal('SIGTERM'))
  process.on('SIGINT', () => onSignal('SIGINT'))

  const laneManager = new LaneManager(lanes, Object.keys(channels))

  const workBase = path.resolve(settings.WORK_DIR)
  fs.mkdirSync(workBase, { recursive: true })
  const pendingReporter = new PendingReporter(callbackClient, settings.WORK_DIR)
  setGlobalReporter(pendingReporter)

  const ipcSockPath = path.join(workBase, 'ipc.sock')
  const ipcServer = new IPCServer(ipcSockPath, pendingReporter)
  await ipcServer.start()

  const restored = await pendingReporter.restoreFromSnapshot()
  if (restored) logger.info(`[Main] PendingReporter 启动恢复 ${restored} 条历史事件`)

  // 内部回调链路自检放后台：观测不占用启动路径，失败也不阻止启动
  const selfcheck = selfcheckAndReport(callbackClient).catch(() => {})

  const executor = buildExecutor(channels, condaRoot, ipcSockPath, plugin, scriptRoot)

  const recovery = new WorkerRecovery(callbackClient, settings.WORK_DIR, plugin)
  await recovery.recover()

  // uploader 放在 recover 之后：恢复扫描可能重排产物，先理清队列现场再开传
  const uploaderSup = new UploaderSupervisor()
  uploaderSup.start()

  const handlers = buildHandlers(executor, channels)
  const reporterTask = pendingReporter.runLoop()

  try {
    const workerTask = runWorker(handlers, laneManager, executor)
    const crashed = workerTask.then(() => 'worker-exit', (e) => { throw e })
    const winner = await Promise.race([crashed, stopping.wait().then(() => 'stop')])
    if (winner === 'stop') {
      logger.info('[Main] 收到停止信号，开始收尾（不等待在跑的任务）')
      // 顺序本身就是判据：⓪ 同步停领 → ① 同步捕获令牌 → ② 同步取消在跑任务（走中断分支，不上报 fail）→ ③ 并发归还
      lease.beginDrain()
      const tokens = lease.heldFencingHeaders()
      const cancelled = lease.cancelRunningTasks()
      if (cancelled) logger.info(`[Main] 已取消 ${cancelled} 个在跑任务（走中断分支，不上报失败）`)
      await lease.releaseHeldLeases(tokens)
      if (typeof workerTask.stop === 'function') workerTask.stop()
      await Promise.race([workerTask.catch(() => {}), new Promise((r) => setTimeout(r, 3000).unref())])
    }
  } finally {
    // uploader 先停：给它宽限期把在途分片落盘，传不完的下次接着传
    await uploaderSup.stop()
    await Promise.race([selfcheck, new Promise((r) => setTimeout(r, 200).unref())])
    await executor.heartbeat.shutdownAll()
    pendingReporter.requestStop()
    await Promise.race([reporterTask.catch(() => {}), new Promise((r) => setTimeout(r, 2000).unref())])
    await ipcServer.close()
    try {
      pendingReporter.snapshotToFile()
    } catch (e) {
      logger.error(`[Main] PendingReporter snapshot 失败: ${e.message}`, e)
    }
  }
}

async function main(argv) {
  const args = parseCliArgs(argv)

  // 主目录：.env 与 channel_config/ 的锚点
  if (args.home) process.env.WEBX_WORKER_HOME = expandHome(args.home)
  const home = cfg.workerHome()

  // 基础层 .env，再叠实例覆盖层。二者会整体刷新单例，必须先于下方所有命令行覆盖
  const baseLoaded = cfg.loadBaseEnv(path.join(home, '.env'))
  const configPath = resolveConfigPath(args.config, home)
  let overlayLoaded
  if (args.extEnv) {
    overlayLoaded = cfg.applyEnvOverlay(args.extEnv)
    if (overlayLoaded === null) {
      console.error(`[Worker] 致命错误：--ext-env 指定的文件不存在: ${args.extEnv}`)
      process.exit(1)
    }
  } else {
    overlayLoaded = cfg.applyEnvOverlay(cfg.defaultOverlayFor(configPath))
  }

  // worker_id 必须在覆盖层之后派生，依据是配置文件裸名
  cfg.deriveWorkerId(path.parse(configPath).name)
  if (args.debug) settings.DEBUG = true

  const effectiveLogDir = setupLogging({
    logDir: args.log || settings.LOG_DIR || null,
    debug: settings.DEBUG,
    maxBytes: settings.LOG_MAX_BYTES,
    maxLines: settings.LOG_MAX_LINES,
  })
  if (args.unknown.length) logger.warning(`[Worker] 忽略不支持的参数: ${args.unknown.join(' ')}`)
  if (args.log) settings.LOG_DIR = args.log // 回写，uploader 子进程要继承（Python 版 K19）

  if (args.workDir) settings.WORK_DIR = expandHome(args.workDir)

  if (!ensureConfigExists(configPath)) process.exit(0)

  const conf = loadChannelConfig(configPath)
  const { CHANNELS: channels, LANES: lanes, CONDA_ROOT: condaRoot, PLUGIN: plugin, SCRIPT_ROOT: scriptRoot } = conf
  const bar = '='.repeat(60)
  console.log(`\n${bar}\n  webx_nodejs_worker — 多 Lane 并发版\n${bar}`)
  console.log(`[+] 主目录:    ${home}`)
  console.log(`[+] 基础配置:  ${baseLoaded || '(无 .env)'}`)
  console.log(`[+] 配置文件:  ${configPath}`)
  console.log(`[+] 环境覆盖层: ${overlayLoaded || '(无)'}`)
  console.log(`[+] Channel:  ${JSON.stringify(Object.keys(channels))}`)
  console.log(`[+] Lane 分组: ${lanes.length ? JSON.stringify(lanes) : '单 Lane 模式（所有 channel 归入一条 Lane）'}`)
  console.log(`[+] 工作目录: ${settings.WORK_DIR}`)
  console.log(`[+] Worker ID:  ${settings.WORKER_ID}`)
  console.log(`[+] 领取通道:  ${settings.TASK_SERVER_URL || settings.WEB_BASE_URL}（主动领任务）`)
  console.log(`[+] 回调通道:  ${settings.WEB_BASE_URL}（报状态，启动时自检）`)
  console.log('[+] 领取模型:  Lane=槽位，跑完才领下一个（无预取、无本地队列）')
  console.log(`[+] DEBUG 模式: ${settings.DEBUG ? '开启' : '关闭'}`)
  console.log(`[+] 日志输出:  ${effectiveLogDir || '仅控制台'}`)
  console.log(`${bar}\n`)

  await asyncMain(channels, lanes, condaRoot, plugin, scriptRoot)
}

module.exports = { main, asyncMain, buildExecutor, buildHandlers, parseCliArgs }
