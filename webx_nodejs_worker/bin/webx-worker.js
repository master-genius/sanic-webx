#!/usr/bin/env node
'use strict'

/**
 * webx_nodejs_worker 入口 —— 子命令分发。
 *
 *   node bin/webx-worker.js [run] [--home DIR] [--config NAME] ...   运行 worker（缺省子命令）
 *   node bin/webx-worker.js cleanup [--status ... --timeout ...]      清理过期任务目录
 *   node bin/webx-worker.js uploader                                  独立上传进程（由 worker 拉起）
 *   node bin/webx-worker.js service one|multi [...]                   生成 systemd 单元
 *   node bin/webx-worker.js doctor [--home DIR] [--config NAME]       对真实服务做部署自检
 *   node bin/webx-worker.js selftest                                  本机假服务端跑一遍完整生命周期
 *
 * 不带子命令（或第一个参数以 - 开头）时按 run 处理，便于部署包装脚本直接透传 flag。
 */

const SUBCOMMANDS = ['run', 'cleanup', 'uploader', 'service', 'doctor', 'selftest']

async function main(argv) {
  let cmd = 'run'
  if (argv.length && !argv[0].startsWith('-')) cmd = argv.shift()

  switch (cmd) {
    case 'run': return require('../src/app').main(argv)
    case 'cleanup': process.exitCode = (await require('../src/cleanup').main(argv)) || 0; return undefined
    case 'uploader': return require('../src/uploader').main(argv)
    case 'service': return require('../src/service').main(argv)
    case 'doctor': process.exitCode = await require('../src/doctor').main(argv); return undefined
    case 'selftest': process.exitCode = await require('../src/selftest').main(argv); return undefined
    default:
      console.error(`未知子命令: ${JSON.stringify(cmd)}（可用: ${SUBCOMMANDS.join(', ')}）`)
      process.exitCode = 2
      return undefined
  }
}

main(process.argv.slice(2)).catch((e) => {
  console.error(e && e.stack ? e.stack : e)
  process.exit(1)
})
