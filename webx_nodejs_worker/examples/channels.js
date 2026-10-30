'use strict'
// webx_nodejs_worker 示例 channel 配置 —— 一个最小的、不含任何业务的 worker。
//
// 用法：
//   cp examples/channels.js   <home>/channel_config/default.js
//   cp examples/demo_task.js  <home>/channel_config/demo_task.js   // 相对路径的脚本按本文件所在目录解析
// 或直接 --config 指向本文件（脚本与本文件同目录，无需拷贝）。
//
// CHANNELS 的键就是 task_type —— 同时是本 worker 的「能力声明」。
module.exports = {
  CHANNELS: {
    demo: {
      script: 'demo_task.js', // .js 固定用当前 node 执行
      // run_env：注入子进程的环境变量 —— GPU/线程绑定这类「机器属性」放这里，不要写进任务参数
      run_env: { DEMO_GREETING: 'hello from run_env' },
    },
  },
  LANES: [], // 空 = 所有 channel 归入一条 Lane
}
