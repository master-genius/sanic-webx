# webx_nodejs_worker —— worker 的 Node.js 实现

`webx_nodejs_worker/` 是 [`webx_worker`](worker.md)（Python）的 Node.js 版本：**同一份协议、同一套环境变量、同一种目录布局**，
所以两个实现可以互换，也可以混用——Python 任务脚本继续用 Python SDK，Node 任务脚本用本包的 SDK，二者读同样的
`WEBX_WORKER_*` 环境变量。与服务端之间只通过 HTTP 通信，协议见 [`worker-protocol.md`](worker-protocol.md)（唯一权威）。

行为（租约 + fencing、心跳与自熔、`ok.json` 先写后通知、启动恢复、优雅停机、uploader 两阶段上传……）以 [`worker.md`](worker.md)
为准，本文只写 **Node 版特有的东西**：怎么跑、依赖什么、和 Python 版哪里不同。

## 1. 依赖与运行

Node ≥ 20（开发于 v24）。依赖都在 `webx_nodejs_worker/package.json`：

| 包 | 用在哪 |
|---|---|
| `gohttp` | 所有 HTTP 客户端流量（经 `src/httpClient.js` 统一出口）：领取、回调、输入下载、存储上传 |
| `cdpc` | 进程管理（`src/procManager.js` 单例）：拉起任务子进程、看住 uploader 子进程（崩溃重启） |
| `topbit` / `hydra-thread` | 已装但**未使用**（假服务端用 `node:http` 实现，无需 Web 框架） |

```bash
cd webx_nodejs_worker
node bin/webx-worker.js selftest          # 无外部依赖：本机假服务端跑完整生命周期，14/14 通过即环境就绪

mkdir -p myworker/channel_config && cd myworker
cp <repo>/webx_nodejs_worker/examples/channels.js   channel_config/default.js
cp <repo>/webx_nodejs_worker/examples/demo_task.js  channel_config/demo_task.js
cat > .env <<'EOF2'
WEBX_WORKER_WEB_BASE_URL=https://tasks.example.com
WEBX_WORKER_INTERNAL_TOKEN=<与服务端 INTERNAL_TOKEN 一致>
WEBX_WORKER_STORAGE_INTERNAL_URL=https://storage.example.com
WEBX_WORKER_WORK_DIR=/data/worker-tasks
EOF2
node <repo>/webx_nodejs_worker/bin/webx-worker.js doctor     # 对真实服务自检（不会领取任务）
node <repo>/webx_nodejs_worker/bin/webx-worker.js            # 运行（等价于 run）
```

子命令：`run`（缺省）｜`cleanup`｜`uploader`（由 worker 拉起，勿手动）｜`service`｜`doctor`｜`selftest`。
命令行：`--home` `--config` `--ext-env` `--work-dir` `--debug` `--log`；未知参数被忽略并告警。
配置项、优先级（真实环境变量 > 实例覆盖层 `<配置名>_env` > `<主目录>/.env` > 默认）、`.env` 语法、`worker_id` 派生规则
与 `worker.md` §6 完全一致。新增一项：`WEBX_WORKER_KILL_GRACE_SECONDS`（默认 3，终止子进程的 SIGTERM→SIGKILL 宽限）。

## 2. 目录布局

```
webx_nodejs_worker/
  bin/webx-worker.js     子命令分发
  src/
    app.js               主程序（启动顺序、SIGTERM 停机三步）
    config.js envnames.js log.js logConfig.js  配置 / 环境变量名（与 Python 版逐字一致）/ 日志
    channelLoader.js plugin.js laneManager.js  channel 配置（CommonJS）/ TaskKindPlugin / Lane
    lease.js broker.js   领取客户端 + 分发循环 / 领取循环 + 周期清理
    executor.js heartbeat.js fencing.js callback.js  执行器 / 心跳与自熔 / 令牌注册表 / 回调客户端
    pendingReporter.js ipcServer.js recovery.js cleanup.js linkHealth.js  可靠性部件
    uploader.js uploaderSupervisor.js httpFileClient.js logWriter.js
    httpClient.js aio.js procManager.js  gohttp 出口 / asyncio 式原语 / cdpc 单例
    fakeServer.js selftest.js doctor.js service.js
    sdk/                 子进程侧 SDK（任务脚本 require）
  examples/              channels.js + demo_task.js
  test/                  node:test（见 §5）
```

## 3. 任务脚本与 SDK

channel 配置是 **CommonJS 模块**（不是 Python）：`module.exports = { CHANNELS, LANES, SCRIPT_ROOT, CONDA_ROOT, PLUGIN }`，
字段含义同 `worker.md` §5；`--config gpu0` ⇒ `channel_config/gpu0.js` 与 `channel_config/gpu0_env`。
`PLUGIN` 钩子名为 camelCase：`logTarget / selectInputs / extendEnv / softCleanPolicy / redactors`。

脚本按扩展名选解释器：`.js/.cjs/.mjs` → 当前 node，`.sh` → bash，其余走 channel 的 `env`（`conda`/`uv`）或 `executor`（缺省 `python`）。
任务脚本以**任务目录为 cwd**，**一律**追加 `--task-id <ID>`。

Node 任务脚本里：

```js
const sdk = require('webx_nodejs_worker/src/sdk')   // executor 已把 NODE_PATH 设为包目录的父目录（目录名需保持 webx_nodejs_worker）
await sdk.reportStepMessage(taskId, subStep, 'Preparing')
const url = await sdk.deliverOnly(taskId, 'out/result.txt', 'result.txt', ['outputs', 'report_url'])
```

SDK 导出：`uploadResult / updateTaskFields / updateStepStatus / downloadArtifact(Stream) / listArtifacts`、`reportStepMessage`、
`deliverRequired / deliverOnly / deliver`、`sendReport`、`configureTaskLogging`。除 `reportStepMessage` 永不 reject 外，
网络函数均为 `async`；可选参数用 options 对象。重试分层同 `worker.md` §10。最小示例见 `examples/`。

## 4. 与 Python 版的有意差异

| 项 | Node 版 | 理由 |
|---|---|---|
| 终止子进程 | 取消 / 超时 / 自熔 / 停机一律先 **SIGTERM**（同步发出），宽限期后 SIGKILL；退出后再对整个进程组无条件 SIGKILL 清残留 | 让任务脚本有机会收尾；Python 版是立即 SIGKILL |
| 子进程管理 | 经 **cdpc** 拉起（`detached`，pgid == pid，整组可杀）；uploader 由 cdpc `restart:'always'` 看住 | cdpc 用 `notExit:true` 与自有信号处理共存，仅追加监听器 |
| 中断 | `AbortSignal`：`handler(taskId, payload, signal)`，被 abort 抛 `Interrupted`（不写 `ok.json`、不上报 fail） | JS 没有 `Task.cancel()` |
| channel 配置 | CommonJS，不是 Python | — |
| 路径名归一化 | `task_id`、`sub_step`、输入文件名做白名单 / 穿越校验 | 修 K12 / K23 |
| 脚本参数 | `.sh` / `.js` 也追加 `--task-id` | Python 版漏了 |
| 已修的已知限制 | K7（无快照时扫降级文件）、K8（重放不改条目）、K10（SDK 对终态 403 不重试，按 reason 处理）、K17（队列条目 nonce）、K18（409 缺头不再无限重发）、K19（`--log` 回写并传给 uploader）、K20（固定重启间隔，无只增退避）、K22（空领取最小间隔 1s） | 新实现顺手避开 |
| uploader 重启 | cdpc 固定 5s 间隔 | 无只增退避 |

仍与 Python 版相同、**未修**：K1（自熔余量小于心跳粒度）、K3、K4（多实例须各自独占 `WORK_DIR`）、K9（uploader 背压未接线，
`isBackpressured()` 已导出但领取循环不调用）、K13（仅 POSIX）、K14、K16（磁盘增长）、K21。

## 5. 验证

```bash
cd webx_nodejs_worker
npm test                              # 约 30 秒：单元 + 真进程 e2e + 跨实现验证
node bin/webx-worker.js selftest      # 本机完整生命周期（14 项）
node bin/webx-worker.js doctor ...    # 对真实服务自检，不领取任务
```

测试刻意做得轻量（基本覆盖主路径与关键边界，不是 Python 的 6900 行测试的逐条移植）：

- `test/*.test.js`：各模块单元测试，共用 `src/fakeServer.js`（协议参考实现，写端点强制 fencing、旧路径 404、撤销回 200 revoked）；
- `test/e2e.test.js`：真 worker 子进程 + 真信号——取消、租约吊销、超时、SIGTERM 停机归还租约，并检查孙进程被清掉；
- `test/crossimpl.test.js`：Node worker 对 **Python 的 `FakeTaskServer`** 跑完整生命周期（需要 `python3`，否则跳过），
  用来防止两个实现各自「自洽地漂移」；
- 变异验证（改坏 → 变红 → 还原）：去掉 `executor` 的 fencing 令牌注入 ⇒ `selftest` 的「写请求带令牌」「无 fencing 拒绝」两项变红。

## 6. 尚未覆盖 / 已知缺口

- `httpFileClient` 直接用 gohttp 的 `download`（`httpClient.request` 会整体缓冲响应，不适合大输入文件），观测手动喂给 `linkHealth`；
  SDK 的 `downloadArtifactStream` 用 `node:http(s)` 实现流式 + Range 续传。后续可在 `httpClient` 加流式 `download()` 收口。
- `uploader` 进程未加载 `linkHealth`，其存储上传遇到的 401 / TLS 不进「链路健康」告警。
- 没有在真实任务服务（`webx` + `task_server`）上做过联调；协议遵守度靠 `fakeServer`、`selftest` 与对 Python 参考实现的跨验证保证。
