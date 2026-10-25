# webx_worker 示例 channel 配置 —— 一个最小的、不含任何业务的 worker。
#
# 用法：
#   cp webx_worker/examples/channels.py  <home>/channel_config/default.py
#   cp webx_worker/examples/demo_task.py <home>/channel_config/demo_task.py   # 脚本相对本文件所在目录解析
#   python -m webx_worker doctor --home <home>
#   python -m webx_worker --home <home>
#
# 或者不拷贝，直接指定路径：
#   python -m webx_worker --config webx_worker/examples/channels.py
#
# CHANNELS 的键就是 task_type —— 同时是本 worker 的「能力声明」：每次领取请求都会带上
# 「我这个空槽能跑哪些 task_type」，服务端不存能力注册表。

import sys

CHANNELS = {
    "demo": {
        # 相对路径按本文件所在目录解析（也可写绝对路径，或在本文件里定义 SCRIPT_ROOT）
        "script": "demo_task.py",
        # env: "" = 直接用 executor 指定的解释器；"conda" / "uv" 见 docs/worker.md
        "env": "",
        "executor": sys.executable,
        # run_env：注入子进程的环境变量 —— GPU/线程绑定这类「机器属性」放这里，
        # 不要写进任务参数，也不要写死进代码
        "run_env": {"DEMO_GREETING": "hello from run_env"},
    },
}

# Lane 分组（可选）。不配置 / 空数组 = 所有 channel 归入【一条】Lane（单 Lane 串行）。
# 一条 Lane 是一个槽位，不是队列；并发的扩展轴是【进程】（多起 worker 实例）。
LANES = []

# 业务接缝（可选）：见 webx_worker/plugin.py
# from webx_worker.plugin import TaskKindPlugin
# PLUGIN = TaskKindPlugin()
