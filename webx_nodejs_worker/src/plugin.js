'use strict'

/**
 * TaskKindPlugin —— 把「与具体任务类型有关的规则」收口成接缝。
 * 默认实现全部中性；需要定制的部署在 channel 配置里导出一个子类实例（PLUGIN）。
 *
 *   logTarget(taskType, subStep, stepsDone) -> [key, filename]
 *   selectInputs(taskType, files) -> files[]
 *   extendEnv(taskType, taskId, env) -> {k: v}      在 fencing 令牌之前注入，令牌最后写入
 *   softCleanPolicy(taskType) -> Set<string> | null  null = rmtree 整目录
 *   redactors: Array<(text) => text>                 前端可见日志脱敏，在内置规则之前依次执行
 */
class TaskKindPlugin {
  constructor() {
    this.redactors = []
  }

  logTarget(taskType, subStep, stepsDone) { // eslint-disable-line no-unused-vars
    if (!subStep) return ['log', 'run.log']
    return [subStep, `${subStep}.log`]
  }

  selectInputs(taskType, files) { // eslint-disable-line no-unused-vars
    return (files || []).filter((f) => f && typeof f === 'object' && f.name)
  }

  extendEnv(taskType, taskId, env) { // eslint-disable-line no-unused-vars
    return {}
  }

  softCleanPolicy(taskType) { // eslint-disable-line no-unused-vars
    return null
  }
}

module.exports = { TaskKindPlugin }
