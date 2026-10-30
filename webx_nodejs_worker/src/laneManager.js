'use strict'

/**
 * LaneManager —— Lane 划分与 task_type 映射（即 worker 的能力声明）。
 *
 * Lane 是槽位不是队列：一条 Lane 同时最多一个任务。并发的扩展轴是进程，不是同类型多槽。
 * 空 LANES = 所有已声明的 channel 归入【一条】Lane（单 Lane 串行），而不是「没有任何类型可领」。
 */

const { getLogger } = require('./log')

const logger = getLogger('laneManager')

class LaneManager {
  /**
   * @param {string[][]} lanes        Lane 分组，如 [['md','ff'],['image']]；空 = 单 Lane
   * @param {Iterable<string>} channelTypes  CHANNELS 的键，用于空 LANES 的兜底与一致性告警
   */
  constructor(lanes, channelTypes = []) {
    const declared = [...new Set(channelTypes)]
    if ((!lanes || lanes.length === 0) && declared.length) lanes = [declared]
    lanes = lanes || []

    this.numLanes = Math.max(lanes.length, 1)
    this.taskToLane = new Map()

    const undeclared = []
    lanes.forEach((laneTasks, i) => {
      for (const t of laneTasks) {
        if (declared.length && !declared.includes(t)) { undeclared.push(t); continue }
        this.taskToLane.set(t, i)
      }
    })
    if (undeclared.length) {
      logger.error(`LANES 中的 ${JSON.stringify(undeclared)} 不在 CHANNELS 里（拼写错误？），已从能力声明中排除，不会领取这些任务`)
    }
    const uncovered = declared.filter((t) => !this.taskToLane.has(t))
    if (uncovered.length) {
      logger.warning(`[LaneManager] 以下 channel 已声明但不在任何 Lane 里，将不会领取这些类型的任务: ${JSON.stringify(uncovered)}`)
    }
    logger.info(`[LaneManager] 初始化: ${this.numLanes} 个 Lane（每 Lane 一个槽位）, 映射=${JSON.stringify([...this.taskToLane])}`)
  }

  /** 该任务类型所属的 Lane 编号；未配置的类型归 Lane 0 */
  getLane(taskType) {
    return this.taskToLane.has(taskType) ? this.taskToLane.get(taskType) : 0
  }

  /** 该 Lane 能跑的全部任务类型 */
  typesOf(laneId) {
    return [...this.taskToLane].filter(([, id]) => id === laneId).map(([t]) => t)
  }
}

module.exports = { LaneManager }
