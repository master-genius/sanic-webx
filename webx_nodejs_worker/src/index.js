'use strict'

/** 包入口：导出可编程使用的主要部件（命令行入口见 bin/webx-worker.js） */
module.exports = {
  config: require('./config'),
  fencing: require('./fencing'),
  plugin: require('./plugin'),
  channelLoader: require('./channelLoader'),
  FakeTaskServer: require('./fakeServer').FakeTaskServer,
}
