'use strict'

/**
 * 精简日志：与 Python logging 的用法对应（getLogger(name).info(...)）。
 * 默认输出到 stderr；可挂文件 sink（见 logConfig.js）。
 */

const LEVELS = { DEBUG: 10, INFO: 20, WARNING: 30, ERROR: 40 }

let threshold = LEVELS.INFO
const sinks = []

function setLevel(name) {
  const v = LEVELS[String(name).toUpperCase()]
  if (v) threshold = v
}

function addSink(fn) {
  sinks.push(fn)
  return () => { const i = sinks.indexOf(fn); if (i >= 0) sinks.splice(i, 1) }
}

function pad(n) { return String(n).padStart(2, '0') }

function stamp(d = new Date()) {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

function emit(level, name, msg, err) {
  if (LEVELS[level] < threshold) return
  let line = `${stamp()} ${level.padEnd(7)} ${name}: ${msg}`
  if (err) line += `\n${err && err.stack ? err.stack : err}`
  if (sinks.length === 0) process.stderr.write(line + '\n')
  for (const s of sinks) {
    try { s(line) } catch (_) { /* sink 故障不能拖垮业务 */ }
  }
}

function getLogger(name) {
  return {
    debug: (m) => emit('DEBUG', name, m),
    info: (m) => emit('INFO', name, m),
    warning: (m) => emit('WARNING', name, m),
    error: (m, err) => emit('ERROR', name, m, err),
    exception: (m, err) => emit('ERROR', name, m, err),
  }
}

module.exports = { getLogger, setLevel, addSink, LEVELS, stamp }
