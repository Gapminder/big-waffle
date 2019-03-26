const { Writable } = require('stream')

const Bunyan = require('bunyan')

const env = require('./env')

const mainLog = Bunyan.createLogger({
  name: 'BigWaffle',
  level: env.LogLevel,
  serializers: Bunyan.stdSerializers
})

// TODO: add stream to log to Google Cloud Logging in case this is a production server

class LogCollector extends Writable {
  /*
   * A LogCollector is a simple in memory stream that accepts log records
   * from a Bunyan logger.
   */
  constructor (options) {
    super({
      emitClose: false,
      objectMode: true,
      write: (record, encoding, callback) => {
        try {
          const level = Bunyan.nameFromLevel[record.level]
          if (!this.records[level]) this.records[level] = []
          const entry = {}
          if (options.include) {
            options.include.forEach(prop => {
              const value = record[prop]
              if (value) entry[prop] = value
            })
          } else if (options.exclude) {
            Object.keys(record).forEach(prop => {
              if (!options.exclude.includes(prop)) {
                const value = record[prop]
                if (value) entry[prop] = value
              }
            })
          } else {
            Object.assign(entry, record)
          }
          this.records[level].push(entry)
          callback(null)
        } catch (err) {
          callback(err)
        }
      }
    })
    this.records = {}
  }
}

function makeLogging (obj, logger = mainLog, options = { include: ['msg', 'time'] }) {
  /*
   * Turn the given obj into a simplified Logger that has
   * 'info(...)', 'warn(...)', etc. methods.
   * The obj will also get a 'log' property that will return an
   * object with keys for the levels ('info', 'warn', etc.)
   * with arrays of log entries.
   */
  const logCollector = new LogCollector(options)
  const objLog = logger.child({
    streams: [
      {
        type: 'raw',
        reemitErrorEvents: false,
        stream: logCollector
      }
    ]
  })
  Object.defineProperty(obj, 'log', {
    get () { return logCollector.records }
  })
  Object.keys(Bunyan.levelFromName).forEach(level => {
    Object.defineProperty(obj, level, {
      value: (record) => objLog[level](record)
    })
  })
  return obj
}

Object.defineProperty(mainLog, 'wrap', { value: (obj, options) => makeLogging(obj, mainLog, options) })

module.exports = (moduleName) => {
  if (moduleName) {
    const log = mainLog.child({ module: moduleName })
    Object.defineProperty(log, 'wrap', { value: (obj, options) => makeLogging(obj, log, options) })
    return log
  } else {
    return mainLog
  }
}
