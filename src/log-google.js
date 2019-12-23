const { Transform } = require('stream')
const { LoggingBunyan } = require('@google-cloud/logging-bunyan')

const GoogleLogLevel = process.env.GOOGLE_LOG_LEVEL || process.env.LOG_LEVEL || 'info'
const GoogleLogName = process.env.GOOGLE_LOG_NAME || 'master'

const RecordLimit = 200
const ShorterMessageLength = 30

module.exports = (logger) => {
  const logRecordLimiter = new Transform({
    objectMode: true,

    transform (obj, encoding, callback) {
      const str = JSON.stringify(obj)
      if (str.length > RecordLimit) {
        logger.warn('Received a too long log record!')
        const smallerObj = {
          hostname: obj.hostname,
          level: obj.level,
          msg: obj.msg.slice(0, ShorterMessageLength),
          name: obj.name,
          pid: obj.pid,
          time: obj.time,
          v: obj.v
        }
        if (obj.module) {
          smallerObj.module = obj.module
        }
        this.push(smallerObj)
        callback()
      } else {
        this.push(obj)
        callback()
      }
    }
  })
  const googleLog = new LoggingBunyan({
    logName: GoogleLogName,
    projectId: 'big-waffle',
    serviceContext: {
      service: `big-waffle-${GoogleLogName}`,
      version: 1
    }
  })
  logRecordLimiter.pipe(googleLog.stream(GoogleLogLevel).stream, { end: false })
  logger.addStream({ level: GoogleLogLevel, type: 'raw', stream: logRecordLimiter })
}
