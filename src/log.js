const Bunyan = require('bunyan')

const env = require('./env')

const mainLog = Bunyan.createLogger({
  name: 'BigWaffle',
  level: env.LogLevel,
  serializers: Bunyan.stdSerializers
})

// TODO: add stream to log to Google Cloud Logging in case this is a production server

module.exports = (moduleName) => {
  if (moduleName) {
    return mainLog.child({ module: moduleName })
  } else {
    return mainLog
  }
}
