const { LoggingBunyan } = require('@google-cloud/logging-bunyan')

const GoogleLogLevel = process.env.GOOGLE_LOG_LEVEL || process.env.LOG_LEVEL || 'info'
const GoogleLogName = process.env.GOOGLE_LOG_NAME || 'master'

module.exports = (logger) => {
  const googleLog = new LoggingBunyan({
    logName: GoogleLogName,
    projectId: 'big-waffle',
    serviceContext: {
      service: `big-waffle-${GoogleLogName}`,
      version: 1
    }
  })

  logger.addStream(googleLog.stream(GoogleLogLevel))
}
