const { Writable } = require('stream')

const Bunyan = require('bunyan')
const fetch = require('node-fetch')

const env = require('./env')
const Log = require('./log')() // the main log 

// Add a "notify" level , that's in between info and warning
const NOTIFY = (Bunyan.INFO + Bunyan.WARN) / 2
Bunyan.levelFromName.notify = NOTIFY
Bunyan.nameFromLevel[NOTIFY] = 'notify'
// A somewhat hacky way to add a 'notify' emit function to the logger. See https://github.com/trentm/node-bunyan/pull/465#issuecomment-363409059
Object.getPrototypeOf(Log).notify = function (msg, ...args) {
  if (typeof msg === 'string') {
    this.fatal({ level: NOTIFY }, msg, ...args)
  } else if (typeof msg === 'object') {
    this.fatal({ level: NOTIFY, ...msg }, ...args)
  } else {
    throw new Error('Invalid arguments provided')
  }
}

class SlackLogStream extends Writable {
  /*
   * A SlackLogStream is a simple stream that accepts log records
   * from a Bunyan logger and sends them to a Slack channel
   * that is configued in the environment
   */
  constructor (channelUrl) {
    super({
      emitClose: false,
      objectMode: true,
      write: (record, encoding, callback) => {
        fetch(channelUrl, {
          method: 'post',
          body: JSON.stringify({ text: record.msg }),
          headers: { 'Content-Type': 'application/json' }
        })
          .then(result => callback())
          .catch(err => {
            console.error(err)
            callback(err)
          })
      }
    })
  }
}

module.exports.logToSlack = function (level = NOTIFY) {
  if (env.SlackChannelUrl) {
    Log.addStream({
      name: 'slack',
      level,
      type: 'raw',
      reemitErrorEvents: false,
      stream: new SlackLogStream(env.SlackChannelUrl)
    })
  }
}
