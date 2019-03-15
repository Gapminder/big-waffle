const fetch = require('node-fetch')

const env = require('./env')
const Log = require('./log')('notifications')

module.exports.Slack = async function (message) {
  if (env.SlackChannelUrl) {
    Log.debug(`Sending ${message} to Slack`)
    await fetch(env.SlackChannelUrl, {
      method: 'post',
      body: JSON.stringify({
        text: message
      }),
      headers: { 'Content-Type': 'application/json' }
    })
  }
}
