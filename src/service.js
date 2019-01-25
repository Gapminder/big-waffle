/**
 * Koa (HTTP) service to handle DDF requests.
 *
 */
const { pipeline } = require('stream')
const Compress = require('koa-compress')
const Koa = require('koa')
const Router = require('koa-router')
const Moment = require('moment')
const toobusy = require('toobusy-js')

const { DB } = require('./maria')
const { Dataset, DataSource, RecordPrinter } = require('./datasets')
const { HTTPPort } = require('./env')

toobusy.maxLag(70)
toobusy.interval(250)
toobusy.onLag(currentLag => {
  console.log(`Event loop lag ${currentLag}ms`)
})

module.exports.DDFService = function () {
  const app = new Koa()
  const api = new Router() // routes for the main API

  api.get('/loaderio-3ca287be03c603428fe74ca6695dde27.txt', async (ctx, next) => {
    /*
     * Route to serve verification by the Loader TaaS
     */
    ctx.type = 'text/plain'
    ctx.body = 'loaderio-3ca287be03c603428fe74ca6695dde27'
  })

  api.get('/', async (ctx, next) => {
    /*
     * List all (public) datasets that are currently available.
     */
    const datasets = await Dataset.all()
    ctx.body = datasets.map(ds => ds.name)
  })

  api.get('/:dataset([_a-z]+)', async (ctx, next) => {
    console.log('Received DDF query')
    const start = Moment()
    const key = ctx.query.key
    const values = ctx.query.values.split(',').map(v => v.trim())
    // TODO: parse and validate all of the params
    let recordStream

    // make sure that clients that are not very patient don't cause problems
    for (const ev of ['aborted', 'close']) {
      ctx.req.on(ev, () => {
        if (recordStream && recordStream.destroy) {
          console.log(`Request unexpectedly closed. Start clean up...`)
          recordStream.destroy(new Error('HTTP Request unexpectedly closed'))
          recordStream = undefined
        }
      })
    }

    try {
      console.log(`DB has ${DB.idleConnections()} idle connections and ${DB.taskQueueSize()} pending connection requests`)
      const dataset = new DataSource(ctx.params.dataset)
      await dataset.open()
      if (ctx.headerSent || ctx.req.aborted) {
        return
      }

      // grab a connection and release it first after a while, to test...
      // const conn1 = await DB.getConnection()
      // setTimeout((c) => c.end(), 6000, conn1)
      // console.log(`DB has ${DB.idleConnections()} idle connections and ${DB.taskQueueSize()} pending connection requests`)
      // const conn2 = await DB.getConnection()
      // setTimeout((c) => c.end(), 4000, conn2)
      // console.log(`DB has ${DB.idleConnections()} idle connections and ${DB.taskQueueSize()} pending connection requests`)
      // const conn3 = await DB.getConnection()
      // setTimeout((c) => c.end(), 2000, conn3)
      // console.log(`DB has ${DB.idleConnections()} idle connections and ${DB.taskQueueSize()} pending connection requests`)

      recordStream = await dataset.queryStream(key, values, () => ctx.headerSent || ctx.req.aborted, start)
      if (!(ctx.headerSent || ctx.req.aborted)) {
        const printer = new RecordPrinter(values)
        // in order to better handle errors while streaming take direct control of the HTTP response
        ctx.res.on('finish', () => {
          process.nextTick(() => {
            console.log(`Responded with ${printer.recordCounter} records`)
            console.log(`Processed request in ${Moment().diff(start, 'milliseconds')}ms`)
          })
        })
        ctx.respond = false
        ctx.res.statusCode = 200
        ctx.res.setHeader('Content-Type', 'application/json')
        pipeline(recordStream, printer, ctx.res, (err) => {
          if (err) {
            console.error(`piping error:`)
            console.error(err)
          }
        })
      }
    } catch (err) {
      if (recordStream && recordStream.destroy) recordStream.destroy(err)
      ctx.respond = true
      if (err.code === 'ER_GET_CONNECTION_TIMEOUT') {
        console.log('DDF query request timed out')
        ctx.throw(503, `Sorry, the DDF Service seems too busy, try again later`)
      } else {
        console.error('Unexpected error!')
        console.error(err)
      }
      ctx.throw(500, `Sorry, the DDF Service seems to have a problem, try again later`)
    }
  })

  app.use(async (ctx, next) => {
    /*
     * Simple check to prevent from this worker to be flooded with requests.
     * This as DDF queries usually take significant amounts of time to process
     */
    if (toobusy()) {
      console.log(`Too busy!`)
      ctx.throw(503, `Sorry server is too busy right now`)
    }
    await next()
  })
  app.use(Compress())
  app.use(api.routes())
  app.listen(HTTPPort)
}
