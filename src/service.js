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

const { Dataset, DataSource, RecordPrinter } = require('./datasets')
const { HTTPPort } = require('./env')

toobusy.maxLag(70)
toobusy.interval(250)
toobusy.onLag(currentLag => {
  console.log(`Event loop lag ${currentLag}ms`)
})

function cleanUp (koaContext, aStream) {
  /*
   * Helper function to clean up a streaming response in unusual situations
   */
  if (koaContext.body && typeof koaContext.body.unpipe === 'function') {
    koaContext.body.unpipe(koaContext.res)
  }
  if (aStream && aStream.emit) {
    aStream.emit('end')
  }
}

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

  api.get('/:dataset([a-z]+)', async (ctx, next) => {
    const start = Moment()
    const key = ctx.query.key
    const values = ctx.query.values.split(',').map(v => v.trim())
    // TODO: parse and validate all of the params
    let recordStream

    // make sure that clients that are not very patient don't cause problems
    for (const ev of ['aborted', 'close']) {
      ctx.req.on(ev, () => {
        cleanUp(ctx, recordStream)
        recordStream = undefined
      })
    }

    try {
      const dataset = new DataSource(ctx.params.dataset)
      await dataset.open()
      recordStream = await dataset.queryStream(key, values, start)
      if (ctx.headerSent || ctx.req.aborted) {
        setImmediate(() => recordStream.destroy(new Error('Acquired DB connection too late'))) // releases the db connection!
      } else {
        const printer = new RecordPrinter(values)
        ctx.res.on('finish', () => {
          console.log(`Responded with ${printer.recordCounter} records`)
          console.log(`Processed request in ${Moment().diff(start, 'milliseconds')}ms`)
        })
        // in order to better handle errors while streaming take direct control of the HTTP response
        ctx.respond = false
        ctx.res.setHeader('Content-Type', 'application/json')
        pipeline(recordStream, printer, ctx.res, (err) => {
          if (err) {
            console.error(err)
          }
        })
      }
    } catch (err) {
      cleanUp(ctx, recordStream)
      ctx.respond = true
      if (err.code === 'ER_GET_CONNECTION_TIMEOUT') {
        console.log('DDF query request timed out')
        ctx.throw(503, `Sorry, the DDF Service seems too busy, try again later`)
      } else {
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
