/**
 * Koa (HTTP) service to handle DDF requests.
 *
 */
const Compress = require('koa-compress')
const Koa = require('koa')
const Router = require('koa-router')
const Moment = require('moment')

const { Dataset, DataSource, RecordPrinter } = require('./datasets')
const { HTTPPort } = require('./env')

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

  api.get('/:dataset', async (ctx, next) => {
    const start = Moment()
    const key = ctx.query.key
    const values = ctx.query.values.split(',').map(v => v.trim())
    // TODO: parse and validate all of the params
    let recordStream

    // make sure that clients that are not very patient don't cause problems
    for (const ev of ['aborted', 'close']) {
      ctx.req.on(ev, () => {
        if (typeof ctx.body.unpipe === 'function') {
          ctx.body.unpipe(ctx.res)
        }
        if (recordStream && recordStream.emit) {
          recordStream.emit('end')
          recordStream = undefined
        }
      })
    }

    // if the DB is too busy notify clients
    let timedOut = false
    const timeout = setTimeout(ctx => {
      if (!ctx.headerSent) {
        if (recordStream && recordStream.emit) {
          recordStream.emit('end')
        }
        ctx.status = 503
        ctx.type = 'text/plain'
        ctx.body = `Sorry, the DDF service is too busy, try again later.`
        timedOut = true
      }
    }, 5000, ctx) // this timeout should be shorter than the DB pool connection timeout, which is 10 sec.

    try {
      const dataset = new DataSource(ctx.params.dataset)
      await dataset.open()
      recordStream = await dataset.queryStream(key, values, start)
      clearTimeout(timeout)
      if (ctx.headerSent) {
        if (timedOut) {
          console.log('DDF query request timed out')
        }
        return
      }
      const printer = RecordPrinter(values)
      ctx.type = 'application/json'
      if (ctx.req.aborted) {
        recordStream.emit('end') // releases the db connection!
      } else {
        ctx.res.on('finish', () => {
          console.log(`Responded with ${printer.recordCounter} records`)
          console.log(`Processed request in ${Moment().diff(start, 'milliseconds')}ms`)
        })
        ctx.body = recordStream.pipe(printer)
      }
    } catch (err) {
      console.error(err)
      if (recordStream && recordStream.emit) {
        recordStream.emit('end')
      }
      ctx.throw(503, `Sorry, the DDF Service seems too busy, try again later`)
    }
  })

  app.use(Compress())
  app.use(api.routes())
  app.listen(HTTPPort)
}
