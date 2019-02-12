/**
 * Koa (HTTP) service to handle DDF requests.
 *
 */
const zlib = require('zlib')
const Compress = require('koa-compress')
const Koa = require('koa')
const Router = require('koa-router')
const Moment = require('moment')
const toobusy = require('toobusy-js')

const { DB } = require('./maria')
const { Dataset, Query, RecordPrinter } = require('./ddf')
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

  api.get('/:dataset([_a-z]+)/:version([0-9]+)?', async (ctx, next) => {
    console.log('Received DDF query')
    const start = Moment()
    const datasetVersion = ctx.params.version || ctx.cookies.get(`${ctx.params.dataset}_version`)
    let ddfQuery
    try {
      const json = JSON.parse(decodeURIComponent(ctx.querystring))
      console.log(`Processing ${JSON.stringify(json)}`)
      ddfQuery = new Query(json)
    } catch (err) {
      console.error(err)
      ctx.throw(400, err.message)
    }

    let recordStream

    // make sure that clients that are not very patient don't cause problems
    for (const ev of ['aborted', 'close']) {
      ctx.req.on(ev, () => {
        if (recordStream && recordStream.cleanup) {
          recordStream.cleanUp(new Error('HTTP Request unexpectedly closed'))
        }
      })
    }

    try {
      console.log(`DB has ${DB.idleConnections()} idle connections and ${DB.taskQueueSize()} pending connection requests`)
      const dataset = new Dataset(ctx.params.dataset, datasetVersion)
      await dataset.open()
      if (!datasetVersion) {
        // save the used, default, version in a session cookie
        ctx.cookies.set(`${dataset.name}_version`, dataset.version) // a session cookie is the default
      } else if (ctx.params.version && ctx.cookies.get(`${dataset.name}_version`)) {
        ctx.cookies.set(`${dataset.name}_version`, undefined) // explicit version was asked for, delete the cookie
      }
      if (ctx.headerSent || ctx.req.aborted) {
        return
      }
      recordStream = await dataset.queryStream(ddfQuery, () => ctx.headerSent || ctx.req.aborted, start)
    } catch (err) {
      if (recordStream && recordStream.cleanUp) recordStream.cleanUp(err)
      if (err.code === 'ER_GET_CONNECTION_TIMEOUT') {
        console.log('DDF query request timed out')
        ctx.throw(503, `Sorry, the DDF Service seems too busy, try again later`)
      } else {
        console.error('Unexpected error!')
        console.error(err)
      }
      ctx.throw(500, `Sorry, the DDF Service seems to have a problem, try again later`)
    }
    if (recordStream) {
      const printer = new RecordPrinter(ddfQuery, ddfQuery.isForData)
      printer._destroy = (err) => {
        if (recordStream.cleanUp) recordStream.cleanUp(err)
        console.log(`Responded with ${printer.recordCounter} records`)
      }
      ctx.status = 200
      ctx.setType = 'application/json'
      ctx.compress = ctx.acceptsEncodings('gzip', 'deflate') !== false
      ctx.body = recordStream.pipe(printer)
    } else {
      ctx.throw(503, `Sorry, the DDF Service seems too busy, try again later`)
    }
  })

  app.use(async (ctx, next) => {
    /*
     * Simple check to prevent from this worker to be flooded with requests.
     * This as DDF queries usually take significant amounts of time to process
     */
    if (toobusy() || DB.taskQueueSize() >= 5) {
      console.log(`Too busy!`)
      ctx.throw(503, `Sorry, the DDF Service is too busy, try again later`)
    }
    await next()
  })

  app.use(require('koa2-cors')({
    origin: '*',
    allowMethods: ['GET', 'HEAD', 'OPTIONS']
  }))
  app.use(Compress({ level: zlib.constants.Z_BEST_SPEED }))
  app.use(api.routes())
  app.listen(HTTPPort)
}
