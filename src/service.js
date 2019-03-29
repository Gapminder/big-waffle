/**
 * Koa (HTTP) service to handle DDF requests.
 *
 */
const zlib = require('zlib')
const Compress = require('koa-compress')
const Koa = require('koa')
const Router = require('koa-router')
const Moment = require('moment')
const Urlon = require('urlon')

const { DB } = require('./maria')
const { Dataset, Query, RecordPrinter } = require('./ddf')
const { AllowCaching, HTTPPort, CPUThrottle, DBThrottle } = require('./env')
const Log = require('./log')('service')

module.exports.DDFService = function (forTesting = false) {
  const app = new Koa()
  const api = new Router() // routes for the main API

  const loaderIOToken = process.env.LOADER_IO_TOKEN
  if (loaderIOToken) {
    api.get(`/${loaderIOToken}.txt`, async (ctx, next) => {
      /*
      * Route to serve verification by the Loader TaaS
      */
      ctx.type = 'text/plain'
      ctx.body = loaderIOToken
    })
  }

  api.get('/ddf-service-directory', (ctx, next) => {
    ctx.body = {
      list: '/',
      query: '/DATASET/VERSION',
      assets: 'DATASET/VERSION/assets/ASSET'
    }
  })

  api.get('/', async (ctx, next) => {
    /*
     * List all (public) datasets that are currently available.
     */
    const datasets = await Dataset.all()
    ctx.set('Cache-Control', 'no-cache, no-store, must-revalidate')
    ctx.body = datasets.map(ds => {
      const rec = {
        name: ds.name,
        version: ds.version
      }
      if (ds.is__default) rec.default = true
      return rec
    })
  })

  api.get('/:dataset([-a-z_0-9]+)/:version([-a-z_0-9]+)?/assets/:asset([-a-z_0-9.]+)', async (ctx, next) => {
    try {
      Log.debug(`DB has ${DB.idleConnections()} idle connections and ${DB.taskQueueSize()} pending connection requests`)
      const dataset = new Dataset(ctx.params.dataset, ctx.params.version)
      await dataset.open(true)
      if (!ctx.params.version) {
        ctx.redirect(`/${dataset.name}/${dataset.version}/assets/${ctx.params.asset}`)
      } else {
        const url = await dataset.urlForAsset(ctx.params.asset, ctx.secure)
        ctx.status = 301 // Permanent redirect!
        ctx.redirect(url)
      }
    } catch (err) {
      if (err.code === 'DDF_DATASET_NOT_FOUND') {
        ctx.throw(404, err.message)
      } else {
        Log.error(err)
      }
      ctx.throw(500, `Sorry, the DDF Service seems to have a problem, try again later`)
    }
  })

  api.get('/:dataset([-a-z_0-9]+)/:version([-a-z_0-9]+)?', async (ctx, next) => {
    Log.debug('Received DDF query')
    const start = Moment()
    let ddfQuery, json
    let version = ctx.params.version
    try {
      if (!(typeof ctx.querystring === 'string' && ctx.querystring.length > 10)) {
        throw new Error('Request has no query')
      }
      try {
        json = Urlon.parse(decodeURIComponent(ctx.querystring)) // despite using urlon we still need to decode!
      } catch (urlonError) {
        json = JSON.parse(decodeURIComponent(ctx.querystring))
      }
      Log.info({ query: json })
      ddfQuery = new Query(json)
    } catch (err) {
      Log.error(err)
      ctx.throw(400, err instanceof SyntaxError ? `Query is malformed: ${err.message}` : err.message)
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
      Log.debug(`DB has ${DB.idleConnections()} idle connections and ${DB.taskQueueSize()} pending connection requests`)
      const dataset = new Dataset(ctx.params.dataset, version)
      await dataset.open(true)
      if (ctx.headerSent || ctx.req.aborted) {
        return
      }
      if (!ctx.params.version) {
        ctx.redirect(`/${dataset.name}/${dataset.version}?${ctx.querystring}`)
        return
      } else {
        version = dataset.version
        recordStream = await dataset.queryStream(ddfQuery, () => ctx.headerSent || ctx.req.aborted, start)
      }
    } catch (err) {
      if (recordStream && recordStream.cleanUp) recordStream.cleanUp(err)
      if (err.code === 'DDF_DATASET_NOT_FOUND') {
        ctx.throw(404, err.message)
      } else if (err.code === 'ER_GET_CONNECTION_TIMEOUT') {
        Log.warn('DDF query request timed out')
        ctx.throw(503, `Sorry, the DDF Service seems too busy, try again later`)
      } else {
        Log.error(err)
      }
      ctx.throw(500, `Sorry, the DDF Service seems to have a problem, try again later`)
    }
    if (recordStream) {
      const printer = new RecordPrinter(ddfQuery, ddfQuery.isForData)
      printer.datasetVersion = version // to ensure the HTTP response includes the actual version used to answer this query
      printer._destroy = (err) => {
        if (recordStream.cleanUp) recordStream.cleanUp(err)
        Log.info(`Responded with ${printer.recordCounter} records`)
      }
      ctx.status = 200
      ctx.type = 'application/json'
      ctx.set('Cache-Control', AllowCaching ? 'public, max-age=31536000, immutable' : 'no-cache, no-store, must-revalidate')
      if (AllowCaching) {
        ctx.set('Cache-Tag', `${ctx.params.dataset}/${version}`)
      }
      ctx.compress = ctx.acceptsEncodings('gzip', 'deflate') !== false
      ctx.body = recordStream.pipe(printer)
    } else {
      ctx.throw(503, `Sorry, the DDF Service seems too busy, try again later`)
    }
  })

  if (forTesting !== true) { // when running tests it's generally nicer to run without throttling to avoid a lot of logging.
    const TooBusy = require('toobusy-js')
    if (CPUThrottle) {
      TooBusy.maxLag(CPUThrottle)
      TooBusy.interval(250)
      TooBusy.onLag(currentLag => {
        if (currentLag > 200) {
          Log.warn(`Event loop lag ${currentLag}ms`)
        } else {
          Log.info(`Event loop lag ${currentLag}ms`)
        }
      })
    }
    app.use(async (ctx, next) => {
      /*
      * Simple check to prevent from this worker to be flooded with requests.
      * This as DDF queries usually take significant amounts of time to process
      */
      if ((CPUThrottle && TooBusy()) || (DBThrottle && DB.taskQueueSize() >= DBThrottle)) {
        Log.info(`Too busy!`)
        ctx.throw(503, `Sorry, the DDF Service is too busy, try again later`)
      }
      await next()
      Log.info({ req: ctx.request, res: ctx.response })
    })
  }

  app.use(require('koa2-cors')({
    origin: '*',
    allowMethods: ['GET', 'HEAD', 'OPTIONS']
  }))
  app.use(Compress({ level: zlib.constants.Z_BEST_SPEED }))
  app.use(api.routes())
  return app.listen(HTTPPort)
}
