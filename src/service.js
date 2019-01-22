/**
 * Koa (HTTP) service to handle DDF requests.
 *
 */
const Compress = require('koa-compress')
const Koa = require('koa')
const Router = require('koa-router')
const Moment = require('moment')

const { Dataset, DataSource } = require('./datasets')
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
    const dataset = new DataSource(ctx.params.dataset)
    await dataset.open()
    const results = await dataset.queryStream(key, values, start)
    ctx.type = 'application/json'
    ctx.body = results
  })

  app.use(Compress())
  app.use(api.routes())
  app.listen(HTTPPort)
}
