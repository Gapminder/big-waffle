/**
 * This is the main module that serves HTTP requests to BigWaffle.
 *
 */
const cluster = require('cluster')
const Compress = require('koa-compress')
const Koa = require('koa')
const Router = require('koa-router')
const Moment = require('moment')

const { Dataset, DataSource } = require('./datasets')
const { envCopy, HTTPPort } = require('./env')

class HttpWorker {
  constructor () {
    this.pid = process.pid
    this.app = new Koa()
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
      const resultStream = await dataset.queryStream(key, values, start)
      ctx.type = 'application/json'
      ctx.body = resultStream
    })

    this.app.use(Compress())
    this.app.use(api.routes())
    this.app.listen(HTTPPort)

    console.log(`Worker ${process.pid} started`)
  }
}

const numCPUs = require('os').cpus().length
console.log(`This system has ${numCPUs} CPUs`)
const numWorkers = Math.max(numCPUs - 1, 1)

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`)
  console.log(JSON.stringify(envCopy))
  
  // Fork workers.
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork(envCopy)
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`)
  })
} else {
  console.log(JSON.stringify(envCopy))
  new HttpWorker() 
}
