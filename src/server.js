/**
 * This is the main module that serves HTTP requests to BigWaffle.
 *
 */
const cluster = require('cluster')
const { envCopy, ReservedCPUs } = require('./env')
const Log = require('./log')()

const numCPUs = require('os').cpus().length
const numWorkers = Math.max(numCPUs - ReservedCPUs - 1, 1)

if (cluster.isMaster && numWorkers > 1) {
  Log.info(`This system has ${numCPUs} CPUs`)
  Log.info(`Master ${process.pid} is running`)

  // Fork workers.
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork(envCopy)
  }

  cluster.on('exit', (worker, code, signal) => {
    Log.info(`worker ${worker.process.pid} died`)
  })
} else {
  require('./service').DDFService()
  if (numWorkers > 1) {
    Log.info(`Worker ${process.pid} started`)
  }
}
