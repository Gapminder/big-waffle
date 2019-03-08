/**
 * Creates and exports the connection pool to the MariaDB.
 */
const MariaDB = require('mariadb')

const env = require('./env')

const connectionOptions = {
  host: env.DBHost,
  database: env.DBName,
  connectionLimit: 10,
  acquireTimeout: 5000, // Keep this shorter than the typical timeout used on a client!
  noControlAfterUse: true // This allows for quick release after the connection has streamed results.
}

if (env.DBUser === '__USER__') {
  // use socket based connection and Unix auth (no explicit password)
  connectionOptions.user = require('os').userInfo().username
  connectionOptions.socketPath = env.DBSocketPath
} else {
  connectionOptions.user = env.DBUser
  connectionOptions.password = env.DBPassword
}

const DB = MariaDB.createPool(connectionOptions)
DB.name = env.DBName

Object.assign(exports, {
  DB
})
