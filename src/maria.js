/**
 * Creates and exports the connection pool to the MariaDB.
 */
const MariaDB = require('mariadb')

const env = require('./env')

const connectionOptions = {
  host: env.DBHost,
  database: env.DBName,
  user: env.DBUser === '__USER__' ? require('os').userInfo().username : env.DB_USER,
  connectionLimit: 10,
  acquireTimeout: env.DbConnectionTimeout * 1000, // Keep this shorter than the typical timeout used on a client!
  noControlAfterUse: true // This allows for quick release after the connection has streamed results.
}

if (env.DBPassword === undefined) {
  // use socket based connection and Unix auth (no explicit password)
  connectionOptions.socketPath = env.DBSocketPath
} else {
  connectionOptions.password = env.DBPassword
}

const DB = MariaDB.createPool(connectionOptions)
DB.name = env.DBName

Object.assign(exports, {
  DB
})
