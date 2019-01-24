/**
 * Common constants and functions for the MongoDB.
 */
const MariaDB = require('mariadb')

const env = require('./env')

const DB = MariaDB.createPool({
  host: env.DBHost,
  user: env.DBUser,
  password: env.DBPassword,
  database: env.DBName,
  connectionLimit: 3,
  acquireTimeout: 5000, // Keep this shorter than the typical timeout used on a client!
  noControlAfterUse: true // This allows for quick release after the connection has streamed results.
})
DB.name = env.DBName

Object.assign(exports, {
  DB
})
