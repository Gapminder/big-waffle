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
  connectionLimit: 500
})
DB.name = env.DBName

Object.assign(exports, {
  DB
})
