/**
 * Common constants and functions for the MongoDB.
 */
const MariaDB = require('mariadb')

const env = require('./env')
console.log(JSON.stringify(env))

const DB = MariaDB.createPool({
  host: env.DBHost,
  user: env.DBUser,
  password: env.DBPassword,
  database: env.DBName,
  connectionLimit: 10
})
DB.name = env.DBName

Object.assign(exports, {
  DB
})
