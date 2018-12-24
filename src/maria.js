/**
 * Common constants and functions for the MongoDB.
 */
const MariaDB = require('mariadb');

const env = require('./env');
console.log(env);
 
const DB = MariaDB.createPool({
  host: env.DBHost,
  user: env.DBUser,
  password: env.DBPassword,
  database: env.DBName,
  connectionLimit: 20
});

Object.assign(exports, {
  DB
});
