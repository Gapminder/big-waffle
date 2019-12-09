require('dotenv').config() // load a .env file if there is any

function getEnvironmentVariable (key, _default) {
  const envVAR = process.env[key]
  if (envVAR) {
    if (typeof _default === 'boolean') {
      return envVAR.toLowerCase() === 'true'
    } else if (typeof _default === 'number') {
      return Number(envVAR)
    }
    return envVAR
  } else {
    return _default
  }
}

const envVars = [
  { name: 'LogLevel', envVar: 'LOG_LEVEL', _default: 'info' }, // one of 'trace', 'debug', 'info', 'warn', 'error', 'fatal'
  { name: 'MaintenanceMode', envVar: 'MAINTENANCE_MODE', _default: false },
  { name: 'CPUThrottle', envVar: 'CPU_THROTTLE', _default: 200 }, // set to 0 to disable
  { name: 'DBThrottle', envVar: 'DB_THROTTLE', _default: 5 }, // set to 0 to disable
  { name: 'ExternalLogger', envVar: 'EXTERNAL_LOG', _default: 'none' }, // Could be 'Google Cloud', etc.
  { name: 'HTTPPort', envVar: 'HTTP_PORT', _default: 80 },
  { name: 'AllowCaching', envVar: 'CACHE_ALLOW', _default: true },
  { name: 'DBHost', envVar: 'DB_HOST', _default: 'localhost' },
  { name: 'DBUser', envVar: 'DB_USER', _default: '__USER__' },
  { name: 'DBPassword', envVar: 'DB_PWD' },
  { name: 'DBSocketPath', envVar: 'DB_SOCKET_PATH', _default: '/var/run/mysqld/mysqld.sock' },
  { name: 'DbConnectionTimeout', envVar: 'DB_CONNECTION_TIMEOUT', _default: 5 }, // in seconds
  { name: 'DBName', envVar: 'DB_NAME', _default: 'gapminder' },
  { name: 'ReservedCPUs', envVar: 'RESERVED_CPUS', _default: 4 },
  { name: 'AssetStore', envVar: 'ASSET_STORE', _default: 'GCS' }, // one of ['GCS'] (and later perhaps S3, etc.)
  { name: 'AssetStoreBucket', envVar: 'ASSET_STORE_BUCKET' },
  { name: 'SlackChannelUrl', envVar: 'SLACK_CHANNEL_URL' },
  { name: 'MaxColumns', envVar: 'DB_MAX_COLUMNS', _default: 1000 },
  { name: 'BehindProxy', envVar: 'BEHIND_PROXY', _default: true } // set to FALSE to ignore proxy headers such as "X-Forwarded-Proto"
]

function envCopy () {
  /*
   * Return a plain object with the relevant process environment variables "as is".
   *
   * This is useful when forking the main process.
   */
  return envVars.reduce((map, def) => {
    const envValue = process.env[def.envVar]
    if (envValue !== undefined) {
      map[def.envVar] = envValue
    }
    return map
  }, {})
}

module.exports = envVars.reduce((map, def) => {
  map[def.name] = getEnvironmentVariable(def.envVar, def._default)
  return map
}, {})

module.exports.envCopy = envCopy()
