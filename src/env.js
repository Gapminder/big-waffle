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
  { name: 'HTTPPort', envVar: 'HTTP_PORT', _default: 80 },
  { name: 'DBHost', envVar: 'DB_HOST', _default: 'localhost' },
  { name: 'DBUser', envVar: 'DB_USER', _default: 'gapminder' },
  { name: 'DBPassword', envVar: 'DB_PWD', _default: 'password' },
  { name: 'DBName', envVar: 'DB_NAME', _default: 'gapminder' },
  { name: 'ReservedCPUs', envVar: 'RESERVED_CPUS', _default: 4 }
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
