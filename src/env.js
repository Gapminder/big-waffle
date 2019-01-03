function getEnvironmentVariable(key, _default) {
  const envVAR = process.env[key];
  if (envVAR) {
    if (typeof(_default) === 'boolean') {
      return envVar.toLowerCase === 'true';
    } else if (typeof(_default) === 'number') {
      return Number(envVAR);
    }
    return envVAR;
  } else {
    return _default;
  }
}

module.exports = {
  LogErrors: getEnvironmentVariable('LOG_ERRORS', false),
  MaintenanceMode: getEnvironmentVariable('MAINTENANCE_MODE', false),
  HTTPPort: getEnvironmentVariable('HTTP_PORT', 80),
  DBHost: getEnvironmentVariable('DB_HOST', 'localhost'),
  DBUser: getEnvironmentVariable('DB_USER', 'gapminder'),
  DBPassword: getEnvironmentVariable('DB_PWD', 'password'),
  DBName: getEnvironmentVariable('DB_NAME', 'gapminder')
};