function getEnvironmentVariable(key, _default) {
  const envVAR = process.env[key];
  if (envVAR) {
    if (typeof(_default) === 'boolean') {
      return envVar.toLowerCase === 'true';
    }
    return envVAR;
  } else {
    return _default;
  }
}

module.exports = {
  LogErrors: getEnvironmentVariable('LOG_ERRORS', false),
  MaintenanceMode: getEnvironmentVariable('MAINTENANCE_MODE', false),
  DBHost: getEnvironmentVariable('DB_HOST', 'localhost'),
  DBUser: getEnvironmentVariable('DB_USER', 'gapminder'),
  DBPassword: getEnvironmentVariable('DB_PWD', 'password'),
  DBName: getEnvironmentVariable('DB_NAME', 'gapminder')
};