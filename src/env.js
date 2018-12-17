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
  MongoUrl: getEnvironmentVariable('MONGO_URL', 'mongodb://localhost:27017/systema_globalis'),
  MongoUser: getEnvironmentVariable('MONGO_USER', null),
  MongoPwd: getEnvironmentVariable('MONGO_PWD', 'password'),
};