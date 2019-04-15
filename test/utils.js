const { execFileSync } = require('child_process')

const request = require('supertest')
const Urlon = require('urlon')

const { HTTPPort, DbConnectionTimeout } = require('../src/env')

const cliTimeout = DbConnectionTimeout * 1000 // CLI operations may take up to 5 seconds
const cliOptions = {
  cwd: process.cwd(),
  env: Object.assign({}, process.env),
  timeout: cliTimeout
}

function setEnvVar (name, value) {
  cliOptions.env[name] = value
}

function clearEnvVar (name) {
  if (cliOptions.env[name]) {
    delete cliOptions.env[name]
  }
}
function loadTestData (name = 'test', version = 0, versionLabel) {
  const versionString = typeof version === 'number' ? `v${version}` : version
  const args = ['src/cli.js', 'load', '-d', `test/ddf--testdata/${versionString}`, name]
  if (versionLabel || version) {
    args.push(versionLabel || versionString)
  }
  return execFileSync('node', args, cliOptions)
}

function DDFQueryClient (dataset = 'test') {
  const client = request(`http://localhost:${HTTPPort}`)
  client.query = function (ddfQueryObject, version, useUrlon = false) {
    const versionPart = version ? `/${version}` : ''
    const queryString = useUrlon ? encodeURIComponent(Urlon.stringify(ddfQueryObject)) : encodeURIComponent(JSON.stringify(ddfQueryObject))
    return client.get(`/${dataset}${versionPart}?${queryString}`).redirects(version ? 0 : 1)
  }
  return client
}

module.exports = {
  cliOptions,
  loadTestData,
  DDFQueryClient,
  setEnvVar,
  clearEnvVar
}
