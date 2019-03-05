const { execFileSync } = require('child_process')

const { after, before, describe, it } = require('mocha')
const request = require('supertest')
const chai = require('chai')
chai.should()
chai.use(require('chai-like'))
chai.use(require('chai-things'))
const Urlon = require('urlon')

const { DB } = require('../src/maria')
const { HTTPPort } = require('../src/env')
const { DDFService } = require('../src/service')

const cliTimeout = 5 * 1000 // CLI operations may take up to 5 seconds
const cliOptions = {
  cwd: process.cwd(),
  env: Object.assign({}, process.env),
  timeout: cliTimeout
}

function loadTestData (name = 'test', asVersion) {
  const args = ['src/cli.js', 'load', '-d', 'test/ddf--testdata', 'test']
  if (asVersion) {
    args.push(asVersion)
  }
  return execFileSync('node', args, cliOptions)
}

function DDFQueryClient (dataset = 'test', version) {
  const client = request(`http://localhost:${HTTPPort}`)
  client.query = function (ddfQueryObject, useUrlon = false) {
    const versionPart = version ? `/${version}` : ''
    const queryString = useUrlon ? encodeURIComponent(Urlon.stringify(ddfQueryObject)) : encodeURIComponent(JSON.stringify(ddfQueryObject))
    return client.get(`/${dataset}${versionPart}?${queryString}`)
  }
  return client
}

describe('DDF Service', function () {
  this.timeout(cliTimeout) // loading test data takes a few seconds

  let client, service

  before('Load test data and start service', function () {
    loadTestData()
    service = DDFService(true) // "forTesting = true" to avoid throttling and spurious logging.
    client = DDFQueryClient()
  })

  after('Stop service and delete test data', function (done) {
    execFileSync('node', ['src/cli.js', 'delete', 'test', '_ALL_'], cliOptions)
    DB.end()
      .then(() => {
        if (service) {
          service.close(done)
        } else {
          done()
        }
      })
  })

  describe('List endpoint', function () {
    it('/list', function () {
      return client.get('/')
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          response.body.should.be.an('array').that.contains.something.like({ name: 'test' })
        })
    })
  })
})
