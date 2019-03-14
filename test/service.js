const { execFileSync } = require('child_process')

const moment = require('moment')
const { after, before, describe, it } = require('mocha')
const chai = require('chai')
chai.should()
chai.use(require('chai-like'))
chai.use(require('chai-things'))

const { DB } = require('../src/maria')
const { DDFService } = require('../src/service')
const { cliOptions, loadTestData, DDFQueryClient } = require('./utils')

describe('DDF Service', function () {
  this.timeout(cliOptions.timeout * 3) // loading test data takes a few seconds

  let client, service, todaysVersion

  before('Load test data and start service', function () {
    // first make sure to delete all test datasets
    execFileSync('node', ['src/cli.js', 'delete', 'test', '_ALL_'], cliOptions)
    // load test data
    loadTestData('test') // this will have a version based on the date, e.g. "2019030601"
    loadTestData('test', 1) // this will have version 'v1'
    todaysVersion = `${moment.utc().format('YYYYMMDD')}01`
    // start the service
    service = DDFService(true) // "forTesting = true" to avoid throttling and spurious logging.
    // prepare the client
    client = DDFQueryClient()
  })

  after('Stop service and delete test data', function (done) {
    function cleanUp () {
      DB.end()
        .then(function () {
          execFileSync('node', ['src/cli.js', 'delete', 'test', '_ALL_'], cliOptions)
        })
        .then(done)
    }
    if (service) {
      service.close(cleanUp)
    } else {
      cleanUp()
    }
  })

  describe('List endpoint', function () {
    it('/list', function () {
      return client.get('/')
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          const list = response.body
          list.should.be.an('array').that.contains.something.eql({ name: 'test', version: 'v1' })
          list.should.contain.something.eql({ name: 'test', version: todaysVersion })
        })
    })
    it('no default version', function () {
      return client.get('/')
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          const list = response.body
          list.should.contain.not.something.like({ name: 'test', default: true })
        })
    })
  })

  describe('Query endpoint', function () {
    it('most recent used as default', function () {
      return client.query({
        select: { key: ['key', 'value'], value: [] },
        from: 'concepts.schema'
      })
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          response.body.should.be.like({ version: 'v1' })
        })
    })
    it('set default version', function () {
      execFileSync('node', ['src/cli.js', 'make-default', 'test', todaysVersion], cliOptions)
      return client.query({
        select: { key: ['key', 'value'], value: [] },
        from: 'concepts.schema'
      })
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          response.body.should.be.like({ version: todaysVersion })
        })
    })
    it('invalid query: key', function () {
      return client.query({
        select: { key: 'key value', value: [] },
        from: 'concepts.schema'
      })
        .set('Accept', 'application/json')
        .expect(400)
    })
    it('invalid query: value', function () {
      return client.query({
        select: { key: ['key', 'value'], values: [] },
        from: 'concepts.schema'
      })
        .set('Accept', 'application/json')
        .expect(400)
    })
    it('invalid query: from missing', function () {
      return client.query({
        select: { key: ['key', 'value'], value: [] }
      })
        .set('Accept', 'application/json')
        .expect(400)
    })
    it('concepts schema', function () {
      return client.query({
        select: { key: ['key', 'value'], value: [] },
        from: 'concepts.schema'
      })
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          response.body.should.be.an('object')
          response.body.should.have.keys(['header', 'rows', 'version'])
          response.body.header.should.have.members(['key', 'value'])
          response.body.rows.should.have.lengthOf(5)
          response.body.rows.should.contain.one.deep.equal([['concept'], 'concept_type'])
        })
    })
    it('fetch entities with domains', function () {
      return client.query({
        select: { key: ['concept'], value: ['domain'] },
        from: 'concepts',
        where: {
          concept_type: 'entity_set'
        }
      })
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          response.body.should.be.an('object')
          response.body.should.have.keys(['header', 'rows', 'version'])
          response.body.header.should.have.members(['concept', 'domain'])
          response.body.rows.should.contain.one.eql(['city', 'geo'])
          response.body.rows.should.contain.one.eql(['gas', null])
        })
    })
    it('fetch populations in cities', function () {
      return client.query({
        select: { key: ['city', 'time'], value: ['population'] },
        from: 'datapoints',
        where: {
          time: { $gte: 2000, $lte: 2015 }
        },
        order_by: ['population']
      })
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          response.body.should.be.an('object')
          response.body.should.have.keys(['header', 'rows', 'version'])
          response.body.header.should.have.members(['city', 'time', 'population'])
          response.body.rows.should.have.lengthOf(2)
          response.body.rows.should.contain.one.eql(['male', 2000, 34567])
          response.body.rows.should.contain.one.eql(['mariehamn_ala', 2000, 12345])
        })
    })
    it('fetch populations in southern cities', function () {
      return client.query({
        select: { key: ['city', 'gender', 'time'], value: ['population'] },
        from: 'datapoints',
        where: {
          $and: [{ geo: '$geo' }]
        },
        join: {
          $geo: {
            key: 'geo',
            where: { latitude: { $lt: 25 } }
          }
        },
        order_by: ['population']
      })
        .set('Accept', 'application/json')
        .expect(200)
        .then(response => {
          response.body.should.be.an('object')
          response.body.should.have.keys(['header', 'rows', 'version'])
          response.body.header.should.have.members(['city', 'gender', 'time', 'population'])
          response.body.rows.should.have.lengthOf(4)
          response.body.rows.should.contain.one.eql(['male', 'male', 1991, 12346])
          response.body.rows.should.contain.one.eql(['hongkong', 'female', 1991, 567890])
        })
    })
  })
})