const { execFileSync } = require('child_process')
const { describe, it } = require('mocha')
const chai = require('chai')
chai.should()
chai.use(require('chai-like'))
chai.use(require('chai-things'))

const { cliOptions, loadTestData, setEnvVar, clearEnvVar } = require('./utils')

function list (name) {
  const output = execFileSync('node', ['src/cli.js', 'list'], cliOptions)
  let result = output.toString().split('\n').filter(line => line.length > 5).map(line => {
    const parts = line.split('.', 2)
    let version = parts[1]
    let isDefaultVersion = false
    if (version.endsWith('*')) {
      isDefaultVersion = true
      version = version.slice(0, -1)
    }
    const entry = {
      name: parts[0],
      version
    }
    if (isDefaultVersion) {
      entry.default = true
    }
    return entry
  })
  if (name) {
    result = result.filter(entry => entry.name && entry.name === 'test')
  }
  return result
}

describe('CLI', function () {
  this.timeout(cliOptions.timeout * 3)
  describe('load', function () {
    it('Load test dataset without errors', function () {
      const scriptOutput = loadTestData()
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test' })
    })
    it('Load test dataset and save with version', function () {
      const scriptOutput = loadTestData('test', 0, 'v2')
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'v2' })
    })
    it('Report error when trying to load same and version', function () {
      const nrOfDatasets = list('test').length
      const scriptOutput = loadTestData('test', 0, 'v2')
      scriptOutput.toString().should.not.match(/error/i)
      list('test').should.be.an('array').with.lengthOf(nrOfDatasets)
    })
    it('Load "wide" dataset without errors', function () {
      setEnvVar('DB_MAX_COLUMNS', 10)
      const scriptOutput = loadTestData('test', 'wide', 'wide')
      clearEnvVar('DB_MAX_COLUMNS')
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'wide' })
    })
  })
  describe('make-default', function () {
    it('Set a version to be the default', function () {
      const args = ['src/cli.js', 'make-default', 'test']
      args.push('v2')
      execFileSync('node', args, cliOptions)
      const datasets = list()
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'v2', default: true })
      datasets.filter(entry => entry.name === 'test' && entry.default).length.should.equal(1)
    })
    // Tests to actually verify that the service returns the correct data for a default version are in the service suite.
  })
  describe('delete', function () {
    it('Delete a specific version', function () {
      const nrDatasetsBefore = list().length
      execFileSync('node', ['src/cli.js', 'delete', 'test', 'v2'], cliOptions)
      const datasets = list()
      datasets.should.have.lengthOf(nrDatasetsBefore - 1)
      datasets.should.be.an('array').that.contains.something.like({ name: 'test' }) // there should be still one version of 'test'
    })
    it('Delete all versions', function () {
      execFileSync('node', ['src/cli.js', 'delete', 'test', '_ALL_'], cliOptions)
      const datasets = list()
      datasets.should.be.an('array').that.does.not.include.something.like({ name: 'test' })
    })
  })
})
