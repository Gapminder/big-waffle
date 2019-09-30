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
      datasets.should.contain.something.like({ name: 'test', default: true })
    })
    it('Report error when trying to load existing dataset and version', function () {
      const nrOfDatasets = list('test').length
      const testfn = () => loadTestData('test', 0, 'v2')
      testfn.should.throw()
      list('test').should.be.an('array').with.lengthOf(nrOfDatasets)
    })
    it(`Report error when trying to load with "latest" as version`, function () {
      const nrOfDatasets = list('test').length
      const testfn = () => loadTestData('test', 0, 'latest')
      testfn.should.throw()
      list('test').should.be.an('array').with.lengthOf(nrOfDatasets)
    })
    it('Load test dataset and publish it', function () {
      const scriptOutput = loadTestData('test', 0, 'v3', true)
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'v3', default: true })
      datasets.filter(entry => entry.name === 'test' && entry.default).length.should.equal(1)
    })
    it('Load dataset with zeros and null values but issue warning', function () {
      const scriptOutput = loadTestData('test', 1, 'zeros')
      scriptOutput.toString().should.match(/contains null value/i)
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'zeros' })
    })
    it('Load test dataset version protected with a password', function () {
      const scriptOutput = loadTestData('test', 0, 'protected', false, 'foobar')
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'protected' })
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
  describe('purge', function () {
    it('Purge with nothing to delete', function () {
      const args = ['src/cli.js', 'purge', 'test']
      execFileSync('node', args, cliOptions)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'v2', default: true })
      datasets.length.should.equal(6)
    })
    it('Purge only older then default', function () {
      execFileSync('node', ['src/cli.js', 'make-default', 'test', 'v3'], cliOptions) // make v3 default => v1 can be purged
      const args = ['src/cli.js', 'purge', 'test']
      execFileSync('node', args, cliOptions)
      const datasets = list('test')
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'v3', default: true })
      datasets.length.should.equal(5)
    })
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
