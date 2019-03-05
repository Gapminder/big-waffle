const { execFileSync } = require('child_process')
const { describe, it } = require('mocha')
const chai = require('chai')
chai.should()
chai.use(require('chai-like'))
chai.use(require('chai-things'))

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

function list () {
  const output = execFileSync('node', ['src/cli.js', 'list'], cliOptions)
  return output.toString().split('\n').filter(line => line.length > 5).map(line => {
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
}

describe('CLI', function () {
  this.timeout(cliTimeout)
  describe('load', function () {
    it('Load test dataset without errors', function () {
      const scriptOutput = loadTestData()
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list()
      datasets.should.be.an('array').that.contains.something.like({ name: 'test' })
    })
    it('Load test dataset and save with version', function () {
      const scriptOutput = loadTestData('test', 'v2')
      scriptOutput.toString().should.not.match(/error/i)
      const datasets = list()
      datasets.should.be.an('array').that.contains.something.like({ name: 'test', version: 'v2' })
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
