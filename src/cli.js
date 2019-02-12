const { resolve } = require('path')
const { ArgumentParser } = require('argparse')
const Moment = require('moment')

const { DB } = require('./maria')
const { Dataset } = require('./ddf/datasets')

function load (name, dirPath, options) {
  const startTime = Moment.utc()
  const sg = new Dataset(name)
  return sg.open()
    .then(async function (ds) {
      if (options.replace !== true) {
        ds.incrementVersion()
      }
      await ds.loadFromDirectory(dirPath, options)
      console.log(`Loading ${ds.name}.${ds.version} took ${Moment.utc().diff(startTime, 'minutes')} minutes.`)
      if (options.onlyParse !== true) {
        await ds.save()
      }
      DB.end()
    })
}

const parser = new ArgumentParser({
  version: '0.1',
  addHelp: true,
  description: 'Tool to load and manage datasets in BigWaffle'
})
const subparsers = parser.addSubparsers({
  title: 'commands',
  dest: 'command'
})
const deleteCmd = subparsers.addParser('delete', {
  help: `Deletes a dataset with all its associated tables`
})
deleteCmd.addArgument(
  'dataset',
  {
    help: 'The name of the dataset'
  }
)
const listCmd = subparsers.addParser('list', {
  help: `List datasets and versions. Give a dataset name to only see the versions of that dataset.`
})
listCmd.addArgument(
  'dataset',
  {
    defaultValue: undefined,
    nargs: '?',
    help: 'The name of the dataset'
  }
)
const loadCmd = subparsers.addParser('load', {
  help: 'Loads a dataset from a directory with CSV files'
})
loadCmd.addArgument(
  ['-d', '--directory'],
  {
    nargs: '?',
    default: '.',
    help: 'Path to the directory that holds the datapackage.json file'
  }
)
loadCmd.addArgument(
  ['--only-parse'], // this will be 'only_parse' in the parsed arguments!
  {
    action: 'storeTrue',
    help: 'Does not actually load the data, but parses all data and prints the proposed schema'
  }
)
loadCmd.addArgument(
  ['--replace'],
  {
    action: 'storeTrue',
    help: 'Does not change the version of the dataset, so replaces the data of the current default version'
  }
)
loadCmd.addArgument(
  'dataset',
  {
    help: 'The name of the dataset'
  }
)
const makeDefaultCmd = subparsers.addParser('make-default', {
  help: 'Make the given version of a given dataset the default version'
})
makeDefaultCmd.addArgument(
  'dataset',
  {
    help: 'The name of the dataset'
  }
)
makeDefaultCmd.addArgument(
  'version',
  {
    help: 'The version of the dataset that should be the default'
  }
)

async function run () {
  const args = parser.parseArgs()
  if (args.command === 'load') {
    return load(args.dataset, resolve(args.directory), { onlyParse: args.only_parse, replace: args.replace })
  } else if (args.command === 'delete') {
    return Dataset.remove(args.dataset)
  } else if (args.command === 'list') {
    const versions = await Dataset.all(args.dataset)
    if (versions && versions.length > 0) {
      for (const version of versions) {
        console.log(`${version.name}.${version.version}${version.is__default ? '*' : ''}`)
      }
    } else {
      const named = args.dataset ? `named '${args.dataset}' ` : ''
      console.log(`No datasets ${named}found.`)
    }
    return null
  } else if (args.command === 'make-default') {
    return Dataset.makeDefaultVersion(args.dataset, args.version)
  } else {
    parser.printUsage()
  }
}

run()
  .then(res => process.exit(0))
  .catch(err => {
    console.error(err)
    process.exit(1)
  })
