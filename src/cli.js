const { resolve } = require('path')
const { ArgumentParser } = require('argparse')
const Moment = require('moment')

const { DB } = require('./maria')
const { Dataset } = require('./ddf/datasets')
const { Slack } = require('./notifications')

function load (name, version, dirPath, options) {
  if (version === 'latest') {
    throw new Error(`Cannot use "latest" as a version`)
  }
  return Dataset.open(name, version)
    .then(async function (ds) {
      if (options.assetsOnly) {
        await ds.importAssets(dirPath)
      } else {
        if (!ds.isNew && version) {
          throw new Error(`Dataset ${name}.${version} already exists`)
        }
        if (!ds.isNew || !version) {
          ds.incrementVersion()
        }
        if (ds.isNew && options.password) {
          ds.password = options.password
        }
        await Slack(`Starting to load dataset ${name} from ${dirPath}${version ? `.${version}` : ''}`)
        const startTime = Moment.utc()
        await ds.loadFromDirectory(dirPath, options)
        if (options.onlyParse !== true) {
          await ds.save(options.publish)
        }
        const msg = `Loading dataset ${ds.name}.${ds.version} took ${Moment.utc().diff(startTime, 'minutes')} minutes.`
        console.log(msg)
        await Slack(msg)
      }
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
deleteCmd.addArgument(
  'version',
  {
    help: `The version of the dataset that should be deleted. "_ALL_" will delete all versions!`
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
  ['--password'],
  {
    help: 'Plain password to protect this version of the dataset'
  }
)
loadCmd.addArgument(
  ['--publish'],
  {
    action: 'storeTrue',
    help: 'Ensure that once loaded this version will be the one served to clients by default'
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
  ['-a', '--assets-only'],
  {
    action: 'storeTrue',
    help: 'Only imports assets into a dataset'
  }
)
loadCmd.addArgument(
  'dataset',
  {
    help: 'The name of the dataset'
  }
)
loadCmd.addArgument(
  'version',
  {
    defaultValue: undefined,
    nargs: '?',
    help: 'A version string to label the loaded dataset with'
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
    help: `The version of the dataset that should be the default, can be 'latest'.`
  }
)
const purgeCmd = subparsers.addParser('purge', {
  help: 'Delete old versions of a dataset. The default (or latest) version, the version preceding that one, and any version newer than that will be retained.'
})
purgeCmd.addArgument(
  'dataset',
  {
    help: 'The name of the dataset'
  }
)

async function showList (datasets, named = undefined) {
  if (!Array.isArray(datasets)) {
    process.exitCode = 1
    datasets = named ? await Dataset.all(named) : []
  }
  if (datasets.length > 0) {
    for (const dataset of datasets) {
      console.log(`${dataset.name}.${dataset.version}${dataset.is__default ? '*' : ''}`)
    }
  } else {
    console.log(`No datasets ${named ? `named '${named}' ` : ''}found.`)
  }
}

async function run () {
  const args = parser.parseArgs()
  if (args.command === 'load') {
    return load(args.dataset, args.version, resolve(args.directory), { assetsOnly: args.assets_only, onlyParse: args.only_parse, publish: args.publish, password: args.password })
  } else if (args.command === 'delete') {
    return showList(await Dataset.remove(args.dataset, args.version), args.dataset)
  } else if (args.command === 'list') {
    return showList(await Dataset.all(args.dataset), args.dataset)
  } else if (args.command === 'make-default') {
    return showList(await Dataset.makeDefaultVersion(args.dataset, args.version), args.dataset)
  } else if (args.command === 'purge') {
    return showList(await Dataset.purge(args.dataset), args.dataset)
  } else {
    return parser.printUsage()
  }
}

run()
  .then(res => {
    process.exitCode = 0
  })
  .catch(err => {
    console.info(`Error: ${err.message}`)
    process.exitCode = 1
  })
  .finally(() => {
    DB.end()
  })
