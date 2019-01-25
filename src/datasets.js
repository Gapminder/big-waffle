const { Transform, finished } = require('stream')
const JSONFile = require('jsonfile')
const Moment = require('moment')

const { DB } = require('./maria')
const { Table } = require('./collections')

class RecordPrinter extends Transform {
  constructor (header) {
    /*
    * Return a Transform stream that can pipe records (Object instances) to a stringified (textual) JSON array representation.
    */
    super(
      {
        readableHighWaterMark: 1000000,
        writableHighWaterMark: 1000,
        writableObjectMode: true
      }
    )
    this.header = header
    this.recordCounter = 0
  }

  _transform (chunk, encoding, callback) {
    try {
      if (this.header && !this._headerPushed) {
        this._headerPushed = true
        this.push(`[\n${JSON.stringify(this.header)}\n`)
      }
      this.push(`,${JSON.stringify(chunk)}`)
      this.recordCounter += 1
      callback()
    } catch (err) {
      console.error(err)
      callback(err)
    }
  }

  _flush (callback) {
    try {
      this.push(`\n]`)
      return callback()
    } catch (err) {
      console.error(err)
      return callback(err)
    }
  }
}

class Dataset {
  /*
   * A Dataset encapsulates a DDF compliant set of data, including:
   * - entity domains
   * - concepts
   * - datapoints
   * - metadata
   *
   */
  constructor (name, version) {
    this.name = name
    this.version = version
    this.entitySets = {} // mapping of entity sets to entity domains
  }

  toJSON () {
    /*
     * Return a plain object that represents this dataset and
     * can be saved in a collection.
     */
    const doc = {}
    for (const key in this) {
      if (key.startsWith('_') === false) {
        doc[key] = this[key]
      }
    }
    return doc
  }

  initialize (doc) {
    Object.assign(this, doc)
  }

  get _keyFields () {
    return ['name', 'version']
  }

  incrementVersion (newVersion = null) {
    if (!this.version) {
      this.version = newVersion || `${Moment.utc().seconds(1).format('YYYYMMDDss')}`
    } else if (newVersion) {
      this.version = newVersion
    } else if (/[0-9]{2}$/.test(this.version)) {
      let root = this.version.slice(0, -2)
      const minorVersion = Number.parseInt(this.version.slice(-2))
      console.log(minorVersion)
      let versionDate = Moment.utc(root, 'YYYYMMDD', true)
      if (versionDate.isValid()) {
        if (versionDate.isSameOrAfter(Moment.utc(), 'day')) {
          versionDate.seconds(minorVersion + 1)
        } else {
          versionDate = Moment.utc().seconds(1)
        }
        this.version = `${versionDate.format('YYYYMMDDss')}`
      } else {
        this.version = `${root}${minorVersion < 10 ? '0' : ''}${minorVersion + 1}`
      }
    } else {
      this.version = this.version + '1'
    }
    this._isNew = true
  }

  async save () {
    if (!this.version) {
      this.incrementVersion()
    }
    const doc = this.toJSON()
    let filter = ''
    if (filter.endsWith(',')) {
      filter = filter.slice(0, -1)
    }
    let sql
    if (this._isNew) {
      filter = this._keyFields.reduce((flt, field) => {
        flt += ` ${field} = '${doc[field]}',`
        delete doc[field]
        return flt
      }, '')
      sql = `INSERT INTO datasets SET${filter} definition = '${JSON.stringify(doc)}';`
    } else {
      filter = this._keyFields.reduce((flt, field) => {
        flt += ` ${field} = '${doc[field]}' AND `
        delete doc[field]
        return flt
      }, '')
      if (filter.endsWith(' AND ')) {
        filter = filter.slice(0, -5)
      }
      sql = `UPDATE datasets SET definition = '${JSON.stringify(doc)}' WHERE${filter};`
    }
    try {
      await DB.query(sql)
      console.log(`${this._isNew ? 'Inserted' : 'Updated'} dataset ${this.name}.${this.version}`)
      delete this._isNew
    } catch (dbErr) {
      console.error(dbErr)
    }
    return this
  }
}

class DataSource extends (Dataset) {
  /*
   * A DataSource manages DDF compliant datasets, typically with collections
   * for:
   * - datapoints
   * - concepts
   * - each of the entity domains
   * - metadata
   *
   * A DataSource exposes either the latest version or a named version.
   * Under the hood the DataSource instance ensures to use the right version for each
   * collection.
   *
   */
  constructor (name, version) {
    super(name)
    this.datapoints = {} // mapping of key tuples to Collections (Tables).
  }

  _getCollection (collectionName) {
    return `${this.name}_${collectionName}_${this.version}`
  }

  _getDatapointCollection (key) {
    let collection = this.datapoints[key]
    if (!collection) {
      collection = this.datapoints[key] = new Table(this._getCollection(key))
    }
    return collection
  }

  async open () {
    try {
      let sql = `SELECT name, version, definition FROM datasets WHERE name = '${this.name}'`
      if (this.version) {
        sql += ` AND version = '${this.version}'`
      }
      sql += ` ORDER BY version DESC`

      const docs = await DB.query(sql)
      const doc = docs && docs.length >= 1 ? docs[0] : undefined
      if (doc) {
        this.version = doc.version
        this.initialize(JSON.parse(doc.definition))
        console.log(`Loaded dataset ${this.name}.${this.version} from DB`)
        if (this._isNew) {
          this._isNew = false
        }
      } else {
        this._isNew = true
      }
    } catch (dbError) {
      console.error(dbError)
    }
    return this
  }

  async queryStream (key, values, abortCheck = () => false, start = undefined) {
    const table = this._getDatapointCollection(key)
    const sql = `SELECT ${values.join(', ')} FROM ${table.name};`
    const connection = await DB.getConnection()

    // we may have had to wait a long time to get the connection so check if we should abort
    if (abortCheck()) {
      connection.end()
      return null
    }

    const recordStream = connection.queryStream({ sql, rowsAsArray: true })
    // const textStream = recordStream.pipe(new RecordPrinter(values, start)) // .pipe(process.stdout)
    finished(recordStream, (err) => {
      if (err) {
        console.error(err)
      }
      // process.nextTick(() => {
        console.log(`Requesting to release connection ${err ? 'because of error' : ''}`)
        connection.end()
          .then(() => console.log(`Released DB connection. DB now has ${DB.idleConnections()} connections`),
            () => console.log(`Released DB connection despite error`))
      })
  // })
    return recordStream
    // const results = await DB.query({ sql, rowsAsArray: true })
    // results.unshift(values)
    // return results
  }

  async revert () {
    /*
     * Restore this Dataset to the one-but-last version
     */
  }

  _getFieldMapForEntityCSVFile (filename) {
    const filenameParser = /ddf-{2}entities-{2}([a-z0-9]+)(-{2}[_a-z0-9]+)?/

    const parsedFilename = filenameParser.exec(filename)
    const domain = parsedFilename[1]
    const idColumnName = parsedFilename[2] ? parsedFilename[2].substring(2) : domain
    if (idColumnName && idColumnName !== domain) {
      return { [idColumnName]: domain }
    }
    return {}
  }

  async loadFromDirectory (dirPath, options) {
    if (dirPath.endsWith('/')) {
      dirPath = dirPath.slice(0, -1)
    }

    const dataPackage = await JSONFile.readFile(`${dirPath}/datapackage.json`)
    const resources = dataPackage.resources.reduce((all, resource) => {
      all[resource.name] = resource.path
      return all
    }, {})
    // 1. Read concepts from file and store in 'concepts' collection.
    const concepts = new Table(this._getCollection('concepts'))
    concepts.primaryIndexOn('concept')
    await concepts.updateSchemaFromCSVFile(dirPath + '/ddf--concepts.csv')
    await concepts.createIn(DB)
    await concepts.loadFromCSVFile(dirPath + '/ddf--concepts.csv')
    // 2. Map entity sets to entity domains
    const domains = (await DB.query(`SELECT concept FROM ${concepts.name} WHERE concept_type = 'entity_domain';`))
      .reduce((d, domain) => {
        d[domain.concept] = new Set()
        return d
      }, {})
    this.entitySets = (await DB.query(`SELECT concept, domain FROM ${concepts.name} WHERE concept_type = 'entity_set';`))
      .reduce((s, entitySet) => {
        s[entitySet.concept] = entitySet.domain
        return s
      }, {})
    console.log(this.entitySets)
    // 2. Create tables for each entity domain and load all files for that entity domain.
    for (const entityDef of dataPackage.ddfSchema.entities) {
      const entitySet = entityDef.primaryKey[0]
      const domain = domains[entitySet] ? entitySet : this.entitySets[entitySet]
      if (!domain) {
        throw new Error(`Domain for entity set ${entitySet} was not defined in concepts`)
      }
      entityDef.resources.forEach(res => domains[domain].add(resources[res]))
    }
    console.log(domains)
    Object.keys(domains).forEach(async dom => {
      const table = new Table(this._getCollection(dom))
      table.primaryIndexOn(dom)
      const files = domains[dom]
      for (const file of files) {
        await table.updateSchemaFromCSVFile(`${dirPath}/${file}`, this._getFieldMapForEntityCSVFile(file))
      }
      console.log(table._schema)
      await table.createIn(DB)
      for (const file of files) {
        await table.loadFromCSVFile(`${dirPath}/${file}`, this._getFieldMapForEntityCSVFile(file))
      }
    })
    // 3. Now create the datapoints tables and then load all the data.
    for (const datapointDef of dataPackage.ddfSchema.datapoints) {
      let primaryIndex = []
      for (const key of datapointDef.primaryKey) {
        primaryIndex.push(this.entitySets[key] || key)
      }
      primaryIndex = primaryIndex.sort().join('$') // the dollar sign is allowed in table names
      let dpCollection = this._getDatapointCollection(primaryIndex)
      dpCollection._files = dpCollection._files || {}
      for (const res of datapointDef.resources) {
        const file = resources[res]
        if (dpCollection._files[file] !== datapointDef.value) { // files are often used multiple times, for different entity sets
          dpCollection._files[file] = datapointDef.value
          await dpCollection.updateSchemaFromCSVFile(`${dirPath}/${file}`, this.entitySets)
        }
      }
    }
    for (const key in this.datapoints) {
      const table = this.datapoints[key]
      if (options.onlyParse === true) {
        table.optimizeSchema()
        console.log(table._schema)
        console.log(`Expected row size is ${table.estimatedRowSize} bytes`)
      } else {
        table.optimizeSchema()
        console.log(`Expected row size is ${table.estimatedRowSize} bytes`)
        const datapointsFieldMap = await table.createIn(DB, false)
        console.log(datapointsFieldMap)
        Object.assign(datapointsFieldMap, this.entitySets)
        const columns = key.split('$').map(columnName => datapointsFieldMap[columnName] || columnName)
        await table.createIndexesIn(DB)
        await table.setPrimaryIndexTo(columns)
        for (const file in table._files) {
          let indicator = table._files[file]
          indicator = table.fieldMap[indicator] || indicator
          await table.loadCSVFile(indicator, `${dirPath}/${file}`, [...columns, indicator])
        }
        await table.dropPrimaryIndex()
        // TODO: create plain, single column, indexes for elements of the key with a sufficient cardinality, e.g. >= 100;
        // SELECT COUNT(DISTINCT gender) FROM population_age$geo$gender$year_2018122701;
      }
    }
    return this
  }

  async updateFromDirectory (dirPath, incrementally = false) {
    /*
     * Create a new version of this dataset with the data in the given directory.
     *
     */

  }
}

Dataset.remove = (name, version = undefined) => {
  /*
   * Delete ALL tables for the dataset with the given name.
   *
   * If no version is given all the tables for all versions will be deleted.
   */
  console.log(`Deleting tables belonging to ${name}`)
  return DB.query({
    sql: `SHOW TABLES WHERE Tables_in_${DB.name} RLIKE '^${name}';`,
    rowsAsArray: true
  })
  .then(tableNames => {
    console.log(`About to delete ${tableNames.length} tables...`)
    return Promise.all(tableNames.map(tableName => {
      return DB.query(`DROP TABLE ${tableName};`)
      .then(() => console.log(`Deleted ${tableName}`))
    }))
  })
  .then(() => {
    return DB.query(`DELETE FROM datasets WHERE name = '${name}';`)
  })
}
Dataset.all = async () => {
  return DB.query(`SELECT name, version FROM datasets;`)
}

/*
 * Create the necessary tables.
 */
DB.query(`CREATE TABLE datasets (name VARCHAR(100) NOT NULL, version CHAR(10), definition JSON);`)
  .then(() => {
    console.log(`Created new datasets table`)
  })
  .catch(err => {
    if (err.code === 'ER_TABLE_EXISTS_ERROR') {
      console.log(`Datasets table found`)
    } else {
      console.error(err)
      process.exit(1)
    }
  })

Object.assign(exports, {
  Dataset,
  DataSource,
  RecordPrinter
})
