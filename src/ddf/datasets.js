const JSONFile = require('jsonfile')
const Moment = require('moment')

const { DB } = require('../maria')
const { Table } = require('../collections')
const { QueryError, SchemaError } = require('./errors')
const { ArrayStream } = require('./queries')

class DDFSchema {
  constructor (obj) {
    this.concepts = {}
    this.entities = {}
    this.datapoints = {}
    if (obj) {
      Object.assign(this, obj)
      if (obj.domains) { // this should be done last
        this.domains = obj.domains
      }
    }
  }

  toJSON () {
    /*
     * Exclude (list of) resources and change Table instances to plain objects
     */
    const obj = { domains: Object.assign({}, this.domains) }
    for (const kind of ['concepts', 'entities', 'datapoints']) {
      obj[kind] = {}
      for (const key in this[kind]) {
        obj[kind][key] = {}
        const ddfTableDef = this[kind][key]
        for (const param in ddfTableDef) {
          if (['resources'].includes(param)) continue
          else if (typeof ddfTableDef[param].toJSON === 'function') {
            obj[kind][key][param] = ddfTableDef[param].toJSON()
          } else {
            obj[kind][key][param] = ddfTableDef[param]
          }
        }
      }
    }
    return obj
  }

  _querySchema (kind = 'concepts') {
    /*
     * The ddfQuery should have a 'select: {key: ["key", "value"], value: []}'
     */
    const kindDef = this[kind] || {}
    const results = []
    for (const keyTuple of Object.keys(kindDef)) {
      const key = keyTuple.split('$')
      results.push(...kindDef[keyTuple].values.map(v => {
        return [key, v] // this is a row in the results
      }))
    }
    return results
  }

  queryStream (ddfQuery) {
    const re = /([a-z]+)\.schema/
    const fromClause = re.exec(ddfQuery.from)
    if (!fromClause) {
      throw QueryError.WrongFrom(ddfQuery)
    }
    return new ArrayStream(this._querySchema(fromClause[1]))
  }

  get domains () {
    /*
     * Return an Object that maps entity set names to domain names
     */
    if (this._domains) {
      return this._domains
    }
    const domains = {}
    for (const entityName in this.entities) {
      const domain = this.entities[entityName].domain
      if (domain) {
        domains[entityName] = domain
      }
    }
    this._domains = domains
    return domains
  }

  set domains (map) {
    // Re-compute the domain mapping
    this._domains = undefined
    return this.domains
  }

  ensureDomains (entitySets) {
    if (!this._domains) {
      this._domains = {} // map entity sets to domains
    }
    for (const { name, domain } of entitySets) {
      if (!(domain && name)) {
        continue
      }
      const entitySet = this.entities[name]
      if (!entitySet) {
        continue
      }
      if (entitySet.domain && entitySet.domain !== domain) {
        throw new SchemaError(`Entity set ${name} can only have one domain!`)
      }
      entitySet.domain = domain
      this._domains[name] = domain
    }
  }

  get conceptsTableDefinition () {
    const tables = Object.keys(this.concepts).map(key => {
      return Object.assign({ kind: 'concepts', key: key.split('$') }, this.concepts[key])
    })
    // there should be only one table (i.e. key) for concepts
    tables[0].resources = [...new Set(tables[0].resources || [])]
    return tables[0]
  }

  fileKeyMapping (keys = [], fileKey) {
    const fileKeys = typeof fileKey === 'string' ? [fileKey] : fileKey // fileKey can be a string or an Array
    if (!this._domains) {
      return {}
    }
    if (keys.length !== fileKeys.length) {
      throw new SchemaError(`File primaryKey ${fileKey} has wrong number of fields for ${keys}`)
    }
    const mapping = fileKeys.reduce((mapping, fKey) => {
      const mappedKey = this._domains[fKey]
      if (mappedKey && mappedKey !== fKey) {
        mapping[fKey] = mappedKey
      }
      return mapping
    }, {})
    return mapping
  }

  get domainTableDefinitions () {
    const domains = {}
    for (const entityName in this.entities) {
      let tableDef
      const entityDef = this.entities[entityName]
      if (entityDef.domain) {
        if (!domains[entityDef.domain]) {
          domains[entityDef.domain] = { kind: 'entities', key: [entityDef.domain], resources: [] }
        }
        tableDef = domains[entityDef.domain]
      } else {
        if (!domains[entityName]) {
          domains[entityName] = { kind: 'entities', key: [entityName], resources: [] }
        }
        tableDef = domains[entityName]
      }
      tableDef.resources.push(...entityDef.resources)
    }
    Object.values(domains).forEach(def => {
      def.resources = [...new Set(def.resources)]
    })
    return Object.values(domains)
  }

  get datapointTableDefinitions () {
    const tables = {}
    const domains = this.domains
    for (const schemaKey in this.datapoints) {
      /* Keys in the same domain will be combined, e.g. ['country', 'time'] becomes ['geo', 'time']
       * and then the domain wide table will be enhanced with a column to filter for the entity set
       * e.g. "is__country"
       */
      const primaryKey = []
      const mappedEntities = []
      for (const k of schemaKey.split('$')) {
        if (domains[k]) {
          primaryKey.push(domains[k])
          mappedEntities.push(k)
        } else {
          primaryKey.push(k)
        }
      }
      const tableKey = primaryKey.sort().join('$')
      if (!tables[tableKey]) {
        tables[tableKey] = { kind: 'datapoints', key: primaryKey, resources: [], entityKeys: [] }
      }
      tables[tableKey].resources.push(...this.datapoints[schemaKey].resources)
      tables[tableKey].entityKeys.push(...mappedEntities)
    }
    Object.values(tables).forEach(def => {
      def.entityKeys = [...new Set(def.entityKeys)]
      def.resources = [...new Set(def.resources)] // TODO: find a way to use only the most specific resource
    })
    return Object.values(tables)
  }

  sqlFor (ddfQuery) {
    if (!this[ddfQuery.from]) {
      throw QueryError.NotSupported()
    }
    // replace key entries that refer to entities to corresponding domains
    const entityKeys = {}
    const filters = []
    for (const k of ddfQuery.select.key) {
      const domain = this.domains[k]
      if (domain) {
        entityKeys[k] = domain // e.g. "country" => "geo"
        filters.push({ [`is--${k.toLowerCase()}`]: 'IS TRUE' }) // this is standard SQL so ok to have here
      }
    }
    const key = ddfQuery.select.key.map(k => entityKeys[k] || k)
    const table = this.tableFor(ddfQuery.from, key)
    if (!table) {
      throw QueryError.NotSupported()
    }
    const projection = [...key, ...ddfQuery.select.value]
    // TODO: build join(s)
    const joins = []
    const sort = (ddfQuery.order_by || [])
    return table.sqlFor({ projection, joins, filters, sort })
  }

  tableFor (kind = 'entities', key = []) {
    const tableKey = key.map(k => this.domains[k] || k).sort().join('$')
    const def = this[kind][tableKey]
    const tableOrArgs = def && def.table ? def.table : null
    if (!tableOrArgs) {
      return null
    } else if (tableOrArgs instanceof Table) {
      return tableOrArgs
    } else {
      def.table = new Table(tableOrArgs)
      return def.table
    }
  }

  setTable (kind = 'datapoints', key = [], table) {
    /*
     * Save relevant info about the table for a key
     */
    this[kind][key.join('$')].table = table
  }

  static fromDDFPackage (packageJSON) {
    /*
     * Return a new DDFSchema instance with the parsed schema of the given Object.
     */
    if (!packageJSON.ddfSchema) {
      throw SchemaError.MissingSchema()
    }
    const ddfSchema = new this()
    for (const kind of Object.keys(packageJSON.ddfSchema)) {
      /*
       * "kind" is one of concepts, entities, datapoints, or synonyms
       */
      const kindDef = ddfSchema[kind] = {}
      for (const valueDef of packageJSON.ddfSchema[kind]) {
        /*
         * "valueDef" is an object with a primaryKey (array), a value and possibly a resource
         */
        const key = valueDef.primaryKey.sort().join('$')
        if (!kindDef[key]) {
          kindDef[key] = { values: [], resources: [] }
        }
        kindDef[key].values.push(valueDef.value)
        kindDef[key].resources.push(...valueDef.resources)
      }
      for (const key of Object.keys(kindDef)) {
        // remove duplicate resources (likely) and values (unlikely!)
        kindDef[key].values = [...new Set(kindDef[key].values)].sort()
        kindDef[key].resources = [...new Set(kindDef[key].resources)].sort()
      }
    }
    return ddfSchema
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
   * A Dataset exposes either the latest version or a named version.
   */
  constructor (name, version) {
    this.name = name
    this.version = version
  }

  toJSON () {
    /*
     * Return a plain object that represents this dataset and
     * can be saved in a collection.
     */
    const doc = {}
    for (const key in this) {
      if (key.startsWith('_')) {
        continue
      } else if (typeof this[key].toJSON === 'function') {
        doc[key] = this[key].toJSON()
      } else {
        doc[key] = this[key]
      }
    }
    return doc
  }

  initialize (doc) {
    Object.assign(this, doc)
    if (doc.schema) {
      this.schema = new DDFSchema(doc.schema)
    }
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
      console.log(sql)
      console.error(dbErr)
    }
    return this
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
      } else {
        sql += ` ORDER BY is__default DESC, version DESC`
      }

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

  async queryStream (ddfQuery, abortCheck = () => false, start = undefined) {
    /*
     * response for {select: {key: ['key', 'value'], value: []}, from: 'concepts.schema'} should be an array with the column names
     * of the concepts. Like [{key: ['concept'], value: 'color'}, {key: ['concept'], value: 'concept_type'}, ....]
     *
     * Likewise for {select: {key: ['key', 'value'], value: []}, from: 'entities.schema'} should be an array with the column names
     * for the different entity sets: [{key: ['country'], value: 'gwid'}, {key: ['geo'], value: 'gwid'}, ...]
     *
     * And for {select: {key: ['key', 'value'], value: []}, from: 'datapoints.schema'} should be an array with all the possible
     * key tuple <> indicator mappings: [{key: ['country', 'time'], value: 'gini'}, {key: ['geo', 'time'], value: 'gini'}, ...]
     *
     * There will also be queries like {select: {key: ['concept'], values: ['color', 'concept_type', ..]}, from: 'concept'} i.e.
     * like SELECT key+values FROM concepts. The response should be an array of objects: [{color: .., concept_type: ...}]
     * Similarly {select: {key: ['geo'], values: ['landlocked', 'main_religion', ..]}, from: 'entities'}
     *
     * Time values need to be parsed by the reader/client, the service just returns strings.
     */
    if (ddfQuery.isForSchema) {
      return this.schema.queryStream(ddfQuery)
    }

    const sql = this.schema.sqlFor(ddfQuery)
    const connection = await DB.getConnection()

    // we may have had to wait a long time to get the connection so check if we should abort
    if (abortCheck()) {
      connection.end()
      return null
    }

    return new Promise((resolve, reject) => {
      const recordStream = connection.queryStream({ sql, rowsAsArray: true })
      recordStream.cleanUp = (err) => {
        delete recordStream.cleanUp
        console.log('Starting to cleanup recordStream')
        if (err) {
          console.log(`because ${err.message}`)
        }
        connection.end().then(() => {
          console.log(`Connection ${connection.threadId} released.`)
        })
      }
      recordStream.once('data', data => {
        recordStream.pause()
        recordStream.unshift(data)
        resolve(recordStream)
      })
      recordStream.on('error', err => {
        process.nextTick((err) => recordStream.cleanUp(err), err)
        reject(err)
      })
    })
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

  async _createTableFor (ddfTable = { kind: 'datapoints', key: [], resources: [] }, options = { onlyParse: false, viaTmpTable: false }) {
    const table = new Table(this._getCollection(ddfTable.key.join('$')), ddfTable.fieldMap)
    const files = ddfTable.resources.reduce((files, resourceName) => {
      const resourceDef = this._resources[resourceName]
      if (!resourceDef) {
        throw new SchemaError(`Resource ${resourceName} is not defined`)
      }
      // key of file could be 'country', but the key for the table is e.g. 'geo'
      files[resourceDef.path] = this.schema.fileKeyMapping(ddfTable.key, resourceDef.key)
      return files
    }, {})
    for (const file of Object.keys(files)) {
      await table.updateSchemaFromCSVFile(file, files[file])
    }
    console.log(`Expected row size is ${table.estimatedRowSize} bytes`)
    if (options.onlyParse !== true) {
      for (const entityKey of ddfTable.entityKeys || []) {
        // the table is for multiple keys, that represent entity sets in the domain as indicated by *the* key
        table.addColumn(`is--${entityKey}`, 'BOOLEAN')
        // these columns are filled once the table has been loaded
      }
      try {
        await table.createIn(DB)
        await table.setPrimaryIndexTo(ddfTable.key)
        for (const file of Object.keys(files)) {
          await table.loadCSVFile(file, files[file], options.viaTmpTable)
        }
        const joins = (ddfTable.entityKeys || []).reduce((joins, entityKey) => {
          const domain = this.schema.domains[entityKey]
          if (!joins[domain]) {
            joins[domain] = {
              domain,
              entityTable: this.schema.tableFor('entities', [domain]), // e.g. the "geo" table}
              columns: []
            }
          }
          joins[domain].columns.push(`is--${entityKey}`)
          return joins
        }, {})
        for (const join of Object.values(joins)) {
          await table.updateFromJoin(join.entityTable, join.domain, join.columns)
        }
        this.schema.setTable(ddfTable.kind, ddfTable.key, table)
      } catch (err) {
        console.error(err)
        process.exit(1)
      } finally {
        table.cleanUp()
      }
    }
    return table
  }

  async loadFromDirectory (dirPath, options) {
    if (dirPath.endsWith('/')) {
      dirPath = dirPath.slice(0, -1)
    }

    const dataPackage = await JSONFile.readFile(`${dirPath}/datapackage.json`)
    this.schema = DDFSchema.fromDDFPackage(dataPackage)

    this._resources = dataPackage.resources.reduce((entries, resource) => {
      entries[resource.name] = { path: `${dirPath}/${resource.path}`, key: resource.schema.primaryKey }
      return entries
    }, {})

    // 1. Read concepts from file(s) and store in 'concepts' collection.
    const concepts = await this._createTableFor(this.schema.conceptsTableDefinition)
    // 2. Update the schema with mapping of entity sets to entity domains
    // TODO: check that the domain of an entity set actually refers to a concept of type entity_domain!
    this.schema.ensureDomains(await DB.query(`SELECT concept AS name, domain FROM ${concepts.name} WHERE concept_type = 'entity_set';`))
    // 3. Create tables for each entity domain and load all files for that entity domain.
    for (const tableDef of this.schema.domainTableDefinitions) {
      await this._createTableFor(tableDef, options)
    }
    // 4. Now create the datapoints tables and then load all the data.
    const datapointTableOptions = Object.assign({ viaTmpTable: true }, options)
    for (const tableDef of this.schema.datapointTableDefinitions) {
      const table = await this._createTableFor(tableDef, datapointTableOptions)
      await table.dropPrimaryIndex()
      // TODO: create plain, single column, indexes for elements of the key with a sufficient cardinality, e.g. >= 100;
      // SELECT COUNT(DISTINCT gender) FROM population_age$geo$gender$year_2018122701;
      table.cleanUp()
    }
    return this
  }

  async updateFromDirectory (dirPath, incrementally = false) {
    /*
     * Create a new version of this dataset with the data in the given directory.
     *
     */

  }

  static async remove (name, version = undefined) {
    /*
    * Delete ALL tables for the dataset with the given name.
    *
    * If the version is given as 'all', the tables for all versions will be deleted!
    * If no version is given the default version will be deleted. And if no default
    * version exists, nothing will be deleted.
    */
    let conn, tableRegEx
    const filter = [`name = '${name}'`]
    try {
      conn = await DB.getConnection()
      let msg = `Deleting tables belonging to ${name}`
      if (version && version.toLowerCase() === 'all') {
        tableRegEx = `^${name}`
        msg += ` (ALL versions)`
      } else if (version === undefined) {
        // get the default version
        const defaultVersions = await conn.query(`SELECT version FROM datasets WHERE name = '${name}' AND is__default IS TRUE`)
        if (defaultVersions.length !== 1) {
          throw new Error(`Could not determine default version for ${name}`)
        }
        version = defaultVersions[0].version
      }
      if (version) {
        filter.push(`version = '${version}'`)
        tableRegEx = `^${name}.+${version}$`
        msg += `.${version}`
      }
      console.log(msg)
      const tableNames = conn.query({
        sql: `SHOW TABLES WHERE Tables_in_${DB.name} RLIKE '${tableRegEx}';`,
        rowsAsArray: true
      })
      console.log(`About to delete ${tableNames.length} tables...`)
      await Promise.all(tableNames.map(tableName => {
        return conn.query(`DROP TABLE \`${tableName}\`;`)
          .then(() => console.log(`Deleted ${tableName}`))
      }))
      await conn.query(`DELETE FROM datasets WHERE ${filter.join(' AND')};`)
    } catch (err) {
      console.error(err)
    } finally {
      if (conn.end) conn.end()
    }
  }

  static async makeDefaultVersion (name, version) {
    let conn
    try {
      conn = await DB.getConnection()
      // Unset any default version
      await conn.query(`UPDATE datasets SET is__default = FALSE WHERE name = '${name}';`)
      // Set the given version to be the default
      await conn.query(`UPDATE datasets SET is__default = TRUE WHERE name = '${name}' AND version = '${version}';`)
      const defaultVersions = await conn.query(`
        SELECT name, version FROM datasets 
        WHERE name = '${name}' AND version = '${version}';`)
      if (defaultVersions.length !== 1) {
        throw new Error(`Default version for ${name} could not be set to ${version}! Check database!`)
      }
      console.log(`Default version for ${defaultVersions[0].name} is now ${defaultVersions[0].version}`)
    } catch (err) {
      console.error(err)
    } finally {
      if (conn.end) conn.end()
    }
  }

  static async all (name = undefined) {
    const filter = name ? ` WHERE name = '${name}'` : ''
    return DB.query(`SELECT name, version, is__default FROM datasets${filter} ORDER BY name ASC, is__default DESC, version DESC;`)
  }
}

/*
 * Create the necessary table(s).
 */
// TODO: save record creation dates
DB.query(`CREATE TABLE datasets (name VARCHAR(100) NOT NULL, version CHAR(10), is__default BOOLEAN DEFAULT FALSE, definition JSON);`)
  .then(() => { // TODO: would be cool to have a CONSTRAINT that would ensure only one version of a dataset can be marked as default
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

module.exports = { Dataset }
