const assert = require('assert')
const Crypto = require('crypto')
const FS = require('fs').promises
const JSONFile = require('jsonfile')
const Moment = require('moment')

const { DB } = require('../maria')
const { Table } = require('../collections')
const { QueryError, QuerySyntaxError, SchemaError } = require('./errors')
const { ArrayStream } = require('./queries')
const CloudStore = require('../cloud-storage')

const Log = require('../log')('datasets')

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

  isInTimeDomain (key) {
    /*
     * Return true if the given key refers to one of the
     * built-in time entity sets, or to the time domain.
     *
     * @param key: a tuple (an array) of DDF concepts.
     */
    return key.length === 1 && ['time', 'year', 'quarter', 'month', 'week', 'day'].includes(key[0])
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

  get tableNames () {
    /*
     * Return a list with the names of all physical tables for this dataset
     */
    const tableNames = []
    for (const kind of ['concepts', 'entities', 'datapoints']) {
      for (const key in this[kind]) {
        const def = this[kind][key]
        if (def && def.table) {
          if (def.table.tables) {
            def.table.tables.forEach(table => {
              const tableName = table.tableName || table.name
              if (tableName) {
                tableNames.push(tableName)
              }
            })
          } else {
            const tableName = def.table.tableName || def.table.name
            if (tableName) {
              tableNames.push(tableName)
            }
          }
        }
      }
    }
    return tableNames
  }

  _querySchema (kind = 'concepts') {
    /*
     * The ddfQuery should have a 'select: {key: ["key", "value"], value: []}'
     */
    const kinds = kind === '*' ? ['concepts', 'entities', 'datapoints'] : [kind]
    const results = []
    kinds.forEach(kindName => {
      const kindDef = this[kindName] || {}
      for (const keyTuple of Object.keys(kindDef)) {
        const key = keyTuple.split('$')
        results.push(...kindDef[keyTuple].values.map(v => {
          return [key, v] // this is a row in the results
        }))
      }
    })
    return results
  }

  queryStream (ddfQuery) {
    const re = /([a-z]+|\*)\.schema/
    const fromClause = re.exec(ddfQuery.from)
    if (!fromClause) {
      throw QuerySyntaxError.WrongFrom(ddfQuery)
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
          domains[entityDef.domain] = { kind: 'entities', key: [entityDef.domain], resources: [], values: [] }
        }
        tableDef = domains[entityDef.domain]
      } else {
        if (!domains[entityName]) {
          domains[entityName] = { kind: 'entities', key: [entityName], resources: [], values: [] }
        }
        tableDef = domains[entityName]
      }
      tableDef.resources.push(...entityDef.resources)
      tableDef.values.push(...entityDef.values.filter(v => !!v))
    }
    Object.values(domains).forEach(def => {
      def.resources = [...new Set(def.resources)]
      def.values = [...new Set(def.values)]
    })
    return Object.values(domains)
  }

  get datapointTableDefinitions () {
    const tables = {}
    const domains = this.domains
    for (const schemaKey in this.datapoints) {
      // Keys in the same domain will be combined, e.g. ['country', 'time'] becomes ['geo', 'time']
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
        tables[tableKey] = { kind: 'datapoints', key: primaryKey, resources: [], entityKeys: [], values: [] }
      }
      tables[tableKey].resources.push(...this.datapoints[schemaKey].resources)
      tables[tableKey].values.push(...this.datapoints[schemaKey].values)
      // tables[tableKey].entityKeys.push(...mappedEntities)
    }
    Object.values(tables).forEach(def => {
      def.entityKeys = [...new Set(def.entityKeys)]
      def.resources = [...new Set(def.resources)] // TODO: find a way to use only the most specific resource
      def.values = [...new Set(def.values)]
    })
    return Object.values(tables)
  }

  _addFilter (filters, filter, tableName = null) {
    /*
     * Retain the MongoDb syntax, but ensure canonical forms
     */
    for (const column in filter) {
      if (['$and', '$or'].includes(column)) {
        // maintain the nesting, i.e. recurse
        const subFilters = filter[column].reduce((subFilters, f) => { // filter.$and MUST be an Array
          this._addFilter(subFilters, f, tableName)
          return subFilters
        }, [])
        if (subFilters.length > 0) {
          filters.push({ [column]: subFilters })
        }
      } else {
        const tableColumn = this.domains[column] || column
        const columnName = tableName ? `${tableName}.${tableColumn}` : tableColumn
        let condition = filter[column] // condition is either an object, or one of boolean, number, string or "variable" like '$geo'
        if (condition === null) { // but every now and then the condition is "null", which should not be allowed
          const whereErr = QuerySyntaxError.WrongWhere()
          whereErr.message = `Invalid where clause ${column ? `for ${column}` : ''}. Condition is "null".`
          throw whereErr
        } else if (typeof condition === 'object') {
          if (Object.keys(condition).length > 1) {
            // make the implicit $and explicit
            filters.push({ $and: Object.keys(condition).reduce((subFilters, operator) => {
              subFilters.push({ [columnName]: { [operator]: condition[operator] } })
              return subFilters
            }, []) })
          } else {
            // TODO: check that property name is an operator, and value is valid for that operator etc.
            filters.push({ [columnName]: condition })
          }
        } else if (typeof condition === 'string' && condition.startsWith('$')) {
          // variable to bind a join, can be ignored as it is not really a filter
          continue
        } else {
          // make the implicit $eq explicit
          filters.push({ [columnName]: { $eq: condition } })
        }
      }
    }
    return filters
  }

  _addJoin (joins, foreignTable, on) {
    /*
     * Add the specified join to the list of joins, but only if the foreign table is not yet in that list.
     *
     * If the foreign table is in the list but with another "on" field raise an error.
     */
    for (const join of joins) {
      if (join.inner.name === foreignTable.name) {
        if (join.on !== on) {
          throw new QueryError(`Second join on '${foreignTable.name}' but with different key: '${on}'`)
        }
        return
      }
    }
    joins.push({ inner: foreignTable, on })
  }

  sqlFor (ddfQuery) {
    if (!this[ddfQuery.from]) {
      throw QueryError.NotSupported()
    }
    // replace key entries that refer to entities to corresponding domains
    const filters = []
    const joins = []
    for (const k of ddfQuery.select.key) {
      const domain = this.domains[k]
      if (domain) {
        if (ddfQuery.from === 'entities') {
          this._addFilter(filters, { [`is--${k.toLowerCase()}`]: { $eq: true } })
        } else {
          const foreignTable = this.tableFor('entities', [domain])
          this._addJoin(joins, foreignTable, domain)
          this._addFilter(filters, { [`is--${k.toLowerCase()}`]: { $eq: true } }, foreignTable.name)
        }
      }
    }
    const key = ddfQuery.select.key.map(k => this.domains[k] || k)
    const table = this.tableFor(ddfQuery.from, key)
    if (!table) {
      throw QueryError.NotSupported()
    }
    const projection = [...key, ...ddfQuery.select.value]

    for (const joinOn in (ddfQuery.join || {})) {
      if (/^\$[_a-z0-9]+/.test(joinOn) !== true) {
        throw QuerySyntaxError.WrongJoin(ddfQuery)
      }
      const joinSpec = ddfQuery.join[joinOn]
      if (!joinSpec) {
        throw QuerySyntaxError.WrongJoin(ddfQuery)
      }
      const joinSpecKey = typeof joinSpec.key === 'string' ? [joinSpec.key] : joinSpec.key
      const foreignTable = this.isInTimeDomain(joinSpecKey) && joinSpecKey.every(p => key.includes(p)) ? table : this.tableFor('entities', joinSpecKey)
      if (!foreignTable) {
        throw QuerySyntaxError.WrongJoin(ddfQuery)
      } else if (foreignTable === table) {
        this._addFilter(filters, joinSpec.where)
      } else {
        this._addFilter(filters, joinSpec.where || {}, foreignTable.name)
        let on = joinOn.slice(1)
        on = this.domains[on] || on
        this._addJoin(joins, foreignTable, on)
      }
    }

    this._addFilter(filters, ddfQuery.where || {})

    const values = [...projection, ...this.definitionFor(ddfQuery.from, key).values]
    const sort = (ddfQuery.order_by || []).reduce((sort, fieldSpec) => {
      if (values.includes(Object.keys(fieldSpec)[0])) {
        sort.push(fieldSpec)
      } else {
        ddfQuery.warn(QueryError.WrongOrderBy(ddfQuery))
      }
      return sort
    }, [])

    return table.sqlFor({ projection, joins, filters, sort, language: ddfQuery.language })
  }

  definitionFor (kind = 'entities', key = []) {
    const tableKey = key.map(k => this.domains[k] || k).sort().join('$')
    return this[kind][tableKey]
  }

  tableFor (kind = 'entities', key = []) {
    const def = this.definitionFor(kind, key)
    const tableOrArgs = def && def.table ? def.table : null
    if (!tableOrArgs) {
      return null
    } else if (tableOrArgs instanceof Table) {
      return tableOrArgs
    } else {
      def.table = Table.specifiedBy(tableOrArgs)
      return def.table
    }
  }

  setTable (kind = 'datapoints', key = [], table) {
    /*
     * Save relevant info about the table for a key
     */
    const canonicalKey = key.join('$')
    if (this[kind] === undefined) {
      this[kind] = {}
    }
    if (this[kind][canonicalKey] === undefined) {
      this[kind][canonicalKey] = {}
    }
    this[kind][canonicalKey].table = table
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
  constructor (name, version, password) {
    this.name = name
    this.version = version
    this._password = password
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
      Log.debug(minorVersion)
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

  get isNew () {
    return this._isNew === true
  }

  async save (publish = false) {
    let conn, sql
    try {
      conn = await DB.getConnection()

      if (publish !== true) {
        await Dataset.ensureDefaultVersion(this.name, conn)
      }

      if (!this.version) {
        this.incrementVersion()
      }
      const doc = this.toJSON()
      let filter = ''
      if (this.isNew) {
        filter = this._keyFields.reduce((flt, field) => {
          flt += ` ${field} = '${doc[field]}',`
          delete doc[field]
          return flt
        }, '')
        if (this.hashedPassword) {
          filter += ` password = '${this.hashedPassword}',`
        }
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
        await conn.query(sql)
      } catch (err) {
        Log.error({ err, sql })
        throw err
      }
      Log.info(`${this.isNew ? 'Inserted' : 'Updated'} dataset ${this.name}.${this.version}`)
      delete this._isNew

      if (publish === true) {
        await this.publish(conn)
      }

      return this
    } catch (err) {
      Log.error(err)
    } finally {
      if (conn.end) conn.end()
    }
  }

  _getCollection (collectionName) {
    return `${this.name}_${collectionName}_${this.version}`
  }

  static async open (name, version = undefined, mustExist = false) {
    let sql = `SELECT name, version, definition, password FROM datasets WHERE name = '${name}'`
    if (version === 'latest') {
      sql += ` ORDER BY imported DESC;`
    } else if (version) {
      sql += ` AND version = '${version}';`
    } else {
      sql += ` ORDER BY is__default DESC, imported DESC;` // if there is no default use the most recently imported version
    }

    let dataset
    const docs = await DB.query(sql)
    const doc = docs && docs.length >= 1 ? docs[0] : undefined
    if (doc) {
      dataset = new this(name, doc.version, doc.password)
      dataset.initialize(JSON.parse(doc.definition))
      Log.debug(`Loaded dataset ${dataset.name}.${dataset.version} from DB`)
      if (dataset._isNew) {
        dataset._isNew = false
      }
    } else if (mustExist) {
      const err = new Error(`Dataset "${name}${version ? `.${version}` : ''}" does not exist.`)
      err.code = 'DDF_DATASET_NOT_FOUND'
      throw err
    } else {
      dataset = new this(name, version === 'latest' ? undefined : version)
      dataset._isNew = true
    }
    return dataset
  }

  get isProtected () {
    return this._password && true
  }

  get hashedPassword () {
    return this._password
  }

  set password (plainTextPassword) {
    this._password = Crypto.createHash('sha256').update(plainTextPassword).digest('hex') // this line is equivalent to the MariaDB function "SHA2(plainTextPassowrd, 256)"
  }

  verifyCredential (credential) {
    assert(credential, 'missing credential')
    const hashedPassword = Crypto.createHash('sha256').update(credential.pass).digest('hex')
    assert(credential.name === this.name && hashedPassword === this.hashedPassword, 'invalid password')
  }

  async queryStream (ddfQuery, abortCheck = () => false, credential) {
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
    if (this.isProtected) {
      try {
        this.verifyCredential(credential)
      } catch (err) {
        Log.debug(err.message)
        const error = new Error('correct password required')
        error.code = 'PASSWORD_REQUIRED'
        throw error
      }
    }

    if (ddfQuery.isForSchema) {
      return this.schema.queryStream(ddfQuery)
    }

    if (this.language && this.language === ddfQuery.language) {
      delete ddfQuery.language // this increases efficiency a bit
    }
    const sql = this.schema.sqlFor(ddfQuery)
    Log.debug(sql)
    const connection = await DB.getConnection()

    // we may have had to wait a long time to get the connection so check if we should abort
    if (abortCheck()) {
      connection.end()
      return null
    }

    return new Promise((resolve, reject) => {
      let resolved = false
      const recordStream = connection.queryStream({ sql, rowsAsArray: true })
      recordStream.cleanUp = (err) => {
        delete recordStream.cleanUp
        Log.debug('Starting to cleanup recordStream')
        if (err) {
          Log.debug(`because ${err.message}`)
        }
        connection.end().then(() => {
          Log.debug(`Connection ${connection.threadId} released.`)
        })
      }
      recordStream.once('end', () => {
        // this happens if there is no data, i.e. the query result is empty
        if (resolved === false) {
          resolved = true
          ddfQuery.info(`DB found 0 records`)
          // need to return a fake stream that is not yet at it's end
          const stream = new ArrayStream([ [] ])
          stream.cleanUp = (err) => recordStream.cleanUp(err)
          resolve(stream)
        }
      })
      recordStream.once('data', data => {
        recordStream.pause()
        recordStream.unshift(data)
        resolved = true
        resolve(recordStream)
      })
      recordStream.on('error', err => {
        resolved = true
        process.nextTick((err) => recordStream.cleanUp(err), err)
        err.sql = sql
        reject(err)
      })
    })
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

  async _createTableFor (ddfTable = { kind: 'datapoints', key: [], resources: [], values: [] }, translations = {}, options = { onlyParse: false, viaTmpTable: false, ignoreNullValues: false }) {
    let table = new Table(this._getCollection(ddfTable.key.join('$')), ddfTable.fieldMap, ddfTable.key)
    const files = ddfTable.resources.reduce((files, resourceName) => {
      const resourceDef = this._resources[resourceName]
      if (!resourceDef) {
        throw new SchemaError(`Resource ${resourceName} is not defined`)
      }
      // key of file could be 'country', but the key for the table is e.g. 'geo'
      files[resourceDef.path] = {
        keyMap: this.schema.fileKeyMapping(ddfTable.key, resourceDef.key),
        values: resourceDef.values
      }
      return files
    }, {})
    for (const file of Object.keys(files)) {
      const fileDetails = files[file]
      await table.updateSchemaFromCSVFile(file, fileDetails.keyMap, options.ignoreNullValues, translations[file])
    }
    table.updateSchemaWithColumns(ddfTable.values)
    Log.info(`Expected row size is ${table.estimatedRowSize} bytes`)
    if (options.onlyParse !== true) {
      for (const entityKey of ddfTable.entityKeys || []) {
        // the table is for multiple keys, that represent entity sets in the domain as indicated by *the* key
        table.addColumn(`is--${entityKey}`, 'BOOLEAN')
        // these columns are filled once the table has been loaded
      }
      try {
        table = await table.createIn(DB)
        await table.setPrimaryIndexTo(ddfTable.key)
        for (const file of Object.keys(files)) {
          const fileDetails = files[file]
          await table.loadCSVFile(file, fileDetails.values, fileDetails.keyMap, translations[file], options.viaTmpTable)
        }
        /*
         * the next section creates joins to update "is--country" columns on a "geo" table
         * However, it was noted that it's at least as fast to do those joins during query processing
         * so the DDFSchema will no longer include entityKeys in the table spec.
         */
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
        Log.error(err)
        process.exit(1)
      } finally {
        table.cleanUp()
      }
    }
    return table
  }

  async _getTranslations (dirPath, dataPackage) {
    /*
     * Return an object with filenames as propterty names that mappings of
     * language id to a corresponding file of translations.
     *
     * For now this is totally dependent on the DDFcsv notion that translation
     * files, and file names, correspond 100% to the files in main data set.
     */
    const translations = {}
    for (const translation of dataPackage.translations || []) {
      const files = await FS.readdir(`${dirPath}/lang/${translation.id}`)
      for (const fileName of files) {
        const path = `${dirPath}/${fileName}`
        if (!translations[path]) {
          translations[path] = {}
        }
        translations[path][translation.id] = `${dirPath}/lang/${translation.id}/${fileName}`
      }
    }
    return translations
  }

  async loadFromDirectory (dirPath, options) {
    if (dirPath.endsWith('/')) {
      dirPath = dirPath.slice(0, -1)
    }

    const dataPackage = await JSONFile.readFile(`${dirPath}/datapackage.json`)
    if (dataPackage.language) {
      this.language = dataPackage.language.id
    }
    this.schema = DDFSchema.fromDDFPackage(dataPackage)

    this._resources = dataPackage.resources.reduce((entries, resource) => {
      entries[resource.name] = {
        path: `${dirPath}/${resource.path}`,
        key: resource.schema.primaryKey,
        values: resource.schema.fields.map(f => f.name).filter(v => resource.schema.primaryKey.includes(v) !== true)
      }
      return entries
    }, {})

    const translations = await this._getTranslations(dirPath, dataPackage)

    const nonDataFileOptions = Object.assign({}, options, { ignoreNullValues: true })
    // 1. Read concepts from file(s) and store in 'concepts' collection.
    const concepts = await this._createTableFor(this.schema.conceptsTableDefinition, translations, nonDataFileOptions)
    // 2. Update the schema with mapping of entity sets to entity domains
    // TODO: check that the domain of an entity set actually refers to a concept of type entity_domain!
    this.schema.ensureDomains(await DB.query(`SELECT concept AS name, domain FROM \`${concepts.tableName}\` WHERE concept_type = 'entity_set';`))
    // 3. Create tables for each entity domain and load all files for that entity domain.
    for (const tableDef of this.schema.domainTableDefinitions) {
      await this._createTableFor(tableDef, translations, nonDataFileOptions)
    }
    // 4. Now create the datapoints tables and then load all the data.
    const datapointTableOptions = Object.assign({ viaTmpTable: true }, options, { ignoreNullValues: false })
    for (const tableDef of this.schema.datapointTableDefinitions) {
      const table = await this._createTableFor(tableDef, translations, datapointTableOptions)
      if (options.onlyParse !== true) {
        await table.dropPrimaryIndex()
        await table.createIndexes()
      }
      table.cleanUp()
    }
    // 5. Import the assets in parallel
    await this.importAssets(dirPath)

    return this
  }

  async importAssets (dirPath) {
    let assets = []
    try {
      assets = await FS.readdir(`${dirPath}/assets`)
    } catch (err) {
      if (err.code === 'ENOENT') { // directory does not exist
        Log.info(`No assets in ${dirPath}`)
      } else {
        throw err
      }
    }
    await Promise.all(assets.map(asset => CloudStore.upload(`${dirPath}/assets/${asset}`, `${this.name}/${this.version}/${asset}`)))
  }

  async protectWith (password, connection = undefined) {
    /*
     * Ensure that this version will be protected by the given password.
     */
    Log.info(`Protecting ${this.name}.${this.version}`)
    let conn
    try {
      conn = connection || await DB.getConnection()
      await conn.query(`
        UPDATE datasets SET password = SHA2(${password}, 256)
        WHERE name = '${this.name} AND version = '${this.version}';`)
      const result = await conn.query(`
        SELECT password FROM datasets 
        WHERE name = '${this.name} AND version = '${this.version}';`)
      this._password = result.password
    } catch (err) {
      Log.error(err)
    } finally {
      if (connection === undefined && conn.end) conn.end()
    }
  }

  async publish (connection = undefined) {
    /*
     * Ensure that this version will be the version that is used by default.
     *
     * If there is another default version for datasets with the same name
     * make this version the new default.
     * If there is no default version and this version is the most recent, i.e. the 'latest',
     * nothing needs to be done.
     */
    Log.info(`Publishing ${this.name}.${this.version}`)
    let conn
    try {
      conn = connection || await DB.getConnection()
      // check if there is already a default, different from this version
      const versions = await conn.query(`
      SELECT name, version, is__default AS isDefault FROM datasets 
      WHERE name = '${this.name}'
      ORDER BY imported DESC;`)
      for (let version of versions) {
        if (version.isDefault) {
          if (version.version !== this.version) {
            await Dataset.makeDefaultVersion(this.name, this.version, conn)
          }
          return this
        }
      }
      // there is no default version, yet.
      if (versions[0].version !== this.version) {
        // this version is not the latest so we should make it the default
        await Dataset.makeDefaultVersion(this.name, this.version, conn)
      }
    } catch (err) {
      Log.error(err)
    } finally {
      if (connection === undefined && conn.end) conn.end()
    }
  }

  async urlForAsset (asset, secure = false) {
    return CloudStore.urlFor(`${this.name}/${this.version}/${asset}`, secure)
  }

  get tableNames () {
    /*
     * Return a list of table names as used by this dataset. These are the names as used in the DB!
     */
    return this.schema.tableNames
  }

  static async remove (name, version = 'latest', connection = undefined) {
    /*
    * Delete ALL tables for the dataset with the given name.
    *
    * If the version is given as '_ALL_', the tables for all versions will be deleted!
    * If no version is given the most recently loaded version will be deleted, unless it
    * is marked as "default."
    * The version parameter can also be a list of versions.
    */
    let conn
    const filters = [`name = '${name}'`]
    try {
      conn = connection || await DB.getConnection()
      let msg = `Deleting tables belonging to ${name}`
      if (Array.isArray(version)) {
        filters.push(`version IN (${version.map(v => `'${v}'`).join(', ')})`)
        msg += `.${version[0]}${version.length > 1 ? version.slice(1).forEach(v => `, ${name}.${v}`) : ''}`
      } else {
        if (version && version.toUpperCase() === '_ALL_') {
          msg += ` (ALL versions)`
          version = null
        } else if (version === 'latest') {
          // get the latest version
          const versions = await conn.query(`
            SELECT version, is__default AS isDefault FROM datasets 
            WHERE name = '${name}'
            ORDER BY imported DESC;`)
          if (versions.length < 1) {
            // nothing to delete
            return await this.all(name, conn)
          }
          if (versions.length > 0 && versions[0].isDefault) {
            throw new Error(`Won't delete the default version for ${name}`)
          }
          version = versions[0].version
        }
        if (version) {
          filters.push(`version = '${version}'`)
          msg += `.${version}`
        }
      }
      Log.info(msg)
      const datasets = await conn.query({
        sql: `SELECT name, version, definition FROM datasets WHERE ${filters.join(' AND ')};`
      })
      const tableNames = datasets.reduce((names, dsRecord) => {
        const ds = new Dataset(dsRecord.name, dsRecord.version)
        ds.initialize(JSON.parse(dsRecord.definition))
        names.push(...ds.tableNames)
        return names
      }, [])
      Log.info(`About to delete ${tableNames.length} tables...`)
      await Promise.all(tableNames.map(tableName => {
        return conn.query(`DROP TABLE \`${tableName}\`;`)
          .then(() => Log.info(`Deleted ${tableName}`))
          .catch(err => {
            if (err.code !== 'ER_BAD_TABLE_ERROR') { // bad table means it doesn't exist
              throw (err)
            }
          })
      }))
      // TODO: remove assets from cloud storage ?
      await conn.query(`DELETE FROM datasets WHERE ${filters.join(' AND ')};`)
      return await this.all(name, conn)
    } catch (err) {
      console.info(err.message)
      Log.warn(err)
    } finally {
      if (connection === undefined && conn.end) conn.end()
    }
  }

  static async makeDefaultVersion (name, version, connection = undefined) {
    let conn = connection
    try {
      conn = conn || await DB.getConnection()
      const versions = await conn.query(`
      SELECT name, version, is__default AS isDefault FROM datasets 
      WHERE name = '${name}' ORDER BY imported DESC;`)
      if (versions.length < 1) {
        throw new Error(`Dataset ${name} does not exist.`)
      }
      if (version !== 'latest' && versions.filter(v => v.version === version).length < 1) {
        throw new Error(`Version ${name}.${version} does not exist.`)
      }
      // Unset any default version
      await conn.query(`UPDATE datasets SET is__default = FALSE WHERE name = '${name}' AND is__default = TRUE;`)
      if (version === 'latest') {
        // Nothing else to do, i.e. there will be no explicit default.
      } else {
        // Set the given version to be the default
        await conn.query(`UPDATE datasets SET is__default = TRUE WHERE name = '${name}' AND version = '${version}';`)
        const defaultVersions = await conn.query(`
          SELECT name, version FROM datasets 
          WHERE name = '${name}' AND is__default = TRUE;`)
        if (defaultVersions.length !== 1 || defaultVersions[0].version !== version) {
          throw new Error(`Default version for ${name} could not be set to ${version}! Check database!`)
        }
        Log.info(`Default version for ${defaultVersions[0].name} is now ${defaultVersions[0].version}`)
      }
      return await this.all(name, conn)
    } catch (err) {
      console.info(err.message)
      Log.warn(err)
    } finally {
      if (connection === undefined && conn.end) conn.end()
    }
  }

  static async ensureDefaultVersion (name, connection = undefined) {
    let conn = connection
    try {
      conn = conn || await DB.getConnection()
      // check for existing default
      const versions = await conn.query(`
        SELECT name, version, is__default AS isDefault FROM datasets 
        WHERE name = '${name}'
        ORDER BY imported DESC;`)
      for (let version of versions) {
        if (version.isDefault) {
          // no need to do anything, as there is a default version
          return
        }
      }
      if (versions.length < 1) {
        return
      }
      await this.makeDefaultVersion(name, versions[0].version, connection)
    } catch (err) {
      console.info(err.message)
      Log.warn(err)
    } finally {
      if (connection === undefined && conn.end) conn.end()
    }
  }

  static async purge (name) {
    /*
     * Delete old versions of datasets with the given name.
     *
     * The default (or latest) version,
     * the version preceding that one,
     * and any version newer than that will be retained.
     *
     * Returns the updated list of versions.
     */
    let conn
    try {
      conn = await DB.getConnection()
      let versionsToDelete = []
      let allVersions = await conn.query(`
        SELECT version, is__default AS isDefault FROM datasets
        WHERE name = '${name}'
        ORDER BY imported DESC;`)
      let defaultVersion
      while (allVersions.length > 0) {
        const version = allVersions.shift()
        if (version.isDefault) {
          defaultVersion = version
          versionsToDelete = []
          allVersions.shift()
        } else {
          versionsToDelete.push(version.version)
        }
      }
      if (!defaultVersion) {
        // never delete the most recent two versions
        versionsToDelete = versionsToDelete.slice(2)
      }
      if (versionsToDelete.length > 0) {
        await this.remove(name, versionsToDelete, conn)
      }
      return await this.all(name, conn)
    } catch (err) {
      Log.error(err)
    } finally {
      if (conn.end) conn.end()
    }
  }

  static async all (name = undefined, connection = undefined) {
    const filter = name ? ` WHERE name = '${name}'` : ''
    return (connection || DB).query(`SELECT name, version, is__default FROM datasets${filter} ORDER BY name ASC, imported DESC;`)
  }
}

/*
 * Create the necessary table(s).
 *
 * The length of 'version' is 40 char too allow for a full git hash
 */
DB.query(`CREATE TABLE datasets (
    name VARCHAR(255) NOT NULL, 
    version VARCHAR(40), 
    is__default BOOLEAN DEFAULT FALSE, definition JSON,
    imported DATETIME DEFAULT CURRENT_TIMESTAMP,
    password VARCHAR(80) DEFAULT NULL);`)
  .then(() => { // TODO: would be cool to have a CONSTRAINT that would ensure only one version of a dataset can be marked as default
    Log.info(`Created new datasets table`)
  })
  .catch(err => {
    if (err.code === 'ER_TABLE_EXISTS_ERROR') {
      Log.debug(`Datasets table found`)
    } else if (err.code === 'ER_TABLEACCESS_DENIED_ERROR') {
      Log.info('This node does not have write access to DB')
    } else {
      Log.error(err)
      process.exit(1)
    }
  })

module.exports = { Dataset }
