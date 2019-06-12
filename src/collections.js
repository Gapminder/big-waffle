const Crypto = require('crypto')
const FileSystem = require('fs')
const { Writable } = require('stream')

const CSVParser = require('csv-parse')
const firstline = require('firstline')
const { sample, sampleSize } = require('lodash')

const Log = require('./log')('collections')
const { MaxColumns } = require('./env')

const wait = ms => new Promise(resolve => setTimeout(resolve, ms)) // helper generator for wait promises

function quoted (arrayOrIdentifier, qualifier = null) {
  /*
   * Return the given identifier, or array of identifers, backtick quoted.
   * This ensures that the identifier(s) can be used for a table name, column name, index, etc.
   *
   * If a qualifier (string) is provided prefix all identifiers with the
   * backtick quoted qualifier and a dot.
   */
  const prefix = qualifier ? `\`${qualifier}\`.` : ''
  if (arrayOrIdentifier instanceof Array) {
    return arrayOrIdentifier.map(identifier => `${prefix}\`${identifier}\``)
  } else {
    return `${prefix}\`${arrayOrIdentifier}\``
  }
}

function sqlSafe (value, quoteResultIfNeeded = false) {
  if (typeof value !== 'string') {
    return value
  }
  // eslint-disable-next-line no-control-regex
  const regex = /[\0\x08\x09\x1a\n\r'\\%]/g
  const matches = ['\\0', '\\x08', '\\x09', '\\x1a', '\\n', '\\r', "'", '"', '\\', '\\\\', '%']
  const replacements = ['\\\\0', '\\\\b', '\\\\t', '\\\\z', '\\\\n', '\\\\r', "''", '""', '\\\\', '\\\\\\\\', '\\%']
  const safeValue = value.replace(regex, (char) => replacements[matches.indexOf(char)])
  if (quoteResultIfNeeded) {
    if (safeValue === 'TRUE' || safeValue === 'FALSE') {
      return safeValue
    }
    return `'${safeValue}'`
  }
  return safeValue
}

function _convertOperand (operand) {
  if (typeof operand === 'string') {
    return `'${operand}'`
  } else if (operand === null || operand === undefined) {
    return `NULL`
  } else if (operand === true) {
    return `TRUE`
  } else if (operand === false) {
    return `FALSE`
  }
  return operand
}

const Conditions = {
  $eq: (col, operand) => {
    if (operand === true) {
      return `${col} IS TRUE`
    } else if (operand === false) {
      return `${col} IS FALSE`
    } else {
      return `${col} <=> ${_convertOperand(operand)}` // NULL <=> NULL is true! (https://mariadb.com/kb/en/library/null-safe-equal/)
    }
  },
  $gt: (col, operand) => `${col} > ${_convertOperand(operand)}`,
  $gte: (col, operand) => `${col} >= ${_convertOperand(operand)}`,
  $lt: (col, operand) => `${col} < ${_convertOperand(operand)}`,
  $lte: (col, operand) => `${col} <= ${_convertOperand(operand)}`,
  $ne: (col, operand) => {
    if (operand === true) {
      return `${col} IS NOT TRUE`
    } else if (operand === false) {
      return `${col} IS NOT FALSE`
    } else {
      return `! (${col} <=> ${_convertOperand(operand)})` // NULL <=> NULL is true! (https://mariadb.com/kb/en/library/null-safe-equal/)
    }
  },
  $in: (col, list) => `${col} IN (${list.map(item => _convertOperand(item)).join(', ')})`,
  $nin: (col, list) => `${col} NOT IN (${list.map(item => _convertOperand(item)).join(', ')})`
}

class RecordProcessor extends Writable {
  constructor (aTable, processorFunction, args = [], highWatermark = 100) {
    super({ objectMode: true, highWatermark })
    this.table = aTable
    this.processorFunction = processorFunction
    this.processorArgs = args
  }

  _writev (chunks, callback) {
    try {
      const promises = []
      for (let chunk of chunks) {
        const result = this.processorFunction.call(this.table, chunk.chunk, ...this.processorArgs) // ok to ignore chunk.encoding
        if (result instanceof Promise) {
          promises.push(result)
        }
      }
      if (promises.length > 0) {
        Promise.all(promises).then(results => callback(null, { processed: chunks.length }))
      } else {
        callback(null, { processed: chunks.length })
      }
    } catch (err) {
      callback(err)
    }
  }

  _write (record, encoding, callback) {
    try {
      const result = this.processorFunction.call(this.table, record, ...this.processorArgs) // ok to ignore chunk.encoding
      if (result instanceof Promise) {
        result.then(value => callback(null, { processed: 1 }))
      } else {
        callback(null, { processed: 1 })
      }
    } catch (err) {
      callback(err)
    }
  }
}

class Collection {
  constructor (nameOrObject) {
    this.name = nameOrObject.name || nameOrObject
    this.keys = new Set()
  }

  _sqlForFilter (filter, foreignTables = {}, language = undefined) {
    /*
     * Return the SQL for one (complex) filter (expression).
     *
     * Filters can be nested in that case this function will recurse.
     */
    const clauses = []
    for (const column in filter) {
      if (['$and', '$or'].includes(column)) { // TODO: add $not and $nor ?
        const subClauses = []
        for (const subFilter of filter[column]) {
          subClauses.push(this._sqlForFilter(subFilter, foreignTables, language))
        }
        clauses.push(`(${subClauses.join(` ${column.slice(1).toUpperCase()}`)})`)
      } else {
        let qualifiedColumnName = column.split('.')
        if (qualifiedColumnName.length > 1) {
          const foreignTable = foreignTables[qualifiedColumnName[0]]
          qualifiedColumnName = foreignTable._qualified(qualifiedColumnName[1], language)
        } else {
          qualifiedColumnName = this._qualified(column, language)
        }
        for (const operator in filter[column]) {
          clauses.push(Conditions[operator](qualifiedColumnName, filter[column][operator]))
        }
      }
      return clauses.join(` AND`)
    }
  }

  cleanUp () {
    if (this._connection) {
      this._connection.end()
      delete this._connection
    }
  }

  async getConnection (database) {
    if (database) {
      this._database = database
    }
    if (this._connection) {
      return this._connection
    }
    this._connection = await this._database.getConnection()
    return this._connection
  }

  async tableForCSVFile (path, keyMap = {}, delimiter = ',') {
    /* Create a (temporary) table for the CSV file, using the CONNECT db engine.
     *
     * The CSV table needs column definitions and those are taken from the
     * schema of this table. There is no check in this method for a proper schema!
     *
     * CREATE TABLE aid_given_percent_of_gni (geo CHAR(3) NOT NULL, time INT NOT NULL,
     * aid_given_percent_of_gni DOUBLE)
     * engine=CONNECT table_type=CSV file_name='/Users/robert/Projects/Gapminder/ddf--gapminder--systema_globalis/ddf--datapoints--aid_given_percent_of_gni--by--geo--time.csv'
     * header=1 sep_char=',' quoted=1;
     */
    const csvTableName = `TT${Crypto.createHash('md5').update(path).digest('hex')}`
    const csvHeader = await firstline(path)
    const csvColumns = this._columns(csvHeader.split(delimiter))
    const columnDefs = csvColumns.reduce((statement, columnName) => {
      const def = this._schema[keyMap[columnName] || columnName]
      let type = def.sqlType
      if (type === `JSON`) { // JSON type is not supported for CSV files
        type = `VARCHAR`
      }
      if (type === `VARCHAR`) {
        type = `VARCHAR(${def.size})`
      }
      if (['TINYINT', 'INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT'].includes(type)) {
        type = `${type} NOT NULL`
      }
      return `${statement} \`${columnName}\` ${type},`
    }, '')
    let sql = `CREATE TABLE ${csvTableName} (${columnDefs.slice(0, -1)}) 
    engine=CONNECT table_type=CSV file_name='${path}' header=1 sep_char='${delimiter}' quoted=1;`
    Log.debug(sql)
    const conn = await this.getConnection()
    await conn.query(sql)
    return csvTableName
  }

  toJSON () {
    /*
     * Return a plain object that represents this collection
     * and can be serialized.
     */
    const doc = {}
    if (this._tableName) {
      doc.tableName = this.tableName
    }
    for (const key in this) {
      if ((key.startsWith('_') || ['keys'].includes(key)) === false) {
        doc[key] = this[key]
      }
    }
    return doc
  }
}

class Table extends Collection {
  /*
   * A Collection that is implemented as table for a relational database.
   */
  constructor (nameOrObject, mappedColumns = {}, keys = []) {
    super(nameOrObject)
    this._schema = {}
    if (nameOrObject.name) {
      Object.assign(this, nameOrObject)
    }
    if (this.name && this.name.length > 64 && !this._tableName) {
      // the name of the table in the DB can be only 64 chars!
      this._tableName = `${this.name.slice(0, 24)}${Crypto.createHash('md5').update(this.name).digest('hex').slice(0, 40)}`
    }
    if (mappedColumns) {
      this._columnNames = Object.assign({}, mappedColumns)
    }
    if (keys && keys.length > 0) {
      this.keys = new Set(keys)
    }
  }

  _column (schemaName, language) {
    if (language) {
      const translatedColumn = this._column(`${schemaName}--${language}`)
      if (this._schema[translatedColumn]) {
        return translatedColumn
      }
    }
    return this._columnNames[schemaName] || schemaName
  }

  _columns (arrayOfSchemaNames, language) {
    return arrayOfSchemaNames.map(n => this._column(n, language))
  }

  _qualified (colNameorArray, language) {
    return typeof colNameorArray === 'string'
      ? quoted(this._column(colNameorArray, language), this.tableName)
      : colNameorArray.map(n => quoted(this._column(n, language), this.tableName))
  }

  get tableName () {
    return this._tableName || this.name
  }

  set tableName (aStringOfMax64Chars) {
    if (!this._tableName && (typeof aStringOfMax64Chars === 'string' && aStringOfMax64Chars.length <= 64)) {
      this._tableName = aStringOfMax64Chars
    }
  }

  get mappedColumns () {
    // Return a copy of the column name mapping
    return Object.assign({}, this._columnNames)
  }

  set mappedColumns (obj) {
    this._columnNames = obj
  }

  get auxillaryColumns () {
    // Return a list with names of the columns that were added during loading, e.g. to support translations
    return Object.keys(this._schema).filter(column => {
      const def = this._schema[column]
      return def.virtual === true || def.auxillary === true
    })
  }

  set auxillaryColumns (columnNames) {
    columnNames.forEach(column => {
      if (!this._schema[column]) {
        this._schema[column] = { auxillary: true }
      } else {
        this._schema[column].auxillary = true
      }
    })
  }

  toJSON () {
    const doc = super.toJSON()
    doc.auxillaryColumns = this.auxillaryColumns
    doc.mappedColumns = this.mappedColumns
    return doc
  }

  estimatedColumnSize (colName) {
    let fieldDef = this._schema[colName]
    if (fieldDef.sqlType === `VARCHAR`) {
      let charSize = 1.1
      if (/^_.+--[a-z]{2}-[a-z]{2}$/i.test(colName)) { // most likely a translated column
        charSize = 2.2
      }
      return Math.round((fieldDef.size) * charSize + 2)
    } else if (['TINYINT', 'INTEGER', 'BIGINT', 'FLOAT'].includes(fieldDef.sqlType)) {
      return 4
    } else if (fieldDef.sqlType === `DOUBLE`) {
      return 8
    } else if (fieldDef.sqlType === `BOOLEAN`) {
      return 4
    } else if ([`JSON`, `TEXT`, `BLOB`].includes(fieldDef.sqlType)) {
      return 10
    } else if (fieldDef.virtual) {
      return 0
    } else {
      Log.warn(`Cannot estimate size for column \`${colName}\``)
      return 0
    }
  }

  get estimatedRowSize () {
    let size = 0
    for (const colName of Object.keys(this._schema)) {
      size += this.estimatedColumnSize(colName)
    }
    return size
  }

  optimizeSchema () {
    // check if it makes sense to pivot one or more of the key columns
    //    let pivots = {}
    //    for (const col of key.split('$')) {
    //      const def = table._schema[col]
    //      if (def.cardinality < 200 && table.estimatedRowSize + (def.cardinality * table.estimatedColumnSize(col)))
    //    }

    for (const def of Object.values(this._schema)) {
      if (def.uniques) {
        delete def.uniques
      }
    }
  }

  async createIndexes (columnsOrMinimumCardinality = 150, database = undefined) {
    const columns = []
    if (typeof columnsOrMinimumCardinality === 'number') {
      Object.keys(this._schema).forEach(columnName => {
        const def = this._schema[columnName]
        if (def.cardinality && def.cardinality >= columnsOrMinimumCardinality) {
          columns.push(columnName)
        }
      })
    } else if (columnsOrMinimumCardinality.forEach) {
      columnsOrMinimumCardinality.forEach(colName => {
        columns.push(this._column(colName))
      })
    }

    if (!columns.length) return

    const sql = `ALTER TABLE \`${this.tableName}\`${columns.map(columnName => ` ADD INDEX IF NOT EXISTS (\`${columnName}\`)`).join(',')};`
    Log.debug(sql)
    Log.info(`Creating indexes on ${columns.join(',')}. This may take a while!`)
    const conn = await this.getConnection(database)
    Log.info(`Created indexes on ${columns.join(',')}`)
    await conn.query(sql)
  }

  async setPrimaryIndexTo (columns, database = undefined) {
    const mappedColumns = this._columns(columns)
    let sql = `ALTER TABLE \`${this.tableName}\` ADD PRIMARY KEY (${quoted(mappedColumns).join(' ,')});`
    Log.debug(sql)
    const conn = await this.getConnection(database)
    await conn.query(sql)
    this.keys = new Set([...this.keys, ...mappedColumns])
    return this
  }

  async dropPrimaryIndex (database = undefined) {
    let sql = `ALTER TABLE \`${this.tableName}\` DROP PRIMARY KEY;`
    const conn = await this.getConnection(database)
    await conn.query(sql)
    return this
  }

  updateSchemaWithColumns (columnNames = []) {
    /*
     * Ensure that the schema has a valid definition for each of
     * the given column names.
     *
     * When the schema was created by scanning datafiles there may be
     * columns for which no data was encountered. For these columns
     * 1 byte TINYINT columns will be added.
     */
    const table = this
    columnNames.forEach(columnName => {
      let def = this._schema[columnName]
      if (!def) {
        def = {}
        this._schema[columnName] = def
        Log.warn(`DDF schema declared spurious value '${columnName}' for ${table.name}`)
      }
      if (!def.virtual && !def.sqlType) {
        def.sqlType = 'TINYINT'
      }
    })
  }

  async createIn (database, withIndexes = true) {
    if (Object.keys(this._schema).length > MaxColumns) {
      const wideTable = WideTable.split(this)
      await wideTable.createIn(database, withIndexes)
      return wideTable
    }

    // shorten too long columnNames and save mapping
    const _columnNames = this._columnNames
    for (const columnName of Object.keys(this._schema)) {
      if (columnName.length > 64) {
        const newColumnName = columnName.slice(0, 55) + sample('abcdefghijkmnopqrstuvwxyz') + sampleSize('abcdefghijkmnopqrstuvwxyz23456789', 5).join('')
        _columnNames[columnName] = newColumnName
        this._schema[newColumnName] = this._schema[columnName]
        delete this._schema[columnName]
      }
    }
    let sql = `CREATE OR REPLACE TABLE \`${this.tableName}\` (`
    sql = Object.keys(this._schema).reduce((statement, columnName) => {
      const def = this._schema[columnName]
      let type = def.sqlType
      let virtual = ''
      if (def.virtual) {
        type = this._schema[this._column(def.value)].sqlType
        def.size = Math.max(this._schema[this._column(def.value)].size || 0, this._schema[this._column(def.fallback)].size || 0)
        virtual = ` AS (IFNULL(\`${this._column(def.value)}\`, \`${this._column(def.fallback)}\`)) VIRTUAL`
      }
      if (type === `VARCHAR`) {
        type = `VARCHAR(${def.size})`
      }
      return `${statement} \`${columnName}\` ${type}${virtual}${withIndexes ? def.index || '' : ''},`
    }, sql)
    sql = `${sql.slice(0, -1)});`
    Log.debug(sql)
    const conn = await this.getConnection(database)
    try {
      await conn.query(sql)
    } catch (err) {
      if (['ER_TOO_MANY_FIELDS', 'ER_TOO_BIG_ROWSIZE'].includes(err.code)) {
        // split the table
        const wideTable = WideTable.split(this)
        await wideTable.createIn(database, withIndexes)
        return wideTable
      } else {
        throw err
      }
    }
    return this
  }

  sqlFor (query = { language: undefined, projection: [], joins: [], filters: [], sort: [] }) {
    const language = query.language
    const columns = this._qualified(query.projection, language).join(', ')
    const innerJoin = query.joins && query.joins.length > 0
      ? query.joins.reduce((sql, join) => {
        sql += `\nINNER JOIN \`${join.inner.tableName}\` ON ${this._qualified(join.on)}=${join.inner._qualified(join.on)}`
        return sql
      }, ` `)
      : ''
    const foreignTables = (query.joins || []).reduce((tables, j) => {
      tables[j.inner.name] = j.inner
      return tables
    }, {})
    const where = query.filters && query.filters.length > 0
      ? `\nWHERE ${query.filters.map(filter => this._sqlForFilter(filter, foreignTables, language)).join(' AND')}`
      : ''
    const order = query.sort && query.sort.length > 0
      ? `\nORDER BY ${query.sort.map(f => {
        const spec = Object.entries(f)[0] // there should only be one entry
        return `${this._qualified(spec[0], language)} ${spec[1]}`
      }).join('AND ')}`
      : ''
    return `SELECT ${columns} FROM \`${this.tableName}\`${innerJoin}${where}${order};`
  }

  _prepareRecord (record, _columnNames = {}) {
    const preparedRecord = Object.assign({}, record)
    for (const field of Object.keys(record)) {
      if (preparedRecord[field] === '' || preparedRecord[field] === undefined || preparedRecord[field] === null) {
        delete preparedRecord[field]
        continue
      }
      if (_columnNames[field]) {
        preparedRecord[_columnNames[field]] = record[field]
        delete preparedRecord[field]
      }
    }
    return preparedRecord
  }

  _updateRecord (record, keyMap, language) {
    /*
     * Return a promise!
     */
    try {
      const preparedRecord = this._prepareRecord(record, keyMap)
      const filter = {}
      const sets = {}
      for (const field of Object.keys(preparedRecord)) {
        if (this.keys.has(field)) {
          filter[field] = preparedRecord[field]
          if ((filter[field] === undefined || filter[field] === null) && this._schema[field].index.match(/PRIMARY/g)) {
            throw new Error(`Record misses value for primary key ${field}\n${JSON.stringify(record)}`)
          }
        } else {
          const columnName = language ? `_${field}--${language}` : field
          sets[columnName] = preparedRecord[field]
        }
      }

      if (language && Object.keys(sets).length < 1) {
        // this is an empty translation record
        return
      }

      let condition = Object.keys(filter).reduce((sql, keyName) => {
        return `${sql}\`${keyName}\`=${sqlSafe(filter[keyName], true)} AND `
      }, ``)
      condition = condition.slice(0, -5)
      let updates = Object.keys(sets).reduce((sql, columnName) => {
        return `${sql}\`${columnName}\`=${sqlSafe(sets[columnName], true)}, `
      }, ``)
      updates = updates.slice(0, -2)
      let values = Object.keys(filter).reduce((sql, columnName) => {
        return `${sql}\`${columnName}\`=${sqlSafe(filter[columnName], true)}, `
      }, ``)
      values = `${values}${updates}`
      if (values.endsWith(', ')) {
        values = values.slice(0, -2)
      }
      let sql
      if (language) {
        sql = `UPDATE \`${this.tableName}\` SET ${updates} WHERE ${condition};`
      } else {
        sql = Object.keys(sets).length > 0 ? `
BEGIN NOT ATOMIC
SELECT COUNT(*) FROM \`${this.tableName}\` WHERE ${condition} INTO @recordExists;
IF @recordExists > 0
THEN
  UPDATE \`${this.tableName}\` SET ${updates} WHERE ${condition};
ELSE
  INSERT INTO \`${this.tableName}\` SET ${values};
END IF;
END;`
          : `
BEGIN NOT ATOMIC
SELECT COUNT(*) FROM \`${this.tableName}\` WHERE ${condition} INTO @recordExists;
IF @recordExists < 1 THEN
  INSERT INTO \`${this.tableName}\` SET ${values};
END IF;
END;`
      }
      return this.getConnection()
        .then(connection => {
          return connection.query(sql)
        })
        .catch(err => {
          if (err.code === 'ER_LOCK_DEADLOCK') {
            Log.info('deadlock occured going to retry in 500 ms')
            return wait(500).then(() => this._connection.query(sql))
          }
          Log.error({ err, sql })
          if (this._connection) {
            this._connection.end()
            delete this._connection
          }
          throw err
        })
    } catch (err) {
      Log.error(err)
      return err
    }
  }

  loadFromCSVFile (path, keyMap = {}, translations = {}, delimiter = ',') {
    /*
     * Parse all records in the CSV file and insert into this SQL table.
     *
     * The file may contain a subset of the final set of columns for this table.
     *
     * Returns a Promise that resolves to this Table.
     */
    const mappedColumnNames = Object.assign({}, this.mappedColumnNames, keyMap)
    return this._pipeCSVFile(path, this._updateRecord, [mappedColumnNames], delimiter, 5)
      .then(thisTable => {
        return Promise.all(Object.keys(translations).map(language => {
          return this._pipeCSVFile(translations[language], this._updateRecord, [mappedColumnNames, language], delimiter, 5)
        }))
      })
      .then(() => {
        return this
      })
  }

  async loadCSVFile (path, columns = [], keyMap = {}, translations = {}, viaTmpTable = false, delimiter = ',') {
    /*
     * Parse all records in the CSV file and insert into this SQL table.
     *
     * The file may contain a subset of the final set of columns for this table.
     * The provided list of (non-key) columns must exist in both the CSV file
     * as well as already in this table.
     *
     * If translations are given load the translations too.
     *
     * Returns a Promise that resolves to this Table.
     */
    if (viaTmpTable !== true || Object.keys(translations).length > 0) {
      return this.loadFromCSVFile(path, keyMap, translations, delimiter) // slower, but can handle large cells
    }
    // 1. Create a (temporary) table for the CSV file, using the CONNECT db engine.
    const tmpTableName = await this.tableForCSVFile(path, keyMap, delimiter = ',')
    // 2. Copy all records
    await this.copyValues(columns, tmpTableName)
    // 3. Delete the temporary table
    const conn = await this.getConnection()
    await conn.query(`DROP TABLE \`${tmpTableName}\``)
    Log.info(`Finished loading ${path} into ${this.tableName}`)
    return this
  }

  async copyValues (columns, tableName) {
    /* Copy the given columns from the given table into this table.
     *
     * INSERT INTO dpstest (geo, time, aid_percent)
     *   SELECT * FROM aid_given_percent_of_gni
     *   ON DUPLICATE KEY UPDATE dpstest.aid_percent = aid_given_percent_of_gni.aid_given_percent_of_gni;
     */
    let sql
    const values = this._columns(columns.filter(c => this._schema[c]))
    const keysAndValues = [...this._columns(Array.from(this.keys)), ...values]
    if (values.length > 0) {
      const updates = values.map(v => `\`${this.tableName}\`.\`${v}\`=\`${tableName}\`.\`${v}\``)
      sql = `INSERT INTO \`${this.tableName}\` (${quoted(keysAndValues).join(', ')})
      SELECT ${quoted(keysAndValues).join(', ')} FROM ${tableName}
      ON DUPLICATE KEY UPDATE ${updates.join(', ')};`
    } else {
      sql = `INSERT IGNORE INTO \`${this.tableName}\` (${quoted(keysAndValues).join(', ')})
      SELECT ${quoted(keysAndValues).join(', ')} FROM ${tableName};`
    }
    Log.debug(sql)
    const conn = await this.getConnection()
    await conn.query(sql)
  }

  addColumn (nameOrObject, sqlType = 'BOOLEAN', size) {
    const name = nameOrObject.name || nameOrObject
    if (this._schema[name]) {
      throw new Error(`Table ${this.name} already has a '${name}' column!`)
    }
    const def = { sqlType }
    if (size) {
      def.size = size
      // TODO: adjust type based on size
    }
    if (nameOrObject.sqlType) {
      Object.assign(def, nameOrObject)
      if (def.name) delete def.name
    }
    this._schema[name] = def
  }

  async updateFromJoin (foreignTable, sharedColumn, columns) {
    const updates = columns.map(c => `\`${this.tableName}\`.\`${this._column(c)}\` = \`${foreignTable.tableName}\`.\`${foreignTable._column(c)}\``)
    const sql = `
    UPDATE \`${this.tableName}\`
    LEFT JOIN \`${foreignTable.tableName}\` 
      ON \`${this.tableName}\`.\`${this._column(sharedColumn)}\` = \`${foreignTable.tableName}\`.\`${foreignTable._column(sharedColumn)}\` 
    SET ${updates.join(', ')}`
    Log.debug(sql)
    const conn = await this.getConnection()
    await conn.query(sql)
    return this
  }

  _updateSchemaWith (record, keyMap = {}, language) {
    /*
     * Update the schema with the data from this record.
     */
    const preparedRecord = this._prepareRecord(record, keyMap)
    for (const column of Object.keys(preparedRecord)) {
      const columnInKey = this.keys.has(column)
      const columnName = language ? (columnInKey ? column : `_${column}--${language}`) : column
      let def = this._schema[columnName]
      if (!def) {
        def = columnInKey ? { uniques: new Set(), count: 0 } : {}
        if (language) {
          /*
           * Add another column, which will be virtual,
           * that takes its values from the language specific column if not null
           * and otherwise from the main, untranslated, column
           */
          this._schema[`${column}--${language}`] = {
            virtual: true,
            value: columnName,
            fallback: column
          }
        }
        this._schema[columnName] = def
      }
      const typicalValue = preparedRecord[column]

      // update cardinality estimate for columns in the key
      if (def.uniques && (def.cardinality === undefined || def.cardinality < 201)) {
        def.uniques.add(typicalValue)
        def.cardinality = def.uniques.size
        if (def.cardinality > 200) {
          delete def.uniques
        }
      }

      if (typeof typicalValue === 'number') {
        if (Number.isInteger(typicalValue)) {
          if (!def.sqlType || def.sqlType === `INTEGER`) {
            def.sqlType = typicalValue > 2147483647 ? `BIGINT` : `INTEGER`
          }
        } else if (Number.isInteger(typicalValue) === false && def.sqlType !== `VARCHAR`) {
          def.sqlType = `DOUBLE`
        }
      } else if (typeof typicalValue === 'boolean' && !def.sqlType) {
        def.sqlType = `BOOLEAN`
      } else if (typeof typicalValue === 'string') {
        def.size = Math.max(sqlSafe(typicalValue).length + 1, def.size || 1)
        if ((!def.sqlType || def.sqlType === `BOOLEAN`) &&
          (columnName.startsWith('is--') || typicalValue.toUpperCase() === 'TRUE' || typicalValue.toUpperCase() === 'FALSE')) {
          def.sqlType = `BOOLEAN`
        } else if (typicalValue.match(/^[{[]{1}/)) {
          if (def.size > 100) {
            def.sqlType = `JSON`
          } else {
            def.sqlType = `VARCHAR`
          }
        } else if (def.size > 2000) {
          def.sqlType = `TEXT`
        } else if (def.sqlType !== `TEXT`) {
          def.sqlType = `VARCHAR`
        }
      }
    }
  }

  _pipeCSVFile (path, processingMethod, args = [], delimiter = ',', highWatermark) {
    const table = this
    return new Promise((resolve, reject) => {
      try {
        const schemaComputer = new RecordProcessor(table, processingMethod, args, highWatermark)
        schemaComputer.on('finish', () => {
          Log.info(`Processed ${path} for ${table.name}`)
          resolve(table)
        })
        schemaComputer.on('error', err => reject(err))

        const parser = CSVParser({
          delimiter: delimiter,
          cast: true,
          columns: true,
          trim: true
        })
        parser.on('error', err => reject(err))

        const csvFile = FileSystem.createReadStream(path)
        Log.info(`Processing ${path} for ${table.name}`)
        Log.debug(`using mapping: ${JSON.stringify(this._columnNames)}`)
        csvFile.pipe(parser).pipe(schemaComputer)
      } catch (err) {
        reject(err)
      }
    })
  }

  updateSchemaFromCSVFile (path, keyMap = {}, translations = {}) {
    /*
     * Scan all records in the CSV file to find the optimal schema for each column in the file.
     *
     * The file may contain a subset of the final set of columns for this table.
     *
     * The translations, if provided, are a mapping of language ids to file paths
     * that should have translations for some of the values for this table.
     * For each such file, i.e. for each translated language, additional columns to
     * hold the translated values will be created.
     *
     * Returns a Promise that resolves to this Table.
     */
    const mappedColumnNames = Object.assign({}, this.mappedColumnNames, keyMap)
    return this._pipeCSVFile(path, this._updateSchemaWith, [mappedColumnNames])
      .then(thisTable => {
        return Promise.all(Object.keys(translations).map(language => {
          return this._pipeCSVFile(translations[language], this._updateSchemaWith, [mappedColumnNames, language])
        }))
      })
      .then(() => {
        return this
      })
  }

  get schema () {
    return Object.entries(this._schema || {}).map(propValue => {
      propValue[1].name = propValue[0]
      return propValue[1]
    })
  }

  static specifiedBy (spec) {
    return spec.tables ? new WideTable(spec) : new this(spec)
  }
}
Table.MaxRowSize = 8000 // NOTE: it is difficult to accurately estimate the row size

class WideTable extends Collection {
  constructor (specification) {
    const preparedSpec = Object.assign({}, specification)
    preparedSpec.keys = new Set(specification.keys || [])
    super(preparedSpec.name)
    this.keys = new Set(preparedSpec.keys) // keys first, as they may be needed when other properties are set
    Object.assign(this, preparedSpec)
  }

  get tables () {
    return this._tables
  }

  set tables (listOfTables = []) {
    this._tables = listOfTables.map(table => table instanceof Table ? table : Table.specifiedBy(table))
    this._schema = {}
    const keys = Array.from(this.keys)
    this._tables.forEach(table => {
      if (!table.values) {
        table.values = table.schema.map(colDef => colDef.name).filter(colName => keys.includes(colName) !== true)
      }
      Object.assign(this._schema, table._schema)
    })
  }

  _columns (arrayOfSchemaNames, language) {
    return this.tables.reduce((cols, table) => {
      return table._columns(cols, language)
    }, arrayOfSchemaNames)
  }

  _qualifiedCol (colName, language) {
    let i = 0
    if (this.keys.has(colName) === false) {
      for (i = 0; i < this._tables.length; i++) {
        if (this._tables[i].values.includes(colName)) break
      }
    }
    const table = this._tables[i]
    return quoted(table._column(colName, language), table.tableName)
  }

  _qualified (colNameorArray, language) {
    return typeof colNameorArray === 'string'
      ? this._qualifiedCol(colNameorArray)
      : colNameorArray.map(col => this._qualifiedCol(col, language))
  }

  _tableFor (column) {
    for (let table of this.tables) {
      if (table.values.includes(column)) {
        return table
      }
    }
    return undefined
  }

  sqlFor (query = { language: undefined, projection: [], joins: [], filters: [], sort: [] }) {
    // if only one value is needed there's no need to join the tables of this wide table
    const values = query.projection.filter(c => this.keys.has(c) === false)
    if (values.length === 1) {
      return this._tableFor(values[0]).sqlFor(query)
    }

    const language = query.language
    const columns = this._qualified(query.projection, language).join(', ')
    const innerJoin = query.joins && query.joins.length > 0
      ? query.joins.reduce((sql, join) => {
        sql += `\nINNER JOIN \`${join.inner.tableName}\` ON ${this._qualified(join.on)}=${join.inner._qualified(join.on)}`
        return sql
      }, ` `)
      : ''
    const foreignTables = (query.joins || []).reduce((tables, j) => {
      tables[j.inner.name] = j.inner
      return tables
    }, {})
    const where = query.filters && query.filters.length > 0
      ? `\nWHERE ${query.filters.map(filter => this._sqlForFilter(filter, foreignTables, language)).join(' AND')}`
      : ''
    const order = query.sort && query.sort.length > 0
      ? `\nORDER BY ${query.sort.map(f => {
        const spec = Object.entries(f)[0] // there should only be one entry
        return `${this._qualified(spec[0], language)} ${spec[1]}`
      }).join('AND ')}`
      : ''
    const jointTable = this.tables.slice(1).reduce((jSQL, table) => {
      const onSQL = Array.from(this.keys).map(keyCol => {
        return `${this.tables[0]._qualified(keyCol)}=${table._qualified(keyCol)}`
      }).join(' AND ')
      return `${jSQL}\n  JOIN \`${table.tableName}\` ON ${onSQL}`
    }, `\`${this.tables[0].tableName}\``)
    return `SELECT ${columns} FROM ${jointTable}${innerJoin}${where}${order};`
  }

  async createIn (database, withIndexes = true) {
    if (database) {
      this._database = database
    }
    this.tables = await Promise.all(this.tables.map(table => table.createIn(database, withIndexes)))
    return this
  }

  async loadCSVFile (path, columns = [], keyMap = {}, translations = {}, viaTmpTable = false, delimiter = ',') {
    /*
     * Each table that is part of this wide table should be updated with the key columns in the CSV file
     * and tables that actually manage any of the given columns should be updated with those columns
     * in the CSV file.
     */
    const tmpTableName = await this.tableForCSVFile(path, keyMap, delimiter)
    await Promise.all(this.tables.map(async table => {
      await table.copyValues(columns, tmpTableName)
      Log.info(`Finished loading ${path} into ${table.tableName}`)
    }))
    const conn = await this.getConnection()
    await conn.query(`DROP TABLE \`${tmpTableName}\``)
  }

  cleanUp () {
    super.cleanUp()
    this.tables.forEach(table => table.cleanUp())
  }

  async createIndexes (columnsOrMinimumCardinality = 150, database = undefined) {
    await Promise.all(this.tables.map(table => table.createIndexes(columnsOrMinimumCardinality, database)))
  }

  async dropPrimaryIndex (database = undefined, force = false) {
    /*
     * Unlike for a normal table a wdie table should retain the primary indexes
     * on its tables to allow for fast joins of those tables.
     */
    if (force) {
      await Promise.all(this.tables.map(table => table.dropPrimaryIndex(database)))
    }
  }

  async setPrimaryIndexTo (columns, database = undefined) {
    await Promise.all(this.tables.map(table => table.setPrimaryIndexTo(columns, database)))
  }

  toJSON () {
    /*
     * Return a plain object that represents this (wide) table.
     */
    const doc = super.toJSON()
    doc.keys = Array.from(this.keys) // it's convenient to keep the keys for the _qualified method
    doc.tables = this.tables.map(table => table.toJSON())
    return doc
  }

  static split (aTable) {
    const spec = {
      name: aTable.name,
      keys: aTable.keys,
      tables: []
    }

    const keyDefs = Array.from(aTable.keys).map(k => {
      const def = { name: k }
      Object.assign(def, aTable._schema[k])
      return def
    })

    const allMappedColumns = aTable.mappedColumns

    function newTable () {
      const mappedColumns = {}
      const table = new Table(`${spec.name}_${WideTable.Suffixes[spec.tables.length]}`, mappedColumns, aTable.keys)
      table._nrColumns = 0
      table._rowSize = 0
      keyDefs.forEach(keyDef => {
        table.addColumn(keyDef)
        if (allMappedColumns[keyDef.name]) {
          mappedColumns[keyDef.name] = allMappedColumns[keyDef.name]
        }
        table._nrColumns++
        table._rowSize += table.estimatedColumnSize(keyDef.name)
      })
      return table
    }

    let table = newTable()
    spec.tables.push(table)
    aTable.schema.forEach(colDef => {
      if (aTable.keys.has(colDef.name) === false) {
        // TODO: add virtual columns that are dependent on this column with this column
        if (table._nrColumns >= MaxColumns - 1 || table._rowSize + aTable.estimatedColumnSize(colDef.name) >= Table.MaxRowSize) {
          table = newTable()
          spec.tables.push(table)
        }
        table.addColumn(colDef)
        if (allMappedColumns[colDef.name]) {
          table._mappedColumns[colDef.name] = allMappedColumns[colDef.name]
        }
        table._nrColumns++
        table._rowSize += table.estimatedColumnSize(colDef.name)
      }
    })
    return new this(spec)
  }
}
WideTable.Suffixes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']

Object.assign(exports, {
  Table,
  setWideTableThreshold: Table.setWideTableThreshold // used in testing
})
