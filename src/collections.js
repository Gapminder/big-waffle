const Crypto = require('crypto')
const FileSystem = require('fs')
const { Writable } = require('stream')

const CSVParser = require('csv-parse')
const firstline = require('firstline')
const { sample, sampleSize } = require('lodash')

const Log = require('./log')('collections')

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

const Conditions = {
  $eq: (col, operand) => {
    if (operand === true) {
      return `${col} IS TRUE`
    } else if (operand === false) {
      return `${col} IS FALSE`
    } else if (typeof operand === 'string') {
      return `${col} = '${operand}'`
    } else {
      return `${col} = ${operand}`
    }
  },
  $lt: (col, operand) => `${col} < ${operand}`,
  $gt: (col, operand) => `${col} > ${operand}`
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
}

class Table extends Collection {
  /*
   * A Collection that is implemented as table for a relational database.
   */
  constructor (nameOrObject, mappedColumns = {}, keys = []) {
    super(nameOrObject)
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
    this._schema = {}
  }

  _column (schemaName) {
    return this._columnNames[schemaName] || schemaName
  }

  _columns (arrayOfSchemaNames) {
    return arrayOfSchemaNames.map(n => this._column(n))
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

  toJSON () {
    /*
     * Return a plain object that represents this dataset and
     * can be saved in a collection.
     */
    const doc = { mappedColumns: this.mappedColumns }
    if (this._tableName) {
      doc.tableName = this.tableName
    }
    for (const key in this) {
      if (key.startsWith('_') === false) {
        doc[key] = this[key]
      }
    }
    return doc
  }

  estimatedColumnSize (colName) {
    let fieldDef = this._schema[colName]
    if (fieldDef.sqlType === `VARCHAR`) {
      return (fieldDef.size) * 2 + 2
    } else if (fieldDef.sqlType === `INTEGER`) {
      return 2
    } else if (fieldDef.sqlType === `DOUBLE`) {
      return 8
    } else if (fieldDef.sqlType === `BOOLEAN`) {
      return 1
    } else if ([`JSON`, `TEXT`, `BLOB`].includes(fieldDef.sqlType)) {
      return 0
    } else if (fieldDef.virtual) {
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

  indexOn (columnName, primary = false, unique = false) {
    let columnDefinition = this._schema[columnName]
    if (!columnDefinition) {
      columnDefinition = { uniques: new Set() }
      this._schema[columnName] = columnDefinition
    }
    columnDefinition.index = `${primary ? ' PRIMARY' : (unique ? ' UNIQUE' : '')} KEY`
    this.keys.add(columnName)
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

  primaryIndexOn (columnName) {
    return this.indexOn(columnName, true)
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

  async createIn (database, withIndexes = true) {
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
    await conn.query(sql)
  }

  async createIndexesIn (database) {
    for (const columnName of Object.keys(this._schema)) {
      const def = this._schema[columnName]
      if (def.index) {
        const sql = `CREATE INDEX \`${columnName}_idx ON\` \`${this.tableName}\` (\`${columnName}\`);`
        Log.debug(sql)
        await (database || this._database).query(sql)
      }
    }
  }

  _sqlForFilter (filter, foreignTables = {}) {
    /*
     * Add the SQL for one filter to the given sql.
     *
     * Filters can be nested in that case this function will recurse
     */
    const clauses = []
    for (const column in filter) {
      if (['$and', '$or'].includes(column)) {
        const subClauses = []
        for (const subFilter of filter[column]) {
          subClauses.push(this._sqlForFilter(subFilter, foreignTables))
        }
        clauses.push(`(${subClauses.join(` ${column.slice(1).toUpperCase()}`)})`)
      } else {
        let qualifiedColumnName = column.split('.')
        if (qualifiedColumnName.length > 1) {
          const foreignTable = foreignTables[qualifiedColumnName[0]]
          qualifiedColumnName = quoted(foreignTable._column(qualifiedColumnName[1]), foreignTable.tableName)
        } else {
          qualifiedColumnName = quoted(this._column(column), this.tableName)
        }
        for (const operator in filter[column]) {
          clauses.push(Conditions[operator](qualifiedColumnName, filter[column][operator]))
        }
      }
      return clauses.join(` AND`)
    }
  }

  sqlFor (query = { projection: [], joins: [], filters: [], sort: [] }) {
    const columns = quoted(this._columns(query.projection), this.tableName).join(', ')
    const innerJoin = query.joins && query.joins.length > 0
      ? query.joins.reduce((sql, join) => {
        sql += `\nINNER JOIN \`${join.inner.tableName}\` ON ${quoted(this._column(join.on), this.tableName)}=${quoted(join.inner._column(join.on), join.inner.tableName)}`
        return sql
      }, ` `)
      : ''
    const foreignTables = (query.joins || []).reduce((tables, j) => {
      tables[j.inner.name] = j.inner
      return tables
    }, {})
    const where = query.filters && query.filters.length > 0
      ? `\nWHERE ${query.filters.map(filter => this._sqlForFilter(filter, foreignTables)).join(' AND')}`
      : ''
    const order = query.sort && query.sort.length > 0
      ? `\nORDER BY ${query.sort.map(f => {
        const spec = Object.entries(f)[0] // there should only be one entry
        return `\`${this._column(spec[0])}\` ${spec[1]}`
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

  cleanUp () {
    if (this._connection) {
      this._connection.end()
      delete this._connection
    }
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
      /*
       * The 'values' part of the SQL is going to be wrong in case of a language (translation)
       * but translation records should point to existing entries, so that part of the SQL
       * will never be used!
       */
      let values = Object.keys(preparedRecord).reduce((sql, columnName) => {
        return `${sql}\`${columnName}\`=${sqlSafe(preparedRecord[columnName], true)}, `
      }, ``)
      values = values.slice(0, -2)
      const sql = Object.keys(sets).length > 0 ? `
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

  async loadCSVFile (path, keyMap = {}, translations = {}, viaTmpTable = false, separator = ',') {
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
    if (viaTmpTable !== true) {
      return this.loadFromCSVFile(path, keyMap, translations, separator) // slower, but can handle large cells
    }
    // 1. Create a (temporary) table for the CSV file, using the CONNECT db engine.
    /* CREATE TABLE aid_given_percent_of_gni (geo CHAR(3) NOT NULL, time INT NOT NULL,
     * aid_given_percent_of_gni DOUBLE)
     * engine=CONNECT table_type=CSV file_name='/Users/robert/Projects/Gapminder/ddf--gapminder--systema_globalis/ddf--datapoints--aid_given_percent_of_gni--by--geo--time.csv'
     * header=1 sep_char=',' quoted=1;
     */
    const tmpTableName = `TT${Crypto.createHash('md5').update(path).digest('hex')}`
    const csvHeader = await firstline(path)
    const csvColumns = this._columns(csvHeader.split(separator))
    const tableColumns = csvColumns.map(c => keyMap[c] || c)
    const columnDefs = csvColumns.reduce((statement, columnName) => {
      const def = this._schema[keyMap[columnName] || columnName]
      let type = def.sqlType
      if (type === `JSON`) { // JSON type is not supported for CSV files
        type = `VARCHAR`
      }
      if (type === `VARCHAR`) {
        type = `VARCHAR(${def.size})`
      }
      return `${statement} \`${columnName}\` ${type},`
    }, '')
    let sql = `CREATE TABLE ${tmpTableName} (${columnDefs.slice(0, -1)}) 
    engine=CONNECT table_type=CSV file_name='${path}' header=1 sep_char='${separator}' quoted=1;`
    Log.debug(sql)
    const conn = await this.getConnection()
    await conn.query(sql)
    // 2. Copy all records
    /* INSERT INTO dpstest (geo, time, aid_percent)
      *   SELECT * FROM aid_given_percent_of_gni
      *   ON DUPLICATE KEY UPDATE dpstest.aid_percent = aid_given_percent_of_gni.aid_given_percent_of_gni;
      */
    const updates = tableColumns.filter(c => !this.keys.has(c)).map(c => `\`${this.tableName}\`.\`${c}\` = ${tmpTableName}.\`${c}\``)
    sql = `
    INSERT INTO \`${this.tableName}\` (${quoted(tableColumns).join(', ')})
      SELECT ${quoted(csvColumns).join(', ')} FROM ${tmpTableName}
      ON DUPLICATE KEY UPDATE ${updates.join(', ')};`
    Log.debug(sql)
    await conn.query(sql)
    // 3. Delete the temporary table
    sql = `DROP TABLE ${tmpTableName}`
    Log.debug(sql)
    await conn.query(sql)
    Log.info(`Finished loading ${path} into ${this.tableName}`)
    return this
  }

  addColumn (name, sqlType = 'BOOLEAN', size) {
    if (this._schema[name]) {
      throw new Error(`Table ${this.name} already has a '${name}' column!`)
    }
    this._schema[name] = { sqlType }
    if (size) {
      this._schema[name].size = size
      // TODO: adjust type based on size
    }
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
      const columnName = language ? (this.keys.has(column) ? column : `_${column}--${language}`) : column
      let def = this._schema[columnName]
      if (!def) {
        def = { uniques: new Set() }
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
      if (def.cardinality === undefined || def.cardinality < 201) {
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
}

Object.assign(exports, {
  Table
})
