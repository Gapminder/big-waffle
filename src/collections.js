const Crypto = require('crypto')
const FileSystem = require('fs')
const { Writable } = require('stream')

const CSVParser = require('csv-parse')
const firstline = require('firstline')
const { sample, sampleSize } = require('lodash')

const wait = ms => new Promise(resolve => setTimeout(resolve, ms)) // helper generator for wait promises

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

class RecordProcessor extends Writable {
  constructor (aTable, processorFunction, keyMap = {}, highWatermark = 100) {
    super({ objectMode: true, highWatermark })
    this.table = aTable
    this.processorFunction = processorFunction
    this.mappedColumnNames = Object.assign({}, aTable.mappedColumnNames, keyMap)
  }

  _writev (chunks, callback) {
    try {
      const promises = []
      for (let chunk of chunks) {
        const result = this.processorFunction.call(this.table, chunk.chunk, this.mappedColumnNames) // ok to ignore chunk.encoding
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
      const result = this.processorFunction.call(this.table, record, this.mappedColumnNames) // ok to ignore chunk.encoding
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
  constructor (name) {
    this.name = name
    this.keys = new Set()
  }
}

class Table extends Collection {
  /*
   * A Collection that is implemented as table for a relational database.
   */
  constructor (name, mappedColumns = {}) {
    super(name)
    if (mappedColumns) {
      this._columnNames = Object.assign({}, mappedColumns)
    }
    this._schema = {}
  }

  _column (schemaName) {
    return this._columnNames[schemaName] || schemaName
  }

  _columns (arrayOfSchemaNames) {
    return arrayOfSchemaNames.map(n => this._column(n))
  }

  get mappedColumns () {
    // Return a copy of the column name mapping
    return Object.assign({}, this._columnNames)
  }

  toJSON () {
    /*
     * Return a plain object that represents this dataset and
     * can be saved in a collection.
     */
    const doc = { mappedColumns: this.mappedColumns }
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
    let sql = `ALTER TABLE ${this.name} ADD PRIMARY KEY (${mappedColumns.join(' ,')});`
    console.log(sql)
    const conn = await this.getConnection(database)
    await conn.query(sql)
    this.keys = new Set([...this.keys, ...mappedColumns])
    return this
  }

  async dropPrimaryIndex (database = undefined) {
    let sql = `ALTER TABLE ${this.name} DROP PRIMARY KEY;`
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
    let sql = `CREATE OR REPLACE TABLE ${this.name} (`
    sql = Object.keys(this._schema).reduce((statement, columnName) => {
      const def = this._schema[columnName]
      let type = def.sqlType
      if (type === `VARCHAR`) {
        type = `VARCHAR(${def.size})`
      }
      return `${statement} ${columnName} ${type}${withIndexes ? def.index || '' : ''},`
    }, sql)
    sql = `${sql.slice(0, -1)});`
    console.log(sql)
    const conn = await this.getConnection(database)
    await conn.query(sql)
  }

  async createIndexesIn (database) {
    for (const columnName of Object.keys(this._schema)) {
      const def = this._schema[columnName]
      if (def.index) {
        const sql = `CREATE INDEX ${columnName}_idx ON ${this.name} (${columnName});`
        console.log(sql)
        await (database || this._database).query(sql)
      }
    }
  }

  _prepareRecord (record, _columnNames = {}) {
    const preparedRecord = Object.assign({}, record)
    for (const field of Object.keys(record)) {
      if ((preparedRecord[field] === '' || preparedRecord[field] === null)) {
        delete preparedRecord[field]
        continue
      }
      if (_columnNames[field]) {
        preparedRecord[_columnNames[field]] = record[field]
        delete preparedRecord[field]
      } else if (field.match(/-/)) {
        const newFieldName = field.replace(/-/g, '_')
        this._columnNames[field] = _columnNames[field] = newFieldName
        preparedRecord[newFieldName] = record[field]
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

  _updateRecord (record, keyMap) {
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
          sets[field] = preparedRecord[field]
        }
      }

      let condition = Object.keys(filter).reduce((sql, keyName) => {
        return `${sql}${keyName}=${sqlSafe(filter[keyName], true)} AND `
      }, ``)
      condition = condition.slice(0, -5)
      let updates = Object.keys(sets).reduce((sql, columnName) => {
        return `${sql}${columnName}=${sqlSafe(preparedRecord[columnName], true)}, `
      }, ``)
      updates = updates.slice(0, -2)
      let values = Object.keys(preparedRecord).reduce((sql, columnName) => {
        return `${sql}${columnName}=${sqlSafe(preparedRecord[columnName], true)}, `
      }, ``)
      values = values.slice(0, -2)
      const sql = Object.keys(sets).length > 0 ? `
BEGIN NOT ATOMIC
SELECT COUNT(*) FROM ${this.name} WHERE ${condition} INTO @recordExists;
IF @recordExists > 0
THEN
  UPDATE ${this.name} SET ${updates} WHERE ${condition};
ELSE
  INSERT INTO ${this.name} SET ${values};
END IF;
END;`
        : `
BEGIN NOT ATOMIC
SELECT COUNT(*) FROM ${this.name} WHERE ${condition} INTO @recordExists;
IF @recordExists < 1 THEN
  INSERT INTO ${this.name} SET ${values};
END IF;
END;`
      return this.getConnection()
        .then(connection => {
          return connection.query(sql)
        })
        .catch(err => {
          if (err.code === 'ER_LOCK_DEADLOCK') {
            console.log('deadlock occured going to retry in 500 ms')
            return wait(500).then(() => this._connection.query(sql))
          }
          console.error(err)
          if (this._connection) {
            this._connection.end()
            delete this._connection
          }
          throw err
        })
    } catch (err) {
      console.log(err)
      return err
    }
  }

  loadFromCSVFile (path, keyMap = {}) {
    /*
     * Parse all records in the CSV file and insert into this SQL table.
     *
     * The file may contain a subset of the final set of columns for this table.
     *
     * Returns a Promise that resolves to this Table.
     */
    const table = this
    return new Promise((resolve, reject) => {
      try {
        const recordLoader = new RecordProcessor(table, table._updateRecord, keyMap, 5)
        recordLoader.on('finish', () => {
          console.log(`Finished loading ${path} into ${table.name}`)
          resolve(table)
        })
        recordLoader.on('error', err => reject(err))

        const parser = CSVParser({
          delimiter: ',',
          cast: true,
          columns: true,
          trim: true
        })
        parser.on('error', err => reject(err))

        const csvFile = FileSystem.createReadStream(path)
        console.log(`Loading ${path} into ${table.name}...`)
        csvFile.pipe(parser).pipe(recordLoader)
      } catch (err) {
        reject(err)
      }
    })
  }

  async loadCSVFile (path, keyMap = {}, viaTmpTable = false, separator = ',') {
    /*
     * Parse all records in the CSV file and insert into this SQL table.
     *
     * The file may contain a subset of the final set of columns for this table.
     * The provided list of (non-key) columns must exist in both the CSV file
     * as well as already in this table.
     *
     * Returns a Promise that resolves to this Table.
     */
    if (viaTmpTable !== true) {
      return this.loadFromCSVFile(path, keyMap) // slower, but can handle large cells
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
      return `${statement} ${columnName} ${type},`
    }, '')
    let sql = `CREATE TABLE ${tmpTableName} (${columnDefs.slice(0, -1)}) 
    engine=CONNECT table_type=CSV file_name='${path}' header=1 sep_char='${separator}' quoted=1;`
    console.log(sql)
    const conn = await this.getConnection()
    await conn.query(sql)
    // 2. Copy all records
    /* INSERT INTO dpstest (geo, time, aid_percent)
      *   SELECT * FROM aid_given_percent_of_gni
      *   ON DUPLICATE KEY UPDATE dpstest.aid_percent = aid_given_percent_of_gni.aid_given_percent_of_gni;
      */
    const updates = tableColumns.filter(c => !this.keys.has(c)).map(c => `${this.name}.${c} = ${tmpTableName}.${c}`)
    sql = `
    INSERT INTO ${this.name} (${tableColumns.join(', ')})
      SELECT ${csvColumns.join(', ')} FROM ${tmpTableName}
      ON DUPLICATE KEY UPDATE ${updates.join(', ')};`
    console.log(sql)
    await conn.query(sql)
    // 3. Delete the temporary table
    sql = `DROP TABLE ${tmpTableName}`
    console.log(sql)
    await conn.query(sql)
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
    const updates = columns.map(c => `${this.name}.${this._column(c)} = ${foreignTable.name}.${foreignTable._column(c)}`)
    const sql = `
    UPDATE ${this.name}
    LEFT JOIN ${foreignTable.name} 
      ON ${this.name}.${this._column(sharedColumn)} = ${foreignTable.name}.${foreignTable._column(sharedColumn)}
    SET ${updates.join(', ')}`
    console.log(sql)
    const conn = await this.getConnection()
    await conn.query(sql)
    return this
  }

  _updateSchemaWith (record, keyMap) {
    /*
     * Update the schema with the data from this record.
     */
    const preparedRecord = this._prepareRecord(record, keyMap)
    for (const columnName of Object.keys(preparedRecord)) {
      let def = this._schema[columnName]
      if (!def) {
        def = { uniques: new Set() }
        this._schema[columnName] = def
      }
      const typicalValue = preparedRecord[columnName]
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

  updateSchemaFromCSVFile (path, keyMap = {}) {
    /*
     * Scan all records in the CSV file to find the optimal schema for each column in the file.
     *
     * The file may contain a subset of the final set of columns for this table.
     *
     * Returns a Promise that resolves to this Table.
     */
    const table = this
    return new Promise((resolve, reject) => {
      try {
        const schemaComputer = new RecordProcessor(table, table._updateSchemaWith, keyMap)
        schemaComputer.on('finish', () => {
          console.log(`Finished scan of ${path} for ${table.name}`)
          resolve(table)
        })
        schemaComputer.on('error', err => reject(err))

        const parser = CSVParser({
          delimiter: ',',
          cast: true,
          columns: true,
          trim: true
        })
        parser.on('error', err => reject(err))

        const csvFile = FileSystem.createReadStream(path)
        console.log(`Scanning ${path} to update schema of ${table.name}`)
        console.log(`using mapping: ${JSON.stringify(this._columnNames)}`)
        csvFile.pipe(parser).pipe(schemaComputer)
      } catch (err) {
        reject(err)
      }
    })
  }
}

Object.assign(exports, {
  Table
})
