const FileSystem = require("fs");
const { Writable } = require('stream');

const CSVParser = require("csv-parse");
const {sample, sampleSize} = require('lodash');

const { DB } = require("./maria");

const wait = ms => new Promise(resolve => setTimeout(resolve, ms)); //helper generator for wait promises

function sqlSafe(value, quoteResultIfNeeded=false) {
  if (typeof value !== 'string') {
    return value;
  }
  const regex = /[\0\x08\x09\x1a\n\r'\\\%]/g;
  const matches = ['\\0', '\\x08', '\\x09', '\\x1a', '\\n', '\\r', "'", '"', "\\", '\\\\', "%"];
  const replacements = ['\\\\0', '\\\\b', '\\\\t', '\\\\z', '\\\\n', '\\\\r', "''", '""', '\\\\', '\\\\\\\\', '\\%'];
  const safeValue = value.replace(regex, (char) => replacements[matches.indexOf(char)]);
  if (quoteResultIfNeeded) {
    if (safeValue === 'TRUE' || safeValue === 'FALSE') {
      return safeValue;
    }
    return `'${safeValue}'`;
  }
  return safeValue;
}

class RecordProcessor extends Writable {
  constructor(aTable, processorFunction, fieldMap={}, highWatermark=100) {
    super({objectMode: true, highWatermark});
    this.table = aTable;
    this.processorFunction = processorFunction;
    this.fieldMap = fieldMap;
  }


  _writev(chunks, callback) {
    try {
      const promises = [];
      for (let chunk of chunks) {
        const result = this.processorFunction.call(this.table, chunk.chunk, this.fieldMap); //ok to ignore chunk.encoding
        if (result instanceof Promise) {
          promises.push(result);
        }
      }
      if (promises.length > 0) {
        Promise.all(promises).then(results => callback(null, {processed: chunks.length}));
      } else {
        callback(null, {processed: chunks.length});
      }
    } catch (err) {
      callback(err);
    }
  }
  
  _write(record, encoding, callback) {
    try {
      const result = this.processorFunction.call(this.table, record, this.fieldMap); //ok to ignore chunk.encoding
      if (result instanceof Promise) {
        result.then(value => callback(null, {processed: 1}));
      } else {
        callback(null, {processed: 1});
      }
    } catch (err) {
      callback(err);
    }
  }
}

class Collection {
  constructor (name) {
    this.name = name;
    this.keys = new Set();
  }
}

class Table extends Collection {
  /*
   * A Collection that is implemented as table for a relational database.
   */
  constructor (name) {
    super(name);
    this.schema = {};
  }
  
  get estimatedRowSize() {
    let size = 0;
    for (const fieldDef of Object.values(this.schema)) {
      if (fieldDef.sqlType === `VARCHAR`) {
        size += (fieldDef.size) * 2 + 2;
      } else if (fieldDef.sqlType === `INTEGER`) {
        size += 2;
      } else if (fieldDef.sqlType === `DOUBLE`) {
        size += 8;
      } else if (fieldDef.sqlType === `BOOLEAN`) {
        size += 1;
      }
    }
    return size;
  }

  indexOn(columnName, primary=false, unique=false) {
    let columnDefinition = this.schema[columnName];
    if (!columnDefinition) {
      columnDefinition = {};
      this.schema[columnName] = columnDefinition;
    }
    columnDefinition.index = `${primary ? ' PRIMARY' : (unique ? ' UNIQUE' : '')} KEY`;
    this.keys.add(columnName);
  }

  primaryIndexOn(columnName) {
    return this.indexOn(columnName, true);
  }
  
  async createIn(database, withIndexes=true) {
    // shorten too long columnNames and save mapping
    const fieldMap = {};
    for (const columnName of Object.keys(this.schema)) {
      if (columnName.length > 64) {
        const newColumnName = sample("abcdefghijkmnopqrstuvwxyz") + sampleSize("abcdefghijkmnopqrstuvwxyz23456789", 5).join('');
        fieldMap[columnName] = newColumnName;
        this.schema[newColumnName] = this.schema[columnName];
        delete this.schema[columnName];
      }
    }
    
    let sql = `CREATE OR REPLACE TABLE ${this.name} (`;
    sql = Object.keys(this.schema).reduce((statement, columnName) => {
      const def = this.schema[columnName];
      let type = def.sqlType;
      if (type === `VARCHAR`) {
        type = `VARCHAR(${def.size})`
      }
      return `${statement} ${columnName} ${type}${withIndexes? def.index || '' : ''},`;
    }, sql);
    sql = `${sql.slice(0,-1)});`;
    console.log(sql);
    await database.query(sql);
    this._database = database;
    return fieldMap;
  }

  async createIndexesIn(database) {
    for (const columnName of Object.keys(this.schema)) {
      const def = this.schema[columnName];
      if (def.index) {
        const sql = `CREATE INDEX ${columnName}_idx ON ${this.name} (${columnName});`;
        console.log(sql);
        await (database || this._database).query(sql);
      }
    }
  }

  _prepareRecord(record, fieldMap={}) {
    const preparedRecord = Object.assign({}, record);
    for (const field of Object.keys(record)) {
      if ((preparedRecord[field] === '' || preparedRecord[field] === null)) {
        delete preparedRecord[field];
        continue;
      }
      if (fieldMap[field]) {
        preparedRecord[fieldMap[field]] = record[field];
        delete preparedRecord[field];
      }
      if (field.match(/\-/)) {
        const newFieldName = field.replace(/\-/g, '_');
        fieldMap[field] = newFieldName;
        preparedRecord[newFieldName] = record[field];
        delete preparedRecord[field];
      } 
    }
    return preparedRecord;
  }

  async getConnection() {
    if (this._connection) {
      return this._connection;
    }
    this._connection = await this._database.getConnection();
    return this._connection;
  }
  
  updateRecord(record, fieldMap) {
    /*
     * Return a promise!
     */
    try {
      const preparedRecord = this._prepareRecord(record, fieldMap)
      const filter = {};
      const sets = {};
      for (const field of Object.keys(preparedRecord)) {
        if (this.keys.has(field)) {
          filter[field] = preparedRecord[field];
          if (!filter[field] && this.schema[field].index.match(/PRIMARY/g)) {
            throw new Error(`Record misses value for primary key ${field}\n${record}`);
          } 
        } else {
          sets[field] = preparedRecord[field];
        }
      }

      let condition = Object.keys(filter).reduce((sql, keyName) => {
        return `${sql}${keyName}=${sqlSafe(filter[keyName], true)} AND `
      }, ``);
      condition = condition.slice(0, -5);
      let updates = Object.keys(sets).reduce((sql, columnName) => {
        return `${sql}${columnName}=${sqlSafe(preparedRecord[columnName], true)}, `
      }, ``);
      updates = updates.slice(0, -2);
      let values = Object.keys(preparedRecord).reduce((sql, columnName) => {
        return `${sql}${columnName}=${sqlSafe(preparedRecord[columnName], true)}, `
      }, ``);
      values = values.slice(0, -2);
      const statement = `
BEGIN NOT ATOMIC
SELECT COUNT(*) FROM ${this.name} WHERE ${condition} INTO @recordExists;
IF @recordExists > 0
THEN
  UPDATE ${this.name} SET ${updates} WHERE ${condition};
ELSE
  INSERT INTO ${this.name} SET ${values};
END IF;
END;`
      this.getConnection()
      .then(connection => {
        const promise = connection.query(statement);
        promise.catch(dbErr => {
          if (dbErr.code === 'ER_LOCK_DEADLOCK') {
            console.log('deadlock occured going to retry in 500 ms');
            wait(500).then(() => connection.query(statement));
          } else {
            throw dbErr;
          }
        });
        return promise;        
      })
      .catch(err => {
        if (this._connection) {
          this._connection.end();
          delete this._connection;
        }
      });
    } catch (err) {
      console.log(err);
      return err;
    }
  }

  loadFromCSVFile(path, fieldMap={}) {
    /*
     * Parse all records in the CSV file and insert into this SQL table.
     * 
     * The file may contain a subset of the final set of columns for this table.
     * 
     * Returns a Promise that resolves to this Table.
     */
    const table = this;
    return new Promise((resolve, reject) => {
      try {
        const recordLoader = new RecordProcessor(table, table.updateRecord, fieldMap, 5);
        recordLoader.on('finish',  () => {
          console.log(`Finished loading ${path} into ${table.name}`);
          resolve(table);
        });
        recordLoader.on('error', err => reject(err));

        const parser = CSVParser({
          delimiter: ",",
          cast: true,
          columns: true,
          trim: true
        });
        parser.on('error', err => reject(err));

        const csvFile = FileSystem.createReadStream(path);
        console.log(`Loading ${path} into ${table.name}...`);
        csvFile.pipe(parser).pipe(recordLoader);

      } catch (err) {
        reject(err);
      }
    });
    
  }

  updateSchemaWith(record, fieldMap) {
    /*
     * Update the schema with the data from this record.
     */
    const preparedRecord = this._prepareRecord(record, fieldMap);
    for (const columnName of Object.keys(preparedRecord)) {
      let def = this.schema[columnName];
      if (!def) {
        def = {};
        this.schema[columnName] = def;
      }
      const typicalValue = preparedRecord[columnName];
      if (typeof typicalValue === 'number') {
        if (Number.isInteger(typicalValue)) {
          if (!def.sqlType || def.sqlType === `INTEGER`) {
            def.sqlType = typicalValue > 2147483647 ? `BIGINT` : `INTEGER`;
          }
        } else if (Number.isInteger(typicalValue) === false && def.sqlType !== `VARCHAR`) {
          def.sqlType = `DOUBLE`;
        }
      } else if (typeof typicalValue === 'boolean' && !def.sqlType) {
          def.sqlType = `BOOLEAN`;
      } else if (typeof typicalValue === 'string') {
        def.size = Math.max(sqlSafe(typicalValue).length + 1, def.size || 1);
        if ((!def.sqlType || def.sqlType === `BOOLEAN`) && 
            (columnName.startsWith('is--') || typicalValue.toUpperCase() === 'TRUE' || typicalValue.toUpperCase() === 'FALSE')) {
          def.sqlType = `BOOLEAN`;
        } else if (typicalValue.match(/^[{[]{1}/)){
           if (def.size > 100) {
            def.sqlType = `JSON`;
          } else {
            def.sqlType = `VARCHAR`;
          }
        } else {
          def.sqlType = `VARCHAR`;
        }
      }
    }
  }

  updateSchemaFromCSVFile(path, fieldMap={}) {
    /*
     * Scan all records in the CSV file to find the optimal schema for each column in the file.
     * 
     * The file may contain a subset of the final set of columns for this table.
     * 
     * Returns a Promise that resolves to this Table.
     */
    const table = this;
    return new Promise((resolve, reject) => {
      try {
        const schemaComputer = new RecordProcessor(table, table.updateSchemaWith, fieldMap);
        schemaComputer.on('finish',  () => {
          console.log(`Finished scan of ${path} for ${table.name}`);
          resolve(table);
        });
        schemaComputer.on('error', err => reject(err));

        const parser = CSVParser({
          delimiter: ",",
          cast: true,
          columns: true,
          trim: true
        });
        parser.on('error', err => reject(err));

        const csvFile = FileSystem.createReadStream(path);
        console.log(`Scanning ${path} to update schema of ${table.name}`);
        console.log(`using mapping: ${JSON.stringify(fieldMap)}`);
        csvFile.pipe(parser).pipe(schemaComputer);

      } catch (err) {
        reject(err);
      }
    });
  }
}

Object.assign(exports, {
  Table
});
