const fs = require("fs");
const { Writable } = require('stream');
const CSVParser = require("csv-parse");

class CSVMongoLoader extends Writable {
  constructor(collection, options={}) {
    super({objectMode: true, highWatermark: 50});
    this.collection = collection;
    this.options = Object.assign({
      keys: null,
      removeEmptyValues: true,
      detectJSONValues: true,
      closeDb: false
    }, options);

    this.query = this.options.keys instanceof Array 
        ? this.options.keys.reduce((acc, keyField) => {acc[keyField] = null; return acc;}, {}) 
        : {_id: null};
    if (this.options.logQuery === true) {
      console.log(this.query);
    }
  
    if (this.options.keys && this.options.keys.length > 0 && this.options.createIndexes !== false) {
      const keySpec = {};
      this.options.keys.reduce((spec, keyField) => {
        spec[keyField] = 1;
        return keySpec;
      }, keySpec);
      console.log(keySpec);
      this.collection.createIndex(keySpec,
        {
          name: 'primary',
          background: true, 
          unique: true
        }
      );
    }
  }
  
  _prepareRecord(record) {
    const options = this.options;
    let query = this.query;
    if (options.idColumnName) {
      record._id = record[options.idColumnName];
      if (record._id === null || record._id === undefined) {
        throw new Error(`{options.idColumnName} missing in ${this.filePath}: ${record}`);
      }
      delete record[options.idColumnName];
    }
    const fieldMap = options.fieldMap;
    for (const field in record) {
      if (fieldMap && fieldMap.field) {
        record[fieldMap[field]] = record[field];
        delete record.field;
      }          
      if (options.detectJSONValues && typeof(record[field]) === 'string' && record[field].match(/^[{[]{1}/)) {
        try {
          record[field] = JSON.parse(record[field]);
        } catch (jsonErr) {
          console.log(`${field} value was not JSON parsable: ${record[field].substring(0, 20)}`);
        }
      }
      if (options.removeEmptyValues && (record[field] === '' || record[field] === null)) {
        delete record[field];
      }
    }
    const filter = Object.assign({}, query);
    for (const keyField in query) {
      filter[keyField] = record[keyField];
      delete record.keyField;
    }
    if (query._id && record._id) {
      delete record._id;
    }
    return {filter, update: {$set: record}, upsert: true};
  }

  _writev(chunks, callback) {
    let bulkOp = [];
    try{
      bulkOp = chunks.map(chunk => {
        return {updateOne: this._prepareRecord(chunk.chunk)};
      });
    } catch (err) {
      callback(err);
    }
    this.collection.bulkWrite(bulkOp, {ordered: false}, (mongoError, dbOpResult) => {
      if (mongoError) {
        console.error(mongoError);
        console.error(`${this.filePath}`);
        callback(mongoError, null);
        return;
      }
      callback(null, dbOpResult);
    });
  }

  _write(record, encoding, callback) {
    const dbOp = this._prepareRecord(record);
    const updatedRecord = Object.assign({}, record);
    const options = this.options;
    this.collection.findOneAndUpdate(dbOp.filter, dbOp.update, {upsert:dbOp.upsert}, (mongoError, dbOpResult) => {
      if (mongoError) {
        console.error(mongoError);
        console.error(`${this.filePath}: ${record}`);
        callback(mongoError, null);
        return;
      }
      if (options.update) {
        if (dbOpResult.value && dbOpResult.value._id) {
          options.update.updatedRecord(this.collection.name, filter, dbOpResult.value, updatedRecord);
        } else {
          options.update.insertedRecord(this.collection.name, filter, updatedRecord);
        }
      }
      callback(null, dbOpResult.value);
      return;
    });
  }
}

class CSVImporter {
  constructor(filePath, collection, options={}) {
    this.filePath = filePath;
    this.options = Object.assign({
        whenFinished: null,
        keys: null,
        removeEmptyValues: true,
        detectJSONValues: true,
        closeDb: false
      }, options);

    this.parser = CSVParser({
      delimiter: ",",
      cast: true,
      columns: true,
      trim: true
    });
    this.parser.on('error', (err) => {
      console.error(err.message)
    })
    const _this = this;
    this.parser.on('end', function() {
      if (typeof(_this.options.whenFinished) === 'function') {
        _this.options.whenFinished();
      }
    });
    this.loader = new CSVMongoLoader(collection, this.options);
  }
  
  processFile() {
    const csvFile = fs.createReadStream(this.filePath);
    csvFile.pipe(this.parser).pipe(this.loader);
  }
}

async function importCSV(filePath, collection, importOptions={}) {
  const importer = new CSVImporter(filePath, collection, importOptions);
  importer.processFile();
}

//function updateEntitiesFromCSV(datasetName, datasetUpdate, dirPath) {
//  const filenameParser = /ddf\-{2}entities\-{2}([a-z0-9]+)(\-{2}[_a-z0-9]+)?/;
//  const entityFiles = fs.readdirSync(dirPath).filter(filename => filenameParser.test(filename));
//  const finished = [];
//  for (const filename of entityFiles) {
//    const parsedFilename = filenameParser.exec(filename);
//    const domain = parsedFilename[1];
//    const idColumnName = parsedFilename[2] ? parsedFilename[2].substring(2) : domain;
//    console.log(`Updating ${domain} with ${idColumnName} entities from ${filename}`);
//    importCSV(`${dirPath}/${filename}`, datasetUpdate, domain, () => {
//        finished.push(filename);
//        console.log(`Finished reading ${filename}`);
//        if (finished.length >= entityFiles.length) {
//          setTimeout(() => datasetUpdate.save(), 3000); //wait for a few seconds for the last db operations
//        }
//      },
//      {
//        dbName: datasetName,
//        idColumnName
//      });
//  };
//}

//async function updateDatasetFromCSV(datasetName, dirPath) {
//  if (dirPath.endsWith('/')) {
//    dirPath = dirPath.slice(0, -1);
//  }
//  const update = await new DatasetUpdate(datasetName).prepare();
//  // 1. Update concepts
//  importCSV(dirPath + '/ddf--concepts.csv', update, 'concepts',
//      () => {
//        //2. Update entities
//        updateEntitiesFromCSV(datasetName, update, dirPath);
//      },
//      {dbName: datasetName, idColumnName: 'concept'});
//}

Object.assign(module.exports, {
  importCSV
});
//updateDatasetFromCSV('systema_globalis', '/Users/robert/Projects/Gapminder/ddf--gapminder--systema_globalis');