const FS = require("fs");
const JSONFile = require('jsonfile');
const { getCollection, getDb, closeDb, ascending, descending } = require("./mongo");
const { importCSV } = require("./csv");

class Dataset {
  /*
   * A Dataset encapsulates a DDF compliant set of data, including:
   * - entity domains
   * - concepts
   * - datapoints
   * - metadata
   * 
   */
  constructor(name, version) {
    this.name = name;
  }

  _asDoc(includeId=false) {
    /*
     * Return a plain object that represents this dataset and
     * can be saved in a MongoDb collection.
     */
    const doc = {};
    for (const key in this) {
      if ((includeId && key === '_id') || key.startsWith('_') === false) {
        doc[key] = this[key];
      }
    }
    return doc;
  }

  initialize(doc) {
    Object.assign(this, doc);
  }

  get _keyFields() {
    return ['name'];
  }
  
  async save() {
    const doc = this._asDoc();
    const filter = this._keyFields.reduce((flt, field) => {
      flt[field] = doc[field];
      delete doc[field];
      return flt;
    }, {});
    let dbOpResult = await this._datasets.updateOne(filter, {$set: doc}, {upsert: true});
    if (dbOpResult.upsertedId) {
      this._id = dbOpResult.upsertedId;
    }
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
  constructor(name, version) {
    super(name);
    this.version = version;
    this.collections = [];
  }
  
  incrementVersion(newVersion = null) {
    if (! this.version) {
      this.version = newVersion || 1.0;
    } else {
      this.version = newVersion || this.version + 1;
    }
  }

  async save() {
    if (! this.version) {
      this.incrementVersion();
    }
    return await super.save();
  }

  get _keyFields() {
    return super._keyFields.concat(['version']);
  }
  
  _versioned(collectionName) {
    return `${collectionName}_${this.version}`;
  }

  async _getCollection(collectionName) {
    if (this._collections === undefined) {
      this._collections = {};
    }
    let collection = this._collections[collectionName]; 
    if (collection === undefined) {
      collection = await getCollection(this._versioned(collectionName), this.name);
      this._collections[collectionName] = collection;
    }
    return collection;
  }

  async open() {
    this._datasets = await getCollection('all', 'datasets');
    const query = {name: this.name};
    if (this.version) {
      query.version = this.version;
    }
    try {
      const docs = await this._datasets.find(query).sort('version', descending).limit(1).toArray();
      const doc = docs && docs.length === 1 ? docs[0] : undefined;
      if (doc) {
        this.initialize(doc);
        console.log(`Loaded dataset ${this.name}.${this.version} from DB`);
      }
    } catch (mongoError) {
      console.error(mongoError);
    }
    return this;
  }

  close() {
    /*
     * Gracefully close all the possibly open connections, streams, etc.
     * 
     * This is useful in functions that will be invoked from a command line shell,
     * so that nodejs will actually exit.
     */
    if (this._datasets) {
      closeDb('datasets');
    }
    if (this._collections instanceof Object) {
      closeDb(this.name);
    }
  }

  async revert() {
    /*
     * Restore this Dataset to the one-but-last version
     */
  }
  
  async loadDatapointsFromDirectory(dirPath, options) {
    if (! options.dataPackage) {
      try {
        options.dataPackage = await JSONFile.readFile(`${dirPath}/datapackage.json`);
      } catch (err) {
        console.error(err);
        return this;
      }
    }
    const dataPackage = options.dataPackage;
    const entitySetDomain = {}; //mapping of entity set names to entity domain names
    const concepts = await this._getCollection('concepts');
    let entitySets = await concepts.find({concept_type: 'entity_set'}, {projection: {_id: 1, domain: 1}}).toArray();
    entitySets = entitySets.reduce((obj, doc) => {
      obj[doc._id] = doc.domain;
      return obj;
    }, {});
    console.log(entitySets);
    const datapoints = await this._getCollection('datapoints');
    let indexes = new Set();
    try {
      let indexes = await datapoints.indexes();
      indexes = new Set(indexes.map(idx => Object.keys(idx.key)[0]));
    } catch (dbErr) {
      if (dbErr.codeName !== 'NamespaceNotFound') { //NamespaceNotFound is for when the collection doesn't exists, which is ok.
        console.error(dbErr);
        throw (dbErr);
      }
    }
    console.log(indexes);
    let filesInProgress = 0;
    let dpSpecIdx = 0;
    let aDataset = this;
    
    function processNextDatapointSpec() {
      let dpSpec = dataPackage.ddfSchema.datapoints[dpSpecIdx];
      dpSpecIdx += 1;

      let isAboutEntitySet = false;
      for (let key of dpSpec.primaryKey) {
        if (entitySets[key]) {
          isAboutEntitySet = true;
          break;
        }
        const idx = entitySets[key] || key;
        if (indexes.has(idx) === false) {
          indexes.add(idx);
          datapoints.createIndex(idx, {name: idx, background: true, sparse: true});
        }
      }
      if (isAboutEntitySet) {
        processNextDatapointSpec();
        return;
      }
      
      console.log(dpSpec);

      for (let resource of dpSpec.resources) {
        //TODO: check the resource def in the dataPackage, for now assume that the resource name == the fileName.
        importCSV(`${dirPath}/${resource}.csv`, datapoints, {
          keys: dpSpec.primaryKey.map(key => entitySets[key] || key),
          createIndexes: false,
          fieldMap: entitySets,
          whenFinished: () => {
            console.log(`Finished reading ${resource}.csv`);
            if (dpSpecIdx < dataPackage.ddfSchema.datapoints.length) {
              processNextDatapointSpec();
            } else {
              aDataset.collections.push['datapoints'];
              aDataset.save();
              if (options.update) {
                setTimeout(() => options.update.save(), 3000); //wait for a few seconds for the last db operations
              }
            }
          }
        });
      }   
    }
    
    processNextDatapointSpec();
  }

  async loadEntitiesFromDirectory(dirPath, options={}) {
    const filenameParser = /ddf\-{2}entities\-{2}([a-z0-9]+)(\-{2}[_a-z0-9]+)?/;
    const entityFiles = FS.readdirSync(dirPath).filter(filename => filenameParser.test(filename));
    const finished = [];
    for (const filename of entityFiles) {
      const parsedFilename = filenameParser.exec(filename);
      const domain = parsedFilename[1];
      const idColumnName = parsedFilename[2] ? parsedFilename[2].substring(2) : domain;
      const collection = await this._getCollection(domain);
      console.log(`Updating ${domain} with ${idColumnName} entities from ${filename}`);
      await importCSV(`${dirPath}/${filename}`, collection, 
        {
          idColumnName,
          whenFinished: () => {
            if (this.collections.includes(domain) === false) {
              this.collections.push(domain);
            }
            finished.push(filename);
            console.log(`Finished reading ${filename}`);
            if (finished.length >= entityFiles.length) {
              if (options.update) {
                setTimeout(() => options.update.save(), 3000); //wait for a few seconds for the last db operations
              }
              this.loadDatapointsFromDirectory(dirPath, options);
            }
          }
        }
      );
    };
  } 
  
  async loadFromDirectory(dirPath) {
    if (dirPath.endsWith('/')) {
      dirPath = dirPath.slice(0, -1);
    }
    // 1. Read concepts from file and store in 'concepts' collection.
    const concepts = await this._getCollection('concepts');
    try {
      await importCSV(dirPath + '/ddf--concepts.csv', concepts,
        {
          idColumnName: 'concept',
          whenFinished: () => {
            if (this.collections.includes('concepts') === false) {
              this.collections.push('concepts');
            }
            this.loadEntitiesFromDirectory(dirPath);
          }
        }
      );
    } catch (err) {
      console.log(err);
      throw (err);
    }
  }
  
  async updateFromDirectory(dirPath, incrementally=false) {
    /*
     * Create a new version of this dataset with the data in the given directory.
     * 
     */
    
  }
}

Object.assign(exports, {
  Dataset
})

const sg = new DataSource("systema_globalis");
sg.open()
  .then(async function(ds) {
    ds.incrementVersion();
    ds.loadFromDirectory('/Users/robert/Projects/Gapminder/ddf--gapminder--systema_globalis');
  });