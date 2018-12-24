const FS = require("fs");

const JSONFile = require('jsonfile');
const Moment = require('moment');

const { DB } = require("./maria");
const { Table } = require("./collections");

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
    this.entitySets = {}; //mapping of entity sets to entity domains
  }

  _asDoc(includeId=false) {
    /*
     * Return a plain object that represents this dataset and
     * can be saved in a collection.
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
    let filter = '';
    if (filter.endsWith(',')) {
      filter = filter.slice(0, -1);
    }
    let sql;
    if (this._isNew) {
      filter = this._keyFields.reduce((flt, field) => {
        flt += ` ${field} = '${doc[field]}',`;
        delete doc[field];
        return flt;
      }, '');
      sql = `INSERT INTO datasets SET${filter} definition = '${JSON.stringify(doc)}';`; 
    } else {
      filter = this._keyFields.reduce((flt, field) => {
        flt += ` ${field} = '${doc[field]}' AND `;
        delete doc[field];
        return flt;
      }, '');
      if (filter.endsWith(' AND ')) {
        filter = filter.slice(0, -5);
      }
      sql = `UPDATE datasets SET definition = '${JSON.stringify(doc)}' WHERE${filter};`;
    }
    try {
      let dbOpResult = await DB.query(sql);
      console.log(`${this._isNew ? 'Inserted': 'Updated'} dataset ${this.name}.${this.version}`);
      delete this._isNew;
    } catch (dbErr) {
      console.error(dbErr);
    }
    return this;
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
  }
  
  incrementVersion(newVersion = null) {
    if (! this.version) {
      this.version = newVersion || `${Moment.utc().seconds(1).format('YYYYMMDDss')}`;
    } else if (newVersion) {
      this.version = newVersion;
    } else if (/[0-9]{2}$/.test(this.version)) {
      let root = this.version.slice(0, -2);
      const minorVersion = Number.parseInt(this.version.slice(-2));
      console.log(minorVersion);
      let versionDate = Moment.utc(root, 'YYYYMMDD', true);
      if (versionDate.isValid()) {
        if (versionDate.isSameOrAfter(Moment.utc(), 'day')) {
          versionDate.seconds(minorVersion + 1);
        } else {
          versionDate = Moment.utc().seconds(1);
        }
        this.version = `${versionDate.format('YYYYMMDDss')}`;
      } else {
        this.version = `${root}${minorVersion < 10 ? '0' : ''}${minorVersion + 1}`;
      }
    } else {
      this.version = this.version + '1';
    }
    this._isNew = true;
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
  
  _getCollection(collectionName) {
    return `${this.name}_${collectionName}_${this.version}`;
  }

  async open() {
    try {
      let sql = `SELECT name, version, definition FROM datasets WHERE name = '${this.name}'`;
      if (this.version) {
        sql += ` AND version = '${this.version}'`;
      }
      sql += ` ORDER BY version DESC`; 

      const docs = await DB.query(sql);
      const doc = docs && docs.length === 1 ? docs[0] : undefined;
      if (doc) {
        this.version = doc.version;
        this.initialize(JSON.parse(doc.definition));
        console.log(`Loaded dataset ${this.name}.${this.version} from DB`);
        if (this._isNew) {
          this._isNew = false;
        }
      } else {
        this._isNew = true;
      }
    } catch (dbError) {
      console.error(dbError);
    }
    return this;
  }

  async revert() {
    /*
     * Restore this Dataset to the one-but-last version
     */
  }  
  
  _getFieldMapForEntityCSVFile(filename) {
    const filenameParser = /ddf\-{2}entities\-{2}([a-z0-9]+)(\-{2}[_a-z0-9]+)?/;

    const parsedFilename = filenameParser.exec(filename);
    const domain = parsedFilename[1];
    const idColumnName = parsedFilename[2] ? parsedFilename[2].substring(2) : domain;
    if (idColumnName && idColumnName !== domain) {
      return {[idColumnName]: domain};
    }
    return {}
  }

  loadFromDirectory(dirPath) {
    if (dirPath.endsWith('/')) {
      dirPath = dirPath.slice(0, -1);
    }

    return new Promise(async (resolve, reject) => {
      try {
        const dataPackage = await JSONFile.readFile(`${dirPath}/datapackage.json`);
        // 1. Read concepts from file and store in 'concepts' collection.
        const concepts = new Table(this._getCollection('concepts'));
        concepts.primaryIndexOn('concept');
        await concepts.updateSchemaFromCSVFile(dirPath + '/ddf--concepts.csv');
        await concepts.createIn(DB);
        await concepts.loadFromCSVFile(dirPath + '/ddf--concepts.csv');
        // 2. Map entity sets to entity domains
        const domains = (await DB.query(`SELECT concept FROM ${concepts.name} WHERE concept_type = 'entity_domain';`))
        .reduce((d, domain) => {
          d[domain.concept] = new Set();
          return d
        }, {});
        this.entitySets = (await DB.query(`SELECT concept, domain FROM ${concepts.name} WHERE concept_type = 'entity_set';`))
        .reduce((s, entitySet) => {
          s[entitySet.concept] = entitySet.domain;
          return s;
        }, {});
        console.log(this.entitySets);
        // 2. Create tables for each entity domain and load all files for that entity domain.
        for (const entityDef of dataPackage.ddfSchema.entities) {
          const entitySet = entityDef.primaryKey[0];
          const domain = domains[entitySet] ? entitySet : this.entitySets[entitySet];
          if (!domain) {
            throw new Error(`Domain for entity set ${entitySet} was not defined in concepts`);
          }
          entityDef.resources.forEach(res => domains[domain].add(res));
        }
        console.log(domains);
        Object.keys(domains).forEach(async dom => {
          const table = new Table(this._getCollection(dom));
          table.primaryIndexOn(dom);
          const files = domains[dom];
          for (const file of files) {
            await table.updateSchemaFromCSVFile(`${dirPath}/${file}.csv`, this._getFieldMapForEntityCSVFile(file)); //TODO: look up the actual file from the resource entry
          }
          console.log(table.schema);
          await table.createIn(DB);
          // 3. Now load the data from each of the resources defined for the (entity sets of the) domain.
          for (const file of files) {
//            if (file != 'ddf--entities--geo--country') continue;
            await table.loadFromCSVFile(`${dirPath}/${file}.csv`, this._getFieldMapForEntityCSVFile(file)); //TODO: look up the actual file from the resource entry
          }
        });
        // 3. Now create the datapoints table and then load all the data.
        const datapoints = new Table(this._getCollection('datapoints'));
        const files = new Set();
        for (const datapointDef of dataPackage.ddfSchema.datapoints) {
          for (const key of datapointDef.primaryKey) {
            datapoints.indexOn(this.entitySets[key] || key);
          }
          for (const file of datapointDef.resources) {
            files.add(file); //TODO: look up the actual file from the resource entry
          }
        }
        for (const file of files) {
          await datapoints.updateSchemaFromCSVFile(`${dirPath}/${file}.csv`, this.entitySets);
        }
        console.log(datapoints.schema);
        console.log(`Expected row size is ${datapoints.estimatedRowSize} bytes`);
        const datapointsFieldMap = await datapoints.createIn(DB, false);
        console.log(datapointsFieldMap);
        Object.assign(datapointsFieldMap, this.entitySets);
        await datapoints.createIndexesIn(DB);
        for (const file of files) {
          await datapoints.loadFromCSVFile(`${dirPath}/${file}.csv`, datapointsFieldMap);
        }
        resolve(this);
      } catch (err) {
        reject(err);
      }
    });
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
//    ds.incrementVersion();
    ds.save();
    await ds.loadFromDirectory('/Users/robert/Projects/Gapminder/ddf--gapminder--systema_globalis');
//    DB.end();
  });