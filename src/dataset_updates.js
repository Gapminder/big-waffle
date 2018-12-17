const FS = require("fs");
const Tmp = require("tmp");
const _ = require("lodash");

const TmpFileDir = "/Users/robert/Projects/Gapminder/BigWaffle/updates";

class DatasetUpdate {
  /*
   * A DatasetUpdate records the changes made when updating a dataset.
   */
  constructor(dataset, fromVersion, toVersion) {
    this.dataset = new Dataset(datasetName);
  }
  
  async prepare() {
    const fd = await new Promise((resolve, reject) => Tmp.file({dir: TmpFileDir, mode: 0o644, postfix: '.json', keep: true},
        (err, path, fd) => {
          if (err) return reject(err);
          console.log(`Created temporary file ${path}`);
          return resolve(fd);
        }));
    this.tmpFile = FS.createWriteStream(null, {fd});
    await this.tmpFile.write(`{"revert": [`);
    return this;
  }

  async _record(change) {
    /*
     * Save the change to the temporary file for this DatasetUpdate.
     */
    this.tmpFile.write("\n    ");
    this.tmpFile.write(JSON.stringify(change));
    return this;
  }
  
  async save() {
    /*
     * Save the recorded changes into a single document in the db.
     */
    if (!this.tmpFile) {
      console.log(`No updates were saved for ${this.dataset.name}!`);
      return this;
    }
    await this.tmpFile.end("\n]}");
    console.log(`Saved update to tmp file....!`);
    //TODO: apply recorded "revert" operations to db of this dataset
  }
  
  async insertedRecord(collection, filter, record) {
    return await this._record({collection, op: {deleteOne: {filter}}});
  }
  
  async updatedRecord(collection, filter, previousRecord, updatedRecord) {
    const $set = {};
    const $unset = {};
    for (let field in updatedRecord) {
      if (previousRecord[field] === undefined) {
        $unset[field] = 1;
      } else if (_.isEqual(previousRecord[field], updatedRecord[field]) === false) {
        $set[field] = previousRecord[field];
      }
    }
    for (let field in previousRecord) {
      if (updatedRecord[field] === undefined) {
        $set[field] = previousRecord[field];
      }
    }
    const update = {$set, $unset};
    if (Object.keys($set).length < 1) {
      delete update.$set;
    }
    if (Object.keys($unset).length < 1) {
      delete update.$unset;
    }
    if (Object.keys(update).length < 1) {
      return this;
    }
    const op = {updateOne: {filter, update}};
    return await this._record({collection, op});
  }
}

Object.assign(exports, {
  Dataset
})
