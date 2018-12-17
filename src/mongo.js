/**
 * Common constants and functions for the MongoDB.
 */
const mongodb = require('mongodb');
const {sample, sampleSize} = require('lodash');

const env = require('./env');

const databases = {};

async function getDb(dbName='systema_globalis') {
  let db = databases[dbName];
  if (db == null) {
    const options = {useNewUrlParser: true}; //current URL string parser is deprecated, and will be removed in a future version.
    if (env.MongoUser) {
      options.auth = {user: env.MongoUser, password: env.MongoPwd};
      options.authSource = 'admin';
    }
    const client = await mongodb.MongoClient.connect(env.MongoUrl, options);
    console.log(`Created Mongo client for DB '${dbName}'`);
    db = client.db(dbName);
    db.close = async function() {
     await client.close(false); 
    }
    databases[dbName] = db;
  }
	return db;
}

async function closeDb(dbName='systema_globalis') {
  const db = databases[dbName];
  if (db) {
    await db.close();
    delete databases[dbName];
  }
}

function closeAll(callback) {
  const promises = [];
  for (let db of Object.values(databases)) {
    promises.push(db.close());
  }
  return Promise.all(promises).then(() => {
    return typeof(callback) === 'function' ? callback() : callback; 
  });
}

async function getCollection(collectionName, dbName='systema_globalis') {
  const db = await getDb(dbName);
  return db.collection(collectionName);
}

const capitals = "ABCDEFGHJKLMNPQRSTUVWXYZ";
const chars = "abcdefghijkmnopqrstuvwxyz"; 
const digits = "23456789"; //1 is often rendered too similar to l

function randomId(length=8, options={}) {
  const _opts = Object.assign({
    withDigits: true, 
    withChars: true, 
    withCapitals: false, 
    intialChar: true, 
    intialCapital: false
  }, options);
  let id = '';
  if (_opts.intialCapital && _opts.withCapitals) {
    id = sample(capitals);
  } else if (_opts.initialChar && _opts.withChars) {
    id = sample(chars);
  }
  let charSet = (_opts.withDigits ? digits : '') 
    + (_opts.withChars ? chars : '') 
    + (_opts.withCapitals ? capitals : '');
  return id + sampleSize(charSet, length - id.length).join('');
}

Object.assign(exports, {
  getDb,
  getCollection,
  closeDb,
  closeAll,
  ObjectID: mongodb.ObjectID,
  randomId,
  ascending: 1,
  descending: -1
});
