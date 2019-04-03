const { AssetStore, AssetStoreBucket } = require('./env')
const Log = require('./log')('cloud-storage')

class CloudStore {
  async upload (localPath, remoteName) {
    return Promise.reject(new Error('Should be implemented by subclass'))
  }
  async urlFor (reference, secure = false) {
    // Return a fully qualified HTTP URL to the stored reference.
    return Promise.resolve(`${secure ? 'https' : 'http'}://${this.baseDir || ''}/${reference}`)
  }

  static local () { // For testing, set ASSET_STORE=local
    return new LocalCloudStore()
  }

  static GCS () {
    return new GoogleCloudStore()
  }
}

class LocalCloudStore extends CloudStore {
  async upload (localPath, remoteName) {
    return Promise.resolve(remoteName)
  }
  async urlFor (reference, secure = false) {
    // Return a fully URL to the stored reference.
    return Promise.resolve(`file://${process.cwd()}/${reference}`)
  }
}

class GoogleCloudStore extends CloudStore {
  constructor () {
    super()
    const GCS = require('@google-cloud/storage').Storage
    this._gcs = new GCS()
    this._bucket = this._gcs.bucket(AssetStoreBucket)
    this.baseDir = `storage.googleapis.com/${AssetStoreBucket}`
  }

  async upload (localPath, remoteName) {
    await this._bucket.upload(localPath, {
      destination: remoteName,
      gzip: true,
      metadata: {
        cacheControl: 'public, max-age=31536000'
      }
    })
    const assetUrl = await this.urlFor(remoteName)
    Log.info(`Succesfully uploaded ${localPath} to ${assetUrl}`)
    return remoteName
  }
}

module.exports = CloudStore[AssetStore]()
