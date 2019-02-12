const { Readable, Transform } = require('stream')

const { QuerySyntaxError } = require('./errors.js')

class Query {
  constructor (obj) {
    Object.assign(this, obj)
    this.validateSyntax()
    this.select.key.sort()
    this.select.value.sort()
  }

  validateSyntax () {
    /*
     * Check that this query has the required components
     * and that optional parts are syntactically correct.
     *
     * Throws an eror in case the syntax is seemed incorrect.
     * Otherwise returns true.
     *
     * Note that this method does not check for existence
     * of concepts in a dataset etc.
     */
    if (this.select === null || typeof this.select !== 'object') {
      throw QuerySyntaxError.MissingSelect(this)
    }
    if (Array.isArray(this.select.key) !== true) {
      throw QuerySyntaxError.WrongSelectKey(this)
    }
    if (Array.isArray(this.select.value) !== true) {
      throw QuerySyntaxError.WrongSelectValue(this)
    }
    if (typeof this.from !== 'string') {
      throw QuerySyntaxError.MissingSelect(this)
    }
    if (this.order_by) {
      if (this.order_by instanceof Array !== true) {
        throw QuerySyntaxError.WrongOrderBy(this)
      }
      const normalizedOrderBy = []
      for (const elem of this.order_by) {
        const field = typeof elem === 'string' ? elem : Object.keys(elem)[0]
        const direction = typeof elem === 'string' ? 'asc' : elem[field]
        if (['asc', 'desc'].includes(direction) !== true) {
          throw QuerySyntaxError.WrongOrderBy(this)
        }
        normalizedOrderBy.push({ [field]: direction })
      }
      this.order_by = normalizedOrderBy
    }
  }

  get header () {
    return [...this.select.key, ...this.select.value]
  }

  get isForEntities () {
    return this.from === 'entities'
  }

  get isForSchema () {
    const re = /([a-z]+)\.schema/
    return re.test(this.from)
  }

  get isForData () {
    return this.from === 'datapoints'
  }
}

class RecordPrinter extends Transform {
  constructor (query, filterNullRecords = false) {
    /*
    * Return a Transform stream that can pipe records (Object instances) to a stringified (textual) JSON array representation.
    */
    super(
      {
        readableHighWaterMark: 1000000,
        writableHighWaterMark: 1000,
        writableObjectMode: true
      }
    )
    this.query = query
    this.recordCounter = 0
    if (filterNullRecords) {
      this._firstValueIndex = query.select.key.length
      this.filterNullRecords = true
    }
  }

  _transform (record, encoding, callback) {
    try {
      if (this.query.header && !this._headerPushed) {
        this._headerPushed = true
        this.push(`[\n${JSON.stringify(this.query.header)}\n`)
      }
      if (this.filterNullRecords) {
        // don't push a record that only contains null values
        for (let idx = this._firstValueIndex; idx < record.length; idx++) {
          if (record[idx]) {
            this.push(`,${JSON.stringify(record)}`)
            this.recordCounter += 1
            break
          }
        }
      } else {
        this.push(`,${JSON.stringify(record)}`)
        this.recordCounter += 1
      }
      callback()
    } catch (err) {
      console.error(err)
      callback(err)
    }
  }

  _flush (callback) {
    try {
      this.push(`\n]`)
      return callback()
    } catch (err) {
      console.error(err)
      return callback(err)
    }
  }
}

class ArrayStream extends Readable {
  constructor (anArray = [], options = {}) {
    super(Object.assign({}, options, { objectMode: true }))
    this.items = anArray
  }

  _read () {
    let backPressure = false
    while (!backPressure) {
      const item = this.items.shift()
      if (item === undefined) {
        return this.push(null) // signal end of this stream
      }
      backPressure = this.push(item)
    }
  }
}

module.exports = {
  Query,
  RecordPrinter,
  ArrayStream
}
