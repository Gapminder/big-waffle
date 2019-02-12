class QueryError extends Error {
  constructor (message, code, query) {
    super(message)
    if (code) {
      this.code = code
    }
    if (query) {
      this.query = query
    }
  }

  static NotSupported (query) {
    return new this(` Query is not supported`, 'QL_NOT_SUPPORTED', query)
  }
}

class QuerySyntaxError extends QueryError {
  static MissingSelect (query) {
    return new this(` Query does not have 'select:'`, 'QL_NO_SELECT', query)
  }
  static WrongSelectKey (query) {
    return new this(
      ` Query 'select: {key:' is missing or is not an array of strings`,
      'QL_WRONG_SELECT_KEY', query)
  }
  static WrongSelectValue (query) {
    return new this(
      ` Query 'select: {value:' is missing or is not an array of strings`,
      'QL_WRONG_SELECT_VALUE', query)
  }
  static MissingFrom (query) {
    return new this(` Query does not have 'from:'`, 'QL_NO_FROM', query)
  }
  static WrongFrom (query) {
    return new this(` Query 'from' is not acceptable`, 'QL_WRONG_FROM', query)
  }
  static WrongOrderBy (query) {
    return new this(` Query 'order_by' is not acceptable`, 'QL_WRONG_ORDER_BY', query)
  }
}

class SchemaError extends Error {
  constructor (message, code, schema) {
    super(message)
    if (code) {
      this.code = code
    }
    if (schema) {
      this.schema = schema
    }
  }

  static MissingSchema () {
    return new this(` Object does not have a 'schema'`, 'SCHEMA_MISSING')
  }
}

module.exports = { QueryError, QuerySyntaxError, SchemaError }
