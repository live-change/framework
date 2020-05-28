const ReactiveDao = require("@live-change/dao")

class ReaderModel {

  constructor(serviceName, modelName, service) {
    this.serviceName = serviceName
    this.modelName = modelName
    this.service = service
    this.tableName = this.serviceName + "_" + this.modelName
  }

  path(id) {
    return ['database', 'tableObject', this.service.databaseName, this.tableName, id]
  }

  rangePath(range = {}) {
    if(typeof range != 'object') {
      const str = range.toString()
      return this.rangePath({ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    if(Array.isArray(range)) this.rangePath(range.join(','))
    return ['database', 'tableRange', this.service.databaseName, this.tableName, range]
  }

  indexRangePath(index, range = {}) {
    if(typeof range != 'object' || Array.isArray(range)) {
      const values = Array.isArray(range) ? range : [range]
      const str = values.map(value => value === undefined ? '' : JSON.stringify(value)).join(':')+'_'
      return this.indexRangePath(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    return ['database', 'query', this.service.databaseName, `(${
        /// TODO: observe objects
        async (input, output, { tableName, indexName, range }) => {
          const mapper = async (res) => input.table(tableName).object(res.to).get()
          await (await input.index(indexName)).range(range).onChange(async (obj, oldObj) => {
            output.change(obj && await mapper(obj), oldObj && await mapper(oldObj))
          })
        }
    })`, { indexName: this.tableName+'_'+index, tableName: this.tableName, range }]
  }

  indexObjectPath(index, range = {}) {
    if(typeof range != 'object' || Array.isArray(range)) {
      const values = Array.isArray(range) ? range : [range]
      const str = values.map(value => value === undefined ? '' : JSON.stringify(value)).join(':')+'_'
      return this.indexObjectPath(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    return ['database', 'queryObject', this.service.databaseName, `(${
        /// TODO: observe object
        async (input, output, { tableName, indexName, range }) => {
          const mapper = async (res) => input.table(tableName).object(res.to).get()
          await (await input.index(indexName)).range(range).onChange(async (obj, oldObj) => {
            output.change(obj && await mapper(obj), oldObj && await mapper(oldObj))
          })
        }
    })`, { indexName: this.tableName+'_'+index, tableName: this.tableName, range }]
  }

  indexRangeDelete(index, range = {}) {
    if(typeof range != 'object' || Array.isArray(range)) {
      const values = Array.isArray(range) ? range : [range]
      const str = values.map(value => value === undefined ? '' : JSON.stringify(value)).join(':')+'_'
      return this.indexRangeDelete(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    this.service.dao.request(['database', 'query'], this.service.databaseName, `(${
        async (input, output, { tableName, indexName, range }) => {
          await (await input.index(indexName)).range(range).onChange(async (ind, oldInd) => {
            output.table(tableName).delete(ind.to)
          })
        }
    })`, { indexName: this.tableName+'_'+index, tableName: this.tableName, range })
  }

  indexRangeUpdate(index, range = {}, update) {
    if(typeof range != 'object' || Array.isArray(range)) {
      const values = Array.isArray(range) ? range : [range]
      const str = values.map(value => value === undefined ? '' : JSON.stringify(value)).join(':')+'_'
      return this.indexRangeDelete(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' }, update)
    }
    const operations = Array.isArray(update) ? update : [{ op:'merge', property: null, value: update }]
    this.service.dao.request(['database', 'query'], this.service.databaseName, `(${
        async (input, output, { tableName, indexName, range, operations }) => {
          await (await input.index(indexName)).range(range).onChange(async (ind, oldInd) => {
            output.table(tableName).update(ind.to, operations)
          })
        }
    })`, { indexName: this.tableName+'_'+index, tableName: this.tableName, range, operations })
  }

  observable(id) {
    return this.service.dao.observable(this.path(id), ReactiveDao.ObservableValue)
  }

  async get(id) {
    return this.service.dao.get(this.path(id), ReactiveDao.ObservableValue)
  }

  rangeObservable(range) {
    return this.service.dao.observable(this.rangePath(range), ReactiveDao.ObservableList)
  }

  async rangeGet(range) {
    return this.service.dao.get(this.path(range), ReactiveDao.ObservableList)
  }

  indexRangeObservable(index, range) {
    return this.service.dao.observable(this.indexRangePath(index, range), ReactiveDao.ObservableList)
  }

  async indexRangeGet(index, range) {
    return this.service.dao.get(this.indexRangePath(index, range), ReactiveDao.ObservableList)
  }

  indexObjectObservable(index, range) {
    return this.service.dao.observable(this.indexObjectPath(index, range))
  }

  async indexObjectGet(index, range) {
    return this.service.dao.get(this.indexObjectPath(index, range))
  }

}

module.exports = ReaderModel
