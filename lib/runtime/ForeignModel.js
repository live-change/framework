const ReactiveDao = require("@live-change/dao")

class ForeignModel {

  constructor(definition, service) {
    this.serviceName = definition.serviceName
    this.modelName = definition.modelName
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
    if(typeof range != 'object') {
      const str = range.toString()
      return this.indexRangePath(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    if(Array.isArray(range)) this.indexRangePath(index, range.join(','))
    return ['database', 'indexRange', this.service.databaseName, this.tableName+'_'+index, range]
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

}

module.exports = ForeignModel
