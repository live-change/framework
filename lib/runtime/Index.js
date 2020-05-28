const ReactiveDao = require("@live-change/dao")

class Index {

  constructor(serviceName, indexName, service) {
    this.serviceName = serviceName
    this.indexName = indexName
    this.service = service
    this.dbIndexName = this.serviceName + "_" + this.indexName
  }

  path(id) {
    return ['database', 'indexObject', this.service.databaseName, this.dbIndexName, id]
  }

  rangePath(range = {}) {
    if(typeof range != 'object') {
      const str = range.toString()
      return this.rangePath({ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    if(Array.isArray(range)) this.rangePath(range.join(','))
    return ['database', 'indexRange', this.service.databaseName, this.dbIndexName, range]
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

}

module.exports = Index
