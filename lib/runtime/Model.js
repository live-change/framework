const ReactiveDao = require("@live-change/dao")

class Model {

  constructor(definition, service) {
    this.service = service
    this.definition = definition
    this.tableName = service.name + "_" + this.definition.name
    this.changeListeners = this.definition.onChange || []
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

  async update(id, data, options) {
    const operations = Array.isArray(data) ? data : [{ op:'merge', property: null, value: data }]
    const res = await this.service.dao.request(
        ['database', 'update'], this.service.databaseName, this.tableName, id, operations)
    const [newObj, oldObj] = res
    await this.handleChange(newObj, oldObj)
    return [newObj, oldObj]
  }

  async delete(id, options) {
    id = id.id || id
    const res = await this.service.dao.request(
        ['database', 'delete'], this.service.databaseName, this.tableName, id)
    await this.handleChange(res.changes)
  }

  async create(data, options) {
    if(!data.id) throw new Error("id must be generated before creation of object")
    let prepData = {...data}
    for(let key in this.definition.properties) {
      if(!prepData.hasOwnProperty(key)) {
        let prop = this.definition.properties[key]
        if (prop.hasOwnProperty('defaultValue')) {
          prepData[key] = prop.defaultValue
        }
      }
    }
    //console.log("CREATE PREP DATA", prepData)
    const res = await this.service.dao.request(
        ['database', 'put'], this.service.databaseName, this.tableName, prepData)
    await this.handleChange(prepData, res)
    return res
  }

  async handleChange(newObj, oldObj) {
    //let id = (change.old_val && change.old_val.id) || (change.new_val && change.new_val.id)
    console.log("CHANGE LISTENERS", this.changeListeners)
    for(let listener of this.changeListeners) await listener(newObj, oldObj)
  }

}

module.exports = Model