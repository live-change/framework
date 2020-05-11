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
    if(typeof range != 'object' || Array.isArray(range)) {
      const values = Array.isArray(range) ? range : [range]
      const str = values.map(value => value === undefined ? '' : JSON.stringify(value)).join(':')+'_'
      return this.indexRangePath(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    if(Array.isArray(range)) this.indexRangePath(index, range.join(','))
    return ['database', 'query', this.service.databaseName, `(${
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
    if(Array.isArray(range)) this.indexObjectPath(index, range.join(','))
    return ['database', 'queryObject', this.service.databaseName, `(${
        async (input, output, { tableName, indexName, range }) => {
          const mapper = async (res) => input.table(tableName).object(res.to).get()
          await (await input.index(indexName)).range(range).onChange(async (obj, oldObj) => {
            output.change(obj && await mapper(obj), oldObj && await mapper(oldObj))
          })
        }
    })`, { indexName: this.tableName+'_'+index, tableName: this.tableName, range }]
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
    await this.handleChange(null, res)
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
    //console.log("HADNLE CHANGE", newObj, oldObj)
    //console.log("CHANGE LISTENERS", this.changeListeners)
    for(let listener of this.changeListeners) await listener(newObj, oldObj)
  }

}

module.exports = Model
