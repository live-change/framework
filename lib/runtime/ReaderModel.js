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

  indexRangePath(index, range = {}, options = {}) {
    if(typeof range != 'object' || Array.isArray(range)) {
      const values = Array.isArray(range) ? range : [range]
      const str = values.map(value => value === undefined ? '' : JSON.stringify(value)).join(':')+'_'
      return this.indexRangePath(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    return ['database', 'query', this.service.databaseName, `(${
        async (input, output, { tableName, indexName, range, options }) => {
          if(range.reverse) output.setReverse(true)
          const objectStates = new Map()
          let objectMapper = obj => obj
          if(options.idFromIndex) objectMapper = (obj, ind) => obj && ({ ...obj, id: ind.id, to: ind.to })
          const mapper = async (res) => input.table(tableName).object(res.to).get()
          await (await input.index(indexName)).range(range).onChange(async (obj, oldObj) => {
            if(obj && !oldObj) {
              const data = objectMapper(await mapper(obj), obj)
              if(data) output.change(data, null)
            }
            if(obj && obj.to) {
              let objectState = objectStates.get(obj.to)
              if(!objectState) {
                objectState = { data: undefined, refs: 1 }
                objectState.reader = input.table(tableName).object(obj.to)
                const ind = obj
                objectState.observer = await objectState.reader.onChange(async obj => {
                  const data = obj && objectMapper(obj, ind)
                  const oldData = objectState.data
                  output.change(data, oldData)
                  if(data) {
                    objectState.data = data
                  } else if(oldObj) {
                    objectState.data = null
                  }
                })
                objectStates.set(obj.to, objectState)
              } else {
                objectState.refs ++
              }
            } else if(oldObj && oldObj.to) {
              let objectState = objectStates.get(oldObj.to)
              if(objectState) {
                objectState.refs --
                if(objectState.refs <= 0) {
                  objectState.reader.unobserve(objectState.observer)
                  objectStates.delete(oldObj.to)
                  output.change(null, objectState.data)
                }
              }
            }
          })
        }
    })`, { indexName: this.tableName+'_'+index, tableName: this.tableName, range, options }]
  }

  indexObjectPath(index, range = {}) {
    if(typeof range != 'object' || Array.isArray(range)) {
      const values = Array.isArray(range) ? range : [range]
      const str = values.map(value => value === undefined ? '' : JSON.stringify(value)).join(':')+'_'
      return this.indexObjectPath(index,{ gte: str, lte: str+'\xFF\xFF\xFF\xFF' })
    }
    return ['database', 'queryObject', this.service.databaseName, `(${
        async (input, output, { tableName, indexName, range }) => {
          let objectReader = null
          let objectObserver = null
          let object = null
          const objectChangeCallback = async (obj, oldObj) => {
            output.change(obj, object)
            object = obj
          }
          await (await input.index(indexName)).range(range).onChange(async (obj, oldObj) => {
            if(objectObserver) await objectReader.unobserve(objectObserver)
            if(obj.to) {
              objectReader = input.table(tableName).object(obj.to)
              objectObserver = await objectReader.onChange(objectChangeCallback)
            } else {
              objectObserver = null
              objectReader = null
              output.change(null, object)
            }
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
    return this.service.dao.get(this.rangePath(range), ReactiveDao.ObservableList)
  }

  indexRangeObservable(index, range, options = {}) {
    return this.service.dao.observable(this.indexRangePath(index, range, options), ReactiveDao.ObservableList)
  }

  async indexRangeGet(index, range, options = {}) {
    return this.service.dao.get(this.indexRangePath(index, range, options), ReactiveDao.ObservableList)
  }

  indexObjectObservable(index, range, options = {}) {
    return this.service.dao.observable(this.indexObjectPath(index, range, options))
  }

  async indexObjectGet(index, range) {
    return this.service.dao.get(this.indexObjectPath(index, range))
  }

  condition(id, condition = x => !!x, timeout = 10000) {
    return new Promise((resolve, reject) => {
      const observable = this.observable(id)
      const timeoutId = setTimeout(() => {
        observable.unobserve(observer)
        return reject(new Error('timeout'))
      }, timeout)
      const observer = (signal, value) => {
        if(signal != 'set') {
          observable.unobserve(observer)
          clearTimeout(timeoutId)
          return reject(new Error(`unknown signal ${signal}`))
        }
        if(condition(signal)) {
          observable.unobserve(observer)
          clearTimeout(timeoutId)
          return resolve(value)
        }
      }
      observable.observe(observer)
    })
  }

}

module.exports = ReaderModel
