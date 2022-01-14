const utils = require("../utils.js")
const debug = require('debug')('framework:updaters:db')
async function update(changes, service, app, force) {

  const dao = app.dao
  const database = app.databaseName

  dao.request(['database', 'createTable'], database, 'eventConsumers').catch(e => 'ok')
  dao.request(['database', 'createTable'], database, 'triggerRoutes').catch(e => 'ok')
  dao.request(['database', 'createTable'], database, 'eventReports').catch(e => 'ok')
  if(app.splitEvents) {
    dao.request(['database', 'createLog'], database, service.name+'_events').catch(e => 'ok')
  } else {
    dao.request(['database', 'createLog'], database, 'events').catch(e => 'ok')
  }
  if(app.splitCommands) {
    dao.request(['database', 'createTable'], database, service.name+'_commands').catch(e => 'ok')
  } else {
    dao.request(['database', 'createTable'], database, 'commands').catch(e => 'ok')
  }
  if(app.splitTriggers) {
    dao.request(['database', 'createTable'], database, service.name+'_triggers').catch(e => 'ok')
  } else {
    dao.request(['database', 'createTable'], database, 'triggers').catch(e => 'ok')
  }

  const generateTableName = (modelName) => {
    return service.name+"_"+modelName
  }

  const indexRequestSettings = {
    requestTimeout: 10 * 60 * 1000 // 10 minutes?
  }

  async function createIndex(table, indexName, index) {
    if(table) {
      indexName = table + '_' + indexName
    } else {
      indexName = generateTableName(indexName)
    }

    debug("CREATE INDEX", indexName)

    const options = {
      multi: index.multi || false
    }

    if(index.function) {
      await dao.request(['database', 'createIndex'], database, indexName,
          `(${index.function})`, { ...(index.parameters || {}) })
    } else {
      if(!table) throw new Error("only function indexes are possible without table")
      if(index.multi) {
        if(Array.isArray(index.property)) throw new Error("multi indexes on multiple properties are not supported!")
        await dao.requestWithSettings(indexRequestSettings, ['database', 'createIndex'], database, indexName, `(${
            async function(input, output, { table, property }) {
              const values = (obj) => {
                let at = obj
                for(const p of property) at = at && at[p]
                if(at === undefined) return []
                if(Array.isArray(at)) return at.map(v => JSON.strigify(v))
                return [at]
              }
              await input.table(table).onChange((obj, oldObj) => {
                if(obj && oldObj) {
                  let pointers = obj && new Set(values(obj))
                  let oldPointers = oldObj && new Set(values(oldObj))
                  for(let pointer of pointers) {
                    if(!!oldPointers.has(pointer)) output.change({ id: pointer+'_'+obj.id, to: obj.id }, null)
                  }
                  for(let pointer of oldPointers) {
                    if(!!pointers.has(pointer)) output.change(null, { id: pointer+'_'+obj.id, to: obj.id })
                  }
                } else if(obj) {
                  values(obj).forEach(v => output.change({ id: v+'_'+obj.id, to: obj.id }, null))
                } else if(oldObj) {
                  values(oldObj).forEach(v => output.change(null, { id: v+'_'+oldObj.id, to: oldObj.id }))
                }
              })
            }
        })`, { property: index.property.slice('.'), table })
      } else {
        if(!table) throw new Error("only function indexes are possible without table")
        const properties = (Array.isArray(index.property) ? index.property : [index.property]).map(p => p.split('.'))
        await dao.requestWithSettings(indexRequestSettings, ['database', 'createIndex'], database, indexName, `(${
            async function(input, output, { table,  properties }) {
              const mapper = (obj) => ({
                id: properties.map(path => {
                  let at = obj
                  for(const p of path) at = at && at[p]
                  return at === undefined ? '' : JSON.stringify(at)
                }).join(':')+'_'+obj.id,
                to: obj.id
              })
              await input.table(table).onChange((obj, oldObj) =>
                  output.change(obj && mapper(obj), oldObj && mapper(oldObj)) )
            }
        })`, { properties, table })
      }
    }

    debug("INDEX CREATED!", index)
  }

  debug("DATABASE UPDATER")

  for(let change of changes) {
    debug("PROCESSING CHANGE", change)
    switch(change.operation) {
      case "createModel": {
        const model = change.model
        const tableName = generateTableName(model.name)
        await dao.request(['database', 'createTable'], database, tableName)
        debug("TABLE CREATED!", tableName)
        for(let indexName in model.indexes) {
          const index = model.indexes[indexName]
          await createIndex(tableName, indexName, index)
        }
      } break
      case "renameModel": {
        const from = generateTableName(change.from)
        const to = generateTableName(change.to)
        const model = change.model
        for(let indexName in model.indexes) {
          let indexName = change.name
          indexName = from + '_' + indexName
          await dao.request(['database', 'deleteIndex'], database, indexName)
        }
        await dao.request(['database', 'renameTable'], database, from, to)
        for(let indexName in model.indexes) {
          const index = model.indexes[indexName]
          await createIndex(to, indexName, index)
        }
      } break
      case "deleteModel": {
        const tableName = generateTableName(change.name)
        debug("DELETE TABLE")
        const model = change.model
        for(let indexName in model.indexes) {
          let indexName = change.name
          indexName = tableName + '_' + indexName
          try {
            await dao.request(['database', 'deleteIndex'], database, indexName)
          } catch(e) {
            console.error(e)
          }
        }
        try {
          await dao.request(['database', 'deleteTable'], database, tableName)
        } catch(e) {
          console.error(e)
        }
      } break
      case "createIndex": {
        const table = change.model ? generateTableName(change.model) : null
        const index = change.index
        await createIndex(table, change.name, index)
      } break
      case "renameIndex": {
        const table = change.model ? generateTableName(change.model) : null
        let from = change.from
        let to = change.to
        if(table) {
          from = table + '_' + from
          to = table + '_' + to
        } else {
          from = generateTableName(from)
          to = generateTableName(to)
        }
        await dao.request(['database', 'renameIndex'], database, from, to)
      } break
      case "deleteIndex": {
        const table = change.model ? generateTableName(change.model) : null
        let indexName = change.name
        if(table) {
          indexName = table + '_' + indexName
        } else {
          indexName = generateTableName(indexName)
        }
        try {
          await dao.request(['database', 'deleteIndex'], database, indexName)
        } catch(e) {
          console.error(e)
        }
      } break
      case "createProperty": {
        const table = generateTableName(change.model)
        const property = change.property
        let update = {}
        update[change.name] = property.defaultValue
        debug("CREATE PROPERTY UPDATE", update)
        await dao.request(['database', 'query'], database, `(${
            async (input, output, { table, update }) =>
              await input.table(table).onChange((obj, oldObj) => {
                if(obj) output.table(table).update(obj.id, [{ op: 'merge', value: update }])
              })
        })`, { table, update })
      } break;
      case "renameProperty": {
        const table = generateTableName(change.model)
        await dao.request(['database', 'query'], database, `(${
            async (input, output, { table, from, to }) => {
              const path = from.slice('.')
              await input.table(table).onChange((obj, oldObj) => {
                if(obj) {
                  let value = obj
                  for(let p of path) value = value && value[p]
                  output.table(table).update(obj.id, [
                    { op: 'set', property: to, value },
                    { op: 'delete', property: from }
                  ])
                }
              })
            }
        })`, { table, from: change.from, to: change.to })
      } break;
      case "deleteProperty": {
        const table = generateTableName(change.model)
        /*await dao.request(['database', 'query'], database, `(${
            async (input, output, { table, property }) => {
              await input.log(table).onChange((obj, oldObj) => {
                if(obj) {
                  output.table(table).update(obj.id, [
                    { op: 'delete', property }
                  ])
                }
              })
            }
        })`, { table, property: change.name })*/
      } break;
      default:
    }
  }
  debug("DATABASE UPDATED")

  debug("CHECKING DATABASE INTEGRITY...")
  const indexes = await dao.get(['database', 'indexesList', database])
  for(const indexName in service.indexes) {
    const fullIndexName = generateTableName(indexName)
    if(!indexes.includes(fullIndexName)) {
      debug("index", fullIndexName, "not found! creating...")
      await createIndex(null, indexName, service.index[indexName])
    }
  }
  for(const modelName in service.models) {
    const tableName = generateTableName(modelName)
    const model = service.models[modelName]
    for(const indexName in model.indexes) {
      const fullIndexName = tableName + '_' + indexName
      if(!indexes.includes(fullIndexName)) {
        debug("I", indexes)
        debug("table ", modelName, " index", fullIndexName, "not found! creating...")
        await createIndex(generateTableName(modelName), indexName, model.indexes[indexName])
      }
    }
  }
}

module.exports = update

