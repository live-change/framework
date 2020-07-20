const { typeName } = require("../utils.js")
const SearchIndexer = require("../runtime/SearchIndexer.js")

function generatePropertyMapping(property, search) {
  //console.log("GENERATE PROPERTY MAPPING", property)
  let options = search ? JSON.parse(JSON.stringify(search)) : {}
  if(property.search === false) options.enabled = false
  if(!options.type) {
    switch(typeName(property.type)) {
      case "String": options.type = "text"; break;
      case "Number": options.type = "double"; break;
      case "Date": options.type = "date"; break;
      case "Boolean": options.type = "boolean"; break;
      case "Array": options.type = "array"; break;
      case "Object": options.type = "object"; break;
      default: options.type = "keyword"
    }
  }
  //console.log("GENERATED PROPERTY MAPPING", property, ":", options)
  return options
}

function generatePropertyMappings(property, propName) {
  const bindings = property.search
      ? (Array.isArray(property.search) ? property.search : [ property.search ])
      : [null]
  const mappings = {}
  for(const binding of bindings) {
    const options = generatePropertyMapping(property, binding)
    //console.log("OPTIONS", options)
    if(options.type == 'object' && !options.properties) {
      options.properties = {}
      for(let propName in property.properties) {
        const mappings = generatePropertyMappings(property.properties[propName], propName)
        for(let key in mappings) options.properties[key] = mappings[key]
      }
      options.include_in_root = true
    }
    if(options.type == 'array') {
      if(typeName(property.of) != "Object") {
        return generatePropertyMappings(property.of, propName)
      } else {
        options.type = 'nested'
      }
    }
    delete options.name
    mappings[(binding && binding.name) || propName] = options
  }
  //console.log("PROPERTY MAPPINGS", propName, mappings)
  return mappings
}

function generateMetadata(model) {
  let properties = {}
  for(const propName in model.properties) {
    const mappings = generatePropertyMappings(model.properties[propName], propName)
    for(let key in mappings) properties[key] = mappings[key]
  }
  let settings = (typeof model.search == 'object') ? model.search.settings : undefined
  return {
    settings,
    mappings: {
      _source: {
        enabled: true
      },
      properties: {
        id: { type: "keyword", index: false },
        ...properties
      }
    }
  }
}

async function updateElasticSearch(changes, service, app, force) {

  const generateIndexName = (modelName) => {
    return (app.searchIndexPrefix+service.name+"_"+modelName).toLowerCase()
  }

  const generateTableName = (modelName) => {
    return service.name+"_"+modelName
  }

  console.log("ELASTICSEARCH UPDATER")

  let changesByModel = new Map()
  const addChange = function(modelName, change) {
    let changes = changesByModel.get(modelName)
    if(!changes) changesByModel.set(modelName, [change])
      else changes.push(change)
  }

  /// Group by model
  for(let change of changes) {
    switch (change.operation) {
      case "createModel": addChange(change.model.name, change); break
      case "renameModel": addChange(change.from, change); break
      case "deleteModel": addChange(change.name, change); break
      case "createProperty":
      case "renameProperty":
      case "deleteProperty":
      case "changePropertyType":
      case "searchEnabled":
      case "searchDisabled":
      case "searchUpdated":
      case "changePropertySearch": addChange(change.model, change); break
      default:
    }
  }

  const search = await app.connectToSearch()

  async function getCurrentAlias(modelName) {
    let alias = await search.indices.getAlias({name: generateIndexName(modelName) })
    //console.log("GOT ALIAS", Object.keys(alias.body)[0])
    return Object.keys(alias.body)[0]
  }

  async function setPropertyDefaultValue(currentAlias, propertyName, defaultValue) {
    const req = {
      index: currentAlias,
      body: {
        query: {
          match_all: {}
        },
        script: {
          source: `ctx._source.${propertyName} = ${JSON.stringify(defaultValue)}`,
          lang: 'painless'
        }
      },
      conflicts: 'proceed'
    }
    console.log("UPDATE BY QUERY", req)
    await search.updateByQuery(req).catch(error => {
      console.error("FIELD UPDATE ERROR", error.meta.body.error, error.meta.body.failures)
      throw error
    })
  }

  for(let [model, changes] of changesByModel.entries()) {
    let definition = service.models[model]
    for(let change of changes) {
      if(!definition.search && change.operation!='searchDisabled' && change.operation!='deleteModel') return
      switch (change.operation) {
        case "createModel": {
          if (changes.length != 1) {
            console.error("Bad model operations set", changes)
            throw new Error("createModel prohibits other operations for model")
          }
          const index =  generateIndexName(change.model.name) + '_1'
          const metadata = generateMetadata(service.models[change.model.name])
          console.log("INDEX", index)
          console.log("METADATA", JSON.stringify(metadata,null, "  "))
          await search.indices.create({
            index,
            body: metadata
          })
          await search.indices.putAlias({
            name: generateIndexName(change.model.name),
            index: generateIndexName(change.model.name) + '_1',
          })
        } break
        case "searchEnabled": {
          const index =  generateIndexName(change.model) + '_1'
          const metadata = generateMetadata(service.models[change.model])
          console.log("INDEX", index)
          console.log("METADATA", JSON.stringify(metadata,null, "  "))
          await search.indices.create({
            index,
            body: metadata
          })
          await search.indices.putAlias({
            name: generateIndexName(change.model),
            index,
          })
        } break
        case "deleteModel":
          if(changes.length != 1) {
            console.error("Bad model operations set", changes)
            throw new Error("deleteModel prohibits other operations for model")
          } /// NO BREAK!
        case "searchDisabled": {
          console.log("SEARCH DISABLED")
          const indexName = generateIndexName(change.model)
          const currentAlias = await getCurrentAlias(change.model).catch(e=>null)
          console.log("DELETE INDEX", currentAlias, "AND ALIAS", indexName)
          if(currentAlias) await search.indices.delete({ index: currentAlias }).catch(e=>{})
          await search.indices.deleteAlias({ name: indexName }).catch(e=>{})
          await app.dao.request(['database', 'delete'], app.databaseName, 'searchIndexes', indexName)
        } break
        case "renameModel": {
          const newAlias = generateIndexName(change.to) + '_1'
          await search.indices.create({
            name: newAlias,
            body: generateMetadata(service.models[change.to])
          })
          await search.indices.putAlias({
            name: generateIndexName(change.to),
            index: newAlias
          })
          const currentAlias = await getCurrentAlias(change.from)
          await search.reindex({ body: {
            source: { index: currentAlias },
            dest: { index: newAlias }
          }})
          await search.indices.delete({ name: currentAlias })
          await search.indices.deleteAlias({ name: generateIndexName(change.from) })
        } break
        default:
      }
    }

    let reindex = false
    for(let change of changes) {
      switch (change.operation) {
        case "renameProperty":
        case "deleteProperty":
        case "changePropertyType":
        case "changePropertySearch":
        case "searchUpdated":
          reindex = true;
          break;
        default:
      }
    }


    if(reindex) {
      try {
        const currentAlias = await getCurrentAlias(model)
        const currentVersion = +currentAlias.slice(currentAlias.lastIndexOf("_") + 1)
        const newVersion = currentVersion + 1
        const newAlias = generateIndexName(model) + "_" + newVersion
        const metadata = generateMetadata(service.models[model])
        console.log("METADATA", JSON.stringify(metadata, null, "  "))
        await search.indices.create({
          index: newAlias,
          body: metadata
        })

        for(let change of changes) { /// Create properties before reindex
          if(change.operation == 'createProperty')
            if(typeof change.property.defaultValue != 'undefined')
              await setPropertyDefaultValue(currentAlias, change.name, change.property.defaultValue)
        }

        /*await search.reindex({
          body: {
            source: { index: currentAlias },
            dest: { index: newAlias }
          }
        })*/

        const indexer = new SearchIndexer(app.dao, app.databaseName, 'Table',
            generateTableName(model), search, newAlias, service.models[model])

        await indexer.copyAll()

        await search.indices.putAlias({
          name: generateIndexName(model),
          index: newAlias
        })
        await search.indices.delete({ index: currentAlias })
      } catch(error) {
        if(error.meta) console.error("REINDEXING ERROR", JSON.stringify(error.meta))
          else console.error("REINDEXING ERROR", error)
        throw error
      }
    } else {
      for(let change of changes) {
        switch (change.operation) {
          case "createProperty": {
            let properties = {}
            properties[change.name] = generatePropertyMapping(change.property)
            const currentAlias = await getCurrentAlias(change.model)
            await search.indices.putMapping({
              index: currentAlias,
              body: {properties}
            }).catch(error => {
              console.error('ES ERROR', error.meta.body.error)
              throw error
            })
            if(typeof change.property.defaultValue != 'undefined')
              await setPropertyDefaultValue(currentAlias, change.name, change.property.defaultValue)
          } break
        }
      }
    }
  }

}

module.exports = updateElasticSearch