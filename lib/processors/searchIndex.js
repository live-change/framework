const utils = require("../utils.js")

module.exports = async function(service, app) {
  const search = await app.connectToSearch()
  const generateIndexName = (modelName) => {
    return (app.searchIndexPrefix+service.name+"_"+modelName).toLowerCase()
  }

  for(let modelName in service.models) {
    const model = service.models[modelName]
    if (!model.search) continue
    const searchIndex = generateIndexName(modelName)
    model.searchIndex = searchIndex
  }
  for(let indexName in service.indexes) {
    const index = service.models[indexName]
    if (!index.search) continue
    const searchIndex = generateIndexName(indexName)
    model.searchIndex = searchIndex
  }
}