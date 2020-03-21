const utils = require("../utils.js")

module.exports = async function(service, app) {
  const search = await app.connectToSearch()
  const generateIndexName = (modelName) => {
    return (app.searchIndexPrefix+service.name+"_"+modelName).toLowerCase()
  }

  for(let modelName in service.models) {
    const model = service.models[modelName]
    if (!model.search) continue
    const index = generateIndexName(modelName)
    model.searchIndex = index

    if(!model.onChange) model.onChange = []
    model.onChange.push(async function(id, oldValue, newValue) {
      if(newValue) { // Update or Insert
        //console.log("SEARCH INSERT", index, id, JSON.stringify(newValue, null, ' '))
        search.index({ index, id, body: newValue })
      } else {
        search.delete({ index, id })
      }
    })
  }
}