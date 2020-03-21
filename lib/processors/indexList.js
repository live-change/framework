const utils = require("../utils.js")

module.exports = function(module, app) {
  for(let modelName in module.models) {
    const model = module.models[modelName]
    for(let propertyName in model.properties) {
      const property = model.properties[propertyName]
      if(property.index) {
        model.indexes = model.indexes || {}
        let name = property.index.name || propertyName
        model.indexes[name] = {
          ...(property.index),
          name,
          property: propertyName
        }
        //console.log(model.indexes)
      }
    }
  }
}
