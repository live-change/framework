const utils = require("../utils.js")

module.exports = function(module, app) {
  for(let modelName in module.models) {
    const model = module.models[modelName]
    for(let propertyName in model.properties) {
      const property = model.properties[propertyName]
      if(property.reverseRelation) { // X to one
        const reverse = property.reverseRelation
        const targetModel = module.models[utils.typeName(property.type)]
        if(reverse.many) {
          targetModel.createAndAddProperty(reverse.name, {
            type: "Array",
            ... reverse.options
          })
        } else {
          targetModel.createAndAddProperty(reverse.name, {
            type: model,
            ... reverse.options
          })
        }
      }
      if(property.of && property.of.reverseRelation) { // X to many
        const targetModel = module.models[utils.typeName(property.of.type)]
        const reverse = property.of.reverseRelation
        if(reverse.many) {
          targetModel.createAndAddProperty(reverse.name, {
            type: "Array",
            ... reverse.options
          })
        } else {
          targetModel.createAndAddProperty(reverse.name, {
            type: model,
            ... reverse.options
          })
        }
      }
    }
  }
}
