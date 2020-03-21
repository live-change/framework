const utils = require("../utils.js")

class ForeignModelDefinition {

  constructor(serviceName, modelName) {
    this.serviceName = serviceName
    this.modelName = modelName
  }

  getTypeName() {
    return this.serviceName+':'+this.modelName
  }

  toJSON() {
    return {
      serviceName: this.serviceName,
      modelName: this.modelName
    }
  }

}

module.exports = ForeignModelDefinition
