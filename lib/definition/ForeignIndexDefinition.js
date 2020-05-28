const utils = require("../utils.js")

class ForeignIndexDefinition {

  constructor(serviceName, indexName) {
    this.serviceName = serviceName
    this.indexName = indexName
  }

  toJSON() {
    return {
      serviceName: this.serviceName,
      indexName: this.indexName
    }
  }

}

module.exports = ForeignIndexDefinition
