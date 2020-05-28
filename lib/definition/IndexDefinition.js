const PropertyDefinition = require("./PropertyDefinition.js")
const utils = require("../utils.js")

class IndexDefinition {

  constructor(definition) {
    this.properties = {}
    for(let key in definition) this[key] = definition[key]
  }

  toJSON() {
    return {
      ... this,
      function: `${this.function}`
    }
  }

  computeChanges( oldIndexParam ) {
    let oldIndex = JSON.parse(JSON.stringify(oldIndexParam))
    oldIndex.indexes = oldIndex.indexes || {}
    let changes = []
    if(oldIndex.function != `${this.function}`) {
      changes.push({ operation: "deleteIndex", name: this.name })
      changes.push({ operation: "createIndex", name: this.name, index: this.toJSON() })
    }
    if(oldIndex.search && !this.search) changes.push({ operation: "indexSearchDisabled", index: this.name })
    if(!oldIndex.search && this.search) changes.push({ operation: "indexSearchEnabled", index: this.name })
    if(oldIndex.search && this.search && JSON.stringify(oldIndex.search) != JSON.stringify(this.search))
      changes.push({ operation: "indexSearchUpdated", index: this.name })

    return changes
  }



}

module.exports = IndexDefinition
