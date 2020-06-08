const utils = require("../utils.js")

class PropertyDefinition {

  constructor(definition) {
    for(let key in definition) this[key] = definition[key]
    if(definition.properties) {
      for (let propName in definition.properties) {
        const propDefn = definition.properties[propName]
        this.createAndAddProperty(propName, propDefn)
      }
    }
    if(definition.of) {
      this.of = new PropertyDefinition(definition.of)
    }
  }

  createAndAddProperty(name, definition) {
    const property = new PropertyDefinition(definition)
    this.properties[name] = property
  }

  toJSON() {
    let properties = undefined
    if(this.properties) {
      properties = {}
      for (let propName in this.properties) {
        properties[propName] = this.properties[propName].toJSON()
      }
    }
    let json = {
      ...this,
      type: utils.typeName(this.type),
      properties
    }
    if(this.of) {
      json.of = this.of.toJSON()
    }
    
    return json
  }

  computeChanges( oldProperty, params, name) {
    let changes = []
    let typeChanged = false
    if(utils.typeName(this.type) != utils.typeName(oldProperty.type)) typeChanged = true
    if((this.of && utils.typeName(this.of.type)) != (oldProperty.of && utils.typeName(oldProperty.of.type)))
      typeChanged = true
    if(typeChanged) {
      changes.push({
        operation: "changePropertyType",
        ...params,
        property: name,
        ...this
      })
    }
    if(JSON.stringify(this.search) != JSON.stringify(oldProperty.search)) {
      changes.push({
        operation: "changePropertySearch",
        ...params,
        property: name,
        ...this
      })
    }
    return changes
  }

}

module.exports = PropertyDefinition
