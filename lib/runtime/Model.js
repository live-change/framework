const ReaderModel = require("./ReaderModel.js")

class Model extends ReaderModel {

  constructor(definition, service) {
    super(service.name, definition.name, service)
    this.service = service
    this.definition = definition
    this.changeListeners = this.definition.onChange || []
  }

  async update(id, data, options) {
    const operations = Array.isArray(data) ? data : [{ op:'merge', property: null, value: data }]
    const res = await this.service.dao.request(
        ['database', 'update'], this.service.databaseName, this.tableName, id, operations)
    const [newObj, oldObj] = res
    await this.handleChange(newObj, oldObj)
    return [newObj, oldObj]
  }

  async delete(id, options) {
    id = id.id || id
    const res = await this.service.dao.request(
        ['database', 'delete'], this.service.databaseName, this.tableName, id)
    await this.handleChange(null, res)
  }

  async create(data, options) {
    if(!data.id) throw new Error("id must be generated before creation of object")
    let prepData = {...data}
    for(let key in this.definition.properties) {
      if(!prepData.hasOwnProperty(key)) {
        let prop = this.definition.properties[key]
        if (prop.hasOwnProperty('defaultValue')) {
          prepData[key] = prop.defaultValue
        }
      }
    }
    //console.log("CREATE PREP DATA", prepData)
    const res = await this.service.dao.request(
        ['database', 'put'], this.service.databaseName, this.tableName, prepData)
    await this.handleChange(prepData, res)
    return res
  }

  async handleChange(newObj, oldObj) {
    //console.log("HADNLE CHANGE", newObj, oldObj)
    //console.log("CHANGE LISTENERS", this.changeListeners)
    for(let listener of this.changeListeners) await listener(newObj, oldObj)
  }

}

module.exports = Model
