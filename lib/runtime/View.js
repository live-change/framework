const ReactiveDao = require('@live-change/dao')

const { prepareParameters, processReturn } = require("./params.js")

class View {
  constructor(definition, service) {
    this.service = service
    this.definition = definition
    this.name = definition.name
  }

  async prepareRequest(parameters, clientData, queryType) {
    const context = {
      service: this.service, client: clientData
    }
    //console.log("PARAMETERS", JSON.stringify(parameters), "DEFN", this.definition.properties)
    parameters = await prepareParameters(parameters, this.definition.properties, this.service)
    //console.log("PREPARED PARAMETERS", parameters)
    return await this.definition.read(parameters, context, queryType, this.service)
  }

  async observable(parameters, clientData) {
    const context = {
      service: this.service, client: clientData
    }
    const preparedParameters = await prepareParameters(parameters, this.definition.properties, this.service)
    const observable = this.definition.observable(preparedParameters, context)
    if(observable === null) return new ReactiveDao.ObservableValue(null)
    return observable
  }

  async get(parameters, clientData) {
    const context = {
      service: this.service, client: clientData
    }
    const preparedParameters = await prepareParameters(parameters, this.definition.properties, this.service)
    if(!this.definition.get) throw new Error(`get method undefined in view ${this.service.name}/${this.name}`)
    return this.definition.get(preparedParameters, context)
  }
}

module.exports = View
