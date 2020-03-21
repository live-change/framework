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

  async fetch(parameters, clientData) {
    const context = {
      service: this.service, client: clientData
    }
    parameters = await prepareParameters(parameters, this.definition.properties, this.service)
    return await this.definition.fetch(parameters, context)
  }

  async observable(parameters, clientData) {
    const preparedParameters = await prepareParameters(parameters, this.definition.properties, this.service)
    return this.definition.observable(preparedParameters, context)
  }

  async get(parameters, clientData) {
    const preparedParameters = await prepareParameters(parameters, this.definition.properties, this.service)
    return this.definition.get(preparedParameters, context)
  }
}

module.exports = View
