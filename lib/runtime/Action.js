const { prepareParameters, processReturn, preFilterParameters } = require("./params.js")

class Action {

  constructor(definition, service) {
    this.definition = definition
    this.service = service
  }

  async runCommand(command, emit) {
    let parameters = command.parameters
    //console.log("PARAMETERS", JSON.stringify(parameters), "DEFN", this.definition.properties)
    let preparedParams = await prepareParameters(parameters, this.definition.properties, this.service)
    //console.log("PREP PARAMS", preparedParams)

    let resultPromise = this.definition.execute({
      ...parameters,
      ...preparedParams
    }, {
      action: this,
      service: this.service,
      client: command.client,
      command
    }, emit)

    resultPromise = resultPromise.then(async result => {
      let processedResult = await processReturn(result, this.definition.returns, this.service)
      return processedResult
    })
    resultPromise.catch(error => {
      console.error(`Action ${this.definition.name} error `, error.stack || error)
    })
    return resultPromise
  }

  async callCommand(parameters, clientData) {
    if(!clientData.roles) throw new Error("no roles")
    const command = {
      type: this.definition.name,
      service: this.service.name,
      client: clientData,
      parameters: await preFilterParameters(parameters, this.definition.properties)
    }
    if(parameters._commandId) command.id = parameters._commandId
    console.log("CALL COMMAND", JSON.stringify(command, null, "  "))
    return this.service.app.command(command, this.definition.requestTimeout)
  }
}

module.exports = Action