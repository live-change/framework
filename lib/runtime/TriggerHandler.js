const { prepareParameters, processReturn } = require("./params.js")

class TriggerHandler {

  constructor(definition, service) {
    this.definition = definition
    this.service = service
  }

  async execute(parameters, emit) {
    //console.log("PARAMETERS", JSON.stringify(parameters), "DEFN", this.definition.properties)
    let preparedParams = await prepareParameters(parameters, this.definition.properties, this.service)
    //console.log("PREP PARAMS", preparedParams)

    let resultPromise = this.definition.execute({
      ...preparedParams
    }, {
      action: this,
      service: this.service,
      trigger: (...args) => this.service.trigger(...args) /// TODO: collet call traces
    }, emit)

    resultPromise = resultPromise.then(async result => {
      let processedResult = await processReturn(result, this.definition.returns, this.service)
      return processedResult
    })
    resultPromise.catch(error => {
      console.error(`Trigger ${this.definition.name} error `, error)
    })
    return resultPromise
  }
}

module.exports = TriggerHandler