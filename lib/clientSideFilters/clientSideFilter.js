
module.exports = function(service, definition, app) {
/*  for(let actionName in service.actions) {
    const action = service.actions[actionName]

  }
  for(let viewName in service.views) {
    const view = service.views[viewName]

  }
  for(let modelName in service.models) {
    const view = service.views[viewName]

  }*/

  delete definition.events
  delete definition.triggers
  delete definition.authenticators
  delete definition.processors
  delete definition.validators
  delete definition.processed
  delete definition.foreignModels
  delete definition.foreignIndexes
  delete definition.config
  delete definition.indexes
}
