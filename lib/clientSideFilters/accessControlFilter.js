const getAccessMethod = require("../processors/accessMethod.js")

module.exports = async function(service, definition, app, client) {

  for(let actionName in definition.actions) {
    const action = service.definition.actions[actionName]
    if(!action.access) continue;
    let access = getAccessMethod(action.access)

    if(access) {
      try {
        if(!await access({ visibilityTest: true }, { client, service, action, visibilityTest: true })) {
          delete definition.actions[actionName]
        }
      } catch(e) {
        console.error(`Access function in action ${actionName} returned error for visibility test with no parameters`)
        console.error(e)
        delete definition.actions[actionName]
      }
    }
  }

  for(let viewName in definition.views) {
    const view = service.definition.views[viewName]
    if(!view.access) continue;
    let access = getAccessMethod(view.access)

    if(access) {
      try {
        if(!await access({ visibilityTest: true }, { client, service, view, visibilityTest: true })) {
          delete definition.views[viewName]
        }
      } catch(e) {
        console.error(`Access function in view ${viewName} returned error for visibility test with no parameters`)
        console.error(e)
        delete definition.views[viewName]
      }
    }
  }

  for(let modelName in definition.models) {
    const model = service.definition.models[modelName]
    if(!model.access) continue;
    let access = getAccessMethod(model.access)

    if(access) {
      try {
        if(!await access({ visibilityTest: true }, { client, service, model, visibilityTest: true })) {
          delete definition.models[modelName]
        }
      } catch(e) {
        console.error(`Access function in model ${modelName} returned error for visibility test with no parameters`)
        console.error(e)
        delete definition.models[modelName]
      }
    }
  }

  definition.credentials = {
    roles: client.roles,
    user: client.user
  }

}
