const { getValidators, validate } = require('../utils/validation.js')

module.exports = function(service, app) {
  for(let actionName in service.actions) {
    const action = service.actions[actionName]
    if(action.skipValidation) continue
    const validators = getValidators(action, service, action)
    if(Object.keys(validators).length > 0) {
      const oldExec = action.execute
      action.execute = async (...args) => {
        const context = args[1]
        return validate(args[0], validators, { source: action, action, service, app, ...context }).then(() =>
          oldExec.apply(action, args)
        )
      }
    }
  }
  for(let viewName in service.views) {
    const view = service.views[viewName]
    if(view.skipValidation) continue
    const validators = getValidators(view, service, view)
    if(Object.keys(validators).length > 0) {
      if (view.read && !view.fetch) {
        const oldRead = view.read
        view.read = async (...args) => {
          const context = args[1]
          return validate(args[0], validators, { source: view, view, service, app, ...context }).then(() =>
              oldRead.apply(view, args)
          )
        }
      } else {
        const oldFetch = view.fetch
        view.fetch = async (...args) => {
          const context = args[1]
          return validate(args[0], validators, { source: view, view, service, app, ...context }).then(() =>
              oldFetch.apply(view, args)
          )
        }
      }
    }
  }
}
