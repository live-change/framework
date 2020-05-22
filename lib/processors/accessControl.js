const getAccessMethod = require("./accessMethod.js")

module.exports = function(module, app) {
  for(let actionName in module.actions) {
    const action = module.actions[actionName]
    if(!action.access) continue;
    let access = getAccessMethod(action.access)
    if(access) {
      const oldExec = action.execute
      action.execute = async (...args) => {
        if(!(await access(...args))) {
          console.error("NOT AUTHORIZED ACTION", module.name, actionName)
          throw "notAuthorized"
        }
        return oldExec.apply(action, args)
      }
    }
  }
  for(let viewName in module.views) {
    const view = module.views[viewName]
    if(!view.access) continue;
    let access = getAccessMethod(view.access)
    if(access) {
      const oldObservable = view.observable
      view.observable = async (...args) => {
        if(!(await access(...args))) {
          console.error("NOT AUTHORIZED VIEW", module.name, viewName)
          throw "notAuthorized"
        }
        return oldObservable.apply(view, args)
      }
      const oldGet = view.get
      view.get = async (...args) => {
        if(!(await access(...args))) {
          console.error("NOT AUTHORIZED VIEW", module.name, viewName)
          throw "notAuthorized"
        }
        return oldGet.apply(view, args)
      }
    }
  }
}
