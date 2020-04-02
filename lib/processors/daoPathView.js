const ReactiveDao = require("@live-change/dao")

module.exports = function(service, app) {
  for(let viewName in service.views) {
    const view = service.views[viewName]
    if(view.daoPath) {
      if(!view.observable) view.observable = async (...args) => {
        const path = await view.daoPath(...args)
        if(path === null) return new ReactiveDao.ObservableValue(null)
        return app.dao.observable(path)
      }
      if(!view.get) view.get = async (...args) => {
        const path = await view.daoPath(...args)
        if(path === null) return null
        return app.dao.get(path)
      }
    }
  }
}