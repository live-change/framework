module.exports = function(service, app) {
  for(let viewName in service.views) {
    const view = service.views[viewName]
    if(view.daoPath) {
      if(!view.observable) view.observable = (...args) => app.dao.observable(view.daoPath(...args))
      if(!view.get) view.get = (...args) => app.dao.get(view.daoPath(...args))
    }
  }
}