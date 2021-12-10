const ReactiveDao = require("@live-change/dao")

const profileLog = require("../utils/profileLog.js")

function promiseMap(promise, fn) {
  if(promise.then) return promise.then(fn)
  return fn(promise)
}

function prepareReactiveDaoDefinition(config, clientData) {
  if(!clientData.roles) throw new Error("no roles")
  let dao = {}
  if(config.remote) {
    const remoteList = config.remote(clientData)
    for (let remote of remoteList) {
      dao[remote.name] = {
        type: "remote",
        generator: remote.generator || ReactiveDao.ObservableList
      }
    }
  }
  if(config.local) {
    const local = config.local(clientData)
    for (let localName in local) {
      dao[localName] = {
        type: "local",
        source: local[localName]
      }
    }
  }
  if(config.services) {
    for (let service of config.services) {
      let methods = {}, values = {}
      for(let actionName in service.actions) {
        let action = service.actions[actionName]
        if(!clientData.roles) throw new Error("no roles")
        methods[actionName] = (params) => action.callCommand(params, clientData)
      }
      for(let viewName in service.views) {
        let view = service.views[viewName]
        if(config.profileReads) {
          values[viewName] = {
            async observable(parameters) {
              const observable = await view.observable(parameters, clientData)
              if(!observable.set) {
                console.error("OBSERVABLE WITH NO SET", observable)
              }
              const oldSet = observable.set.bind(observable)
              const oldObserve = observable.observe.bind(observable)
              const oldUnobserve = observable.unobserve.bind(observable)
              let observableTime = Date.now()
              observable.set = (...args) => {
                if(args[0] !== undefined)
                  profileLog.log({ operation: "observeLoaded",
                    serviceName: service.name, viewName, parameters, client: clientData,
                    duration: Date.now() - observableTime })
                return oldSet(...args)
              }
              observable.observe = (...args) => {
                observableTime = Date.now()
                profileLog.log({ operation: "observe",
                  serviceName: service.name, viewName, parameters, client: clientData })
                return oldObserve(...args)
              }
              observable.unobserve = (...args) => {
                profileLog.log({ operation: "unobserve",
                  serviceName: service.name, viewName, parameters, client: clientData,
                  observationDuration: Date.now() - observableTime })
                return oldUnobserve(...args)
              }
              return observable
            },
            async get(parameters) {
              const profileOp = await profileLog.begin({ operation: "get",
                serviceName: service.name, viewName, parameters, client: clientData })
              const promise = view.get(parameters, clientData)
              await profileLog.endPromise(profileOp, promise)
              return promise
            }
          }
        } else {
          values[viewName] = {
            observable(parameters) {
              return view.observable(parameters, clientData)
            },
            get(parameters) {
              return view.get(parameters, clientData)
            }
          }
        }
      }
      if(config.shareDefinition) {
        values['definition'] = {
          observable(parameters) {
            return new ReactiveDao.ObservablePromiseProxy(
                service.app.clientSideDefinition(service, clientData)
                    .then(x => new ReactiveDao.ObservableValue(x))
            )
          },
          async get(parameters) {
            return await service.app.clientSideDefinition(service, clientData)
          }
        }
      }
      dao[service.name] = {
        type: "local",
        source: new ReactiveDao.SimpleDao({ methods, values })
      }
    }
    if(config.shareDefinition) {
      dao['metadata'] = {
        type: "local",
        source: new ReactiveDao.SimpleDao({
          methods: {},
          values: {
            serviceNames: {
              observable(parameters) {
                return new ReactiveDao.ObservableValue(config.services.map(s => s.name))
              },
              async get(parameters) {
                return config.services.map(s => s.name)
              }
            },
            serviceDefinitions: {
              observable(parameters) {
                return new ReactiveDao.ObservablePromiseProxy(
                    Promise.all(
                      config.services.map(service => service.app.clientSideDefinition(service, clientData))
                    ).then(x => new ReactiveDao.ObservableValue(x))
                )
                /*let definitions = config.services.map(s => s.definition.toJSON())
                return new ReactiveDao.ObservableValue(definitions)*/
              },
              async get(parameters) {
                return Promise.all(config.services.map(s => s.app.clientSideDefinition(s, clientData)))
              }
            },
            api: {
              observable(parameters) {
                return new ReactiveDao.ObservablePromiseProxy(
                  Promise.all(
                    config.services.map(service => service.app.clientSideDefinition(service, clientData))
                  ).then(services => new ReactiveDao.ObservableValue({
                    client: { ...clientData, sessionKey: undefined },
                    services
                  }))
                )
              },
              async get(parameters) {
                return Promise.all(
                  config.services.map(service => service.app.clientSideDefinition(service, clientData))
                ).then(services => ({
                  client: { ...clientData, sessionKey: undefined },
                  services
                }))
              }
            }
          }
        })
      }
    }
  }
  dao.protocols = config.protocols || {}
  return dao
}

class RTCMSDao extends ReactiveDao {
  constructor(config, clientData) {
    super(clientData.sessionId, prepareReactiveDaoDefinition(config, clientData))
    //console.log("Created dao with clientData",clientData)
    if( !clientData.roles ) throw new Error("NO ROLES!!")
  }
}

module.exports = RTCMSDao
