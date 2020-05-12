const ModelDefinition = require("./ModelDefinition.js")
const ForeignModelDefinition = require("./ForeignModelDefinition.js")
const ActionDefinition = require("./ActionDefinition.js")
const TriggerDefinition = require("./TriggerDefinition.js")
const ViewDefinition = require("./ViewDefinition.js")
const EventDefinition = require("./EventDefinition.js")
const utils = require("../utils.js")

function createModelProxy(definition, model) {
  return new Proxy(model, {
    get(target, prop, receiver) {
      const runtime  = definition._runtime
      if(runtime) {
        const modelRuntime = runtime.models[model.name]
        if(modelRuntime[prop]) {
          return Reflect.get(modelRuntime, prop, receiver)
        }
      }
      return Reflect.get(target, prop, receiver)
    }
  })
}

function createForeignModelProxy(definition, model) {
  let fk = model.serviceName + "_" + model.modelName
  return new Proxy(model, {
    get(target, prop, receiver) {
      const runtime  = definition._runtime
      if(runtime) {
        const modelRuntime = runtime.foreignModels[fk]
        if(modelRuntime[prop]) {
          return Reflect.get(modelRuntime, prop, receiver)
        }
      }
      return Reflect.get(target, prop, receiver)
    }
  })
}

class ServiceDefinition {

  constructor(definition) {
    this.models = {}
    this.actions = {}
    this.views = {}
    this.events = {}
    this.foreignModels = {}
    this.triggers = {}
    this.validators = {}
    for(let key in definition) this[key] = definition[key]
  }

  model(definition) {
    const model = new ModelDefinition(definition)
    this.models[model.name] = model
    return createModelProxy(this, model)
  }
  
  action(definition) {
    const action = new ActionDefinition(definition)
    this.actions[action.name] = action
    return action
  }

  event(definition) {
    const event = new EventDefinition(definition)
    this.events[event.name] = event
    return event
  }

  view(definition) {
    const view = new ViewDefinition(definition)
    this.views[view.name] = view
    return view
  }

  trigger(definition) {
    const trigger = new TriggerDefinition(definition)
    this.triggers[trigger.name] = trigger
    return trigger
  }

  foreignModel(serviceName, modelName) {
    const model = new ForeignModelDefinition(serviceName, modelName)
    this.foreignModels[serviceName + "_" + modelName] = model
    return createForeignModelProxy(this, model)
  }

  toJSON() {
    let models = {}
    for(let key in this.models) models[key] = this.models[key].toJSON()
    let foreignModels = {}
    for(let key in this.foreignModels) foreignModels[key] = this.foreignModels[key].toJSON()
    let actions = {}
    for(let key in this.actions) actions[key] = this.actions[key].toJSON()
    let events = {}
    for(let key in this.events) events[key] = this.events[key].toJSON()
    let views = {}
    for(let key in this.views) views[key] = this.views[key].toJSON()
    let triggers = {}
    for(let key in this.triggers) triggers[key] = this.triggers[key].toJSON()
    return {
      ...this,
      _runtime: undefined,
      models,
      foreignModels,
      actions,
      views,
      events,
      triggers
    }
  }

  callTrigger(data) {
    if(!this._runtime) throw new Error("triggers can be called only on runtime")
    this._runtime.trigger(data)
  }

  computeChanges( oldModuleParam ) {
    let oldModule = JSON.parse(JSON.stringify(oldModuleParam))
    let changes = []
    changes.push(...utils.crudChanges(oldModule.models || {}, this.models || {},
        "Model", "model", { }))
    return changes
  }

}

module.exports = ServiceDefinition
