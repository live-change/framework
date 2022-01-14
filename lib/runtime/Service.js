const Model = require("./Model.js")
const ForeignModel = require("./ForeignModel.js")
const Index = require("./Index.js")
const View = require("./View.js")
const Action = require("./Action.js")
const EventHandler = require("./EventHandler.js")
const TriggerHandler = require("./TriggerHandler.js")

class Service {

  constructor(definition, app) {
    this.definition = definition
    this.app = app
    this.name = definition.name

    this.profileLog = app.profileLog

    this.dao = definition.daoFactory ? definition.daoFactory(app) : app.dao
    this.databaseName = app.databaseName

    this.models = {}
    for(let modelName in this.definition.models) {
      this.models[modelName] = new Model( this.definition.models[modelName], this )
    }

    this.foreignModels = {}
    for(let modelName in this.definition.foreignModels) {
      this.foreignModels[modelName] = new ForeignModel( this.definition.foreignModels[modelName], this )
    }

    this.indexes = {}
    for(let indexName in this.definition.indexes) {
      this.indexes[indexName] = new Index( this.name, this.definition.indexes[indexName].name, this )
    }

    this.foreignIndexes = {}
    for(let indexName in this.definition.indexes) {
      const defn = this.definition.indexes[indexName]
      this.foreignIndexes[indexName] =
          new Index( defn.serviceName, defn.indexName , this )
    }

    this.views = {}
    for(let viewName in this.definition.views) {
      this.views[viewName] = new View( this.definition.views[viewName], this )
    }

    this.actions = {}
    for(let actionName in this.definition.actions) {
      this.actions[actionName] = new Action( this.definition.actions[actionName], this )
    }

    this.triggers = {}
    for(let triggerName in this.definition.triggers) {
      this.triggers[triggerName] = new TriggerHandler( this.definition.triggers[triggerName], this )
    }

    this.events = {}
    for(let eventName in this.definition.events) {
      this.events[eventName] = new EventHandler( this.definition.events[eventName], this )
    }

    this.authenticators = this.definition.authenticators
  }

  async start(config) {
    this.definition._runtime = this

    //console.log("DB", this.db)
    //console.log("USERS", await (await r.table("users_User").run(this.db)).toArray())

    //console.log("DEFN", this.definition)
    //console.log("DEFN JSON", JSON.stringify(this.definition.toJSON(), null, "  "))

    let promises = config.processes.map(proc => proc(this, config))
    await Promise.all(promises)

    for(const beforeStartCallback of this.definition.beforeStartCallbacks) {
      await beforeStartCallback(this)
    }

    console.log("Service", this.definition.name, "started")
  }

  async trigger(data) {
    return this.app.trigger(data)
  }

  async triggerService(service, data) {
    return this.app.triggerService(service, data)
  }

}


module.exports = Service
