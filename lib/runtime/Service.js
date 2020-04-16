const Model = require("./Model.js")
const ForeignModel = require("./ForeignModel.js")
const View = require("./View.js")
const Action = require("./Action.js")
const EventHandler = require("./EventHandler.js")
const TriggerHandler = require("./TriggerHandler.js")
const ReactiveDao = require("@live-change/dao")

const eventSourcing = require('@live-change/event-sourcing')

class Service {

  constructor(definition, app) {
    this.definition = definition
    this.app = app
    this.name = definition.name

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

  }

  async start(config) {

    this.definition._runtime = this

    //console.log("DB", this.db)
    //console.log("USERS", await (await r.table("users_User").run(this.db)).toArray())

    //console.log("DEFN", this.definition)
    //console.log("DEFN JSON", JSON.stringify(this.definition.toJSON(), null, "  "))

    if(config.runCommands) this.startCommandExecutor()
    if(config.handleEvents) this.startEventListener()

    //if(config.startEventListener) this.startEventListener()

    console.log("Service", this.definition.name, "started")
  }

  async trigger(data) {
    return this.app.trigger(data)
  }

  async triggerService(service, data) {
    return this.app.triggerService(service, data)
  }

  async startEventListener() {
    if(this.app.splitEvents) {
      this.eventSourcing = new eventSourcing.EventSourcing(this.dao, this.databaseName,
          'events_'+this.name, this.name,
          { filter: (event) => event.service == this.name })
    } else {
      this.eventSourcing = new eventSourcing.EventSourcing(this.dao, this.databaseName,
          'events', this.name,
          { filter: (event) => event.service == this.name })
    }


    for (let eventName in this.events) {
      const event = this.events[eventName]
      this.eventSourcing.addEventHandler(eventName, (ev) => event.execute(ev))
    }

    this.eventSourcing.start()
  }

  async startCommandExecutor() {
    this.commandQueue = new eventSourcing.CommandQueue(this.dao, this.databaseName,
        this.app.splitCommands ? `${this.name}_commands` : 'commands',
        { filter: (command) => command.service == this.name } )
    this.keyBasedCommandQueues = new eventSourcing.KeyBasedExecutionQueues(r => r.key)
    for (let actionName in this.actions) {
      const action = this.actions[actionName]
      if(action.queuedBy) {
        const keyFunction = typeof action.queuedBy == 'function' ? action.queuedBy : (
            Array.isArray(action.queuedBy) ? (c) => JSON.stringify(action.queuedBy.map(k=>c.parameters[k])) :
                (c) => JSON.stringify(c.parameters[keyFunction]) )
        this.commandQueue.addCommandHandler(actionName, async (command) => {
          let emittedEvents = new Map()
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, { commandId: command.id })
              : new SingleEmitQueue(this, { commandId: command.id })
          const routine = async () => {
            const result = await action.runCommand(command, (...args) => emit.emit(...args))
            await emit.commit()
            return result
          }
          routine.key = keyFunction(command)
          return this.keyBasedCommandQueues.queue(routine)
        })
      } else {
        this.commandQueue.addCommandHandler(actionName, async (command) => {
          let emittedEvents = new Map()
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, { commandId: command.id })
              : new SingleEmitQueue(this, { commandId: command.id })
          const result = await action.runCommand(command, (...args) => emit.emit(...args))
          await emit.commit()
          return result
        })
      }
    }

    await this.dao.request(['database', 'createTable'], this.databaseName, 'triggersRoutes').catch(e => 'ok')

    this.triggerQueue = new eventSourcing.CommandQueue(this.dao, this.databaseName,
        this.app.splitTriggers ? `${this.name}_triggers` : 'triggers',
        { filter: (trigger) => trigger.service == this.name } )
    this.keyBasedTriggerQueues = new eventSourcing.KeyBasedExecutionQueues(r => r.key)
    for (let triggerName in this.triggers) {
      const trigger = this.triggers[triggerName]
      await this.dao.request(['database', 'put'], this.databaseName, 'triggersRoutes',
          { id: triggerName+'=>'+this.name, trigger: triggerName, service: this.name })
      if(trigger.queuedBy) {
        const keyFunction = typeof trigger.queuedBy == 'function' ? trigger.queuedBy : (
            Array.isArray(trigger.queuedBy) ? (c) => JSON.stringify(trigger.queuedBy.map(k=>c.parameters[k])) :
                (c) => JSON.stringify(c.parameters[keyFunction]) )
        this.triggerQueue.addCommandHandler(triggerName, async (trig) => {
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, { triggerId: trigger.id })
              : new SingleEmitQueue(this, { triggerId: trigger.id })
          const routine = async () => {
            let result
            try {
              result = await trigger.execute(trig, (...args) => emit.emit(...args))
            } catch (e) {
              console.error(`TRIGGER ${triggerName} ERROR`, e.stack)
              throw e
            }
            await emit.commit()
            return result
          }
          routine.key = keyFunction(trigger)
          return this.keyBasedTriggerQueues.queue(routine)
        })
      } else {
        this.triggerQueue.addCommandHandler(triggerName, async (trig) => {
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, { triggerId: trigger.id })
              : new SingleEmitQueue(this, { triggerId: trigger.id })
          let result
          try {
            result = await trigger.execute(trig, (...args) => emit.emit(...args))
          } catch (e) {
            console.error(`TRIGGER ${triggerName} ERROR`, e.stack)
            throw e
          }
          console.log("TRIG EXECD")
          await emit.commit()
          return result
        })
      }
    }

    this.commandQueue.start()
    this.triggerQueue.start()
  }

}

class SplitEmitQueue {
  constructor(service, flags = {}) {
    this.service = service
    this.flags = flags
    this.emittedEvents = new Map()
  }

  emit(service, event) {
    if(!event) {
      event = service
      if(Array.isArray(event)) {
        let hasServices = false
        for(let ev of event) {
          if(ev.service) hasServices = true
        }
        if(hasServices) {
          for(let ev of event) {
            this.emit(ev)
          }
          return
        }
      } else {
        service = event.service || this.service.name
      }
    }
    let events = this.emittedEvents.get(service)
    if(!events) {
      events = []
      this.emittedEvents.set(service, events)
    }
    if(Array.isArray(event)) {
      for(let ev of event) ev.service = service
      events.push(...event)
    } else {
      event.service = service
      events.push(event)
    }
  }

  async commit() {
    let promises = []
    for(const [service, events] of this.emittedEvents.keys()) {
      promises.push(this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          this.service.name+'_events', { type: 'bucket', events, ...this.flags }))
    }
    return Promise.all(promises)
  }
}

class SingleEmitQueue {
  constructor(service, flags = {}) {
    this.service = service
    this.flags = flags
    this.emittedEvents = []
  }

  emit(service, event) {
    if(!event) {
      event = service
      service = this.service.name
    }
    if(Array.isArray(event)) {
      for(let ev of event) if(!ev.service) ev.service = service
      this.emittedEvents.push(...event)
    } else {
      if(!event.service) event.service = service
      this.emittedEvents.push(event)
    }
  }

  async commit() {
    return this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          'events', { type: 'bucket', events: this.emittedEvents, ...this.flags })
  }
}



module.exports = Service
