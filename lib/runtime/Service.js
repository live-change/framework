const Model = require("./Model.js")
const ForeignModel = require("./ForeignModel.js")
const Index = require("./Index.js")
const View = require("./View.js")
const Action = require("./Action.js")
const EventHandler = require("./EventHandler.js")
const TriggerHandler = require("./TriggerHandler.js")
const SearchIndexer = require("./SearchIndexer.js")
const ReactiveDao = require("@live-change/dao")

const EventSourcing = require('../utils/EventSourcing.js')
const CommandQueue = require('../utils/CommandQueue.js')
const KeyBasedExecutionQueues = require('../utils/KeyBasedExecutionQueues.js')

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

  }

  async start(config) {

    this.definition._runtime = this

    //console.log("DB", this.db)
    //console.log("USERS", await (await r.table("users_User").run(this.db)).toArray())

    //console.log("DEFN", this.definition)
    //console.log("DEFN JSON", JSON.stringify(this.definition.toJSON(), null, "  "))

    let promises = []
    if(config.runCommands) promises.push(this.startCommandExecutor())
    if(config.handleEvents) promises.push(this.startEventListener())
    if(config.indexSearch) promises.push(this.startSearchIndexer())

    await Promise.all(promises)

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
      this.eventSourcing = new EventSourcing(this.dao, this.databaseName,
          'events_'+this.name, this.name,
          { filter: (event) => event.service == this.name })
    } else {
      this.eventSourcing = new EventSourcing(this.dao, this.databaseName,
          'events', this.name,
          { filter: (event) => event.service == this.name })
    }


    for (let eventName in this.events) {
      const event = this.events[eventName]
      this.eventSourcing.addEventHandler(eventName, async (ev, bucket) => {
        await event.execute(ev, bucket)
      })
      this.eventSourcing.onBucketEnd = async (bucket, handledEvents) => {
        if(bucket.reportFinished && handledEvents.length > 0) {
          await this.dao.request(['database', 'update'], this.databaseName, 'eventReports', bucket.reportFinished,[
            { op: "mergeSets", property: 'finished', values: handledEvents.map(ev => ({ id: ev.id, type: ev.type })) }
          ])
        }
      }
    }

    this.eventSourcing.start()
  }

  async startCommandExecutor() {
    this.commandQueue = new CommandQueue(this.dao, this.databaseName,
        this.app.splitCommands ? `${this.name}_commands` : 'commands', this.name)
    this.keyBasedCommandQueues = new KeyBasedExecutionQueues(r => r.key)
    for (let actionName in this.actions) {
      const action = this.actions[actionName]
      if(action.definition.queuedBy) {
        const queuedBy = action.definition.queuedBy
        const keyFunction = typeof queuedBy == 'function' ? queuedBy : (
            Array.isArray(queuedBy) ? (c) => JSON.stringify(queuedBy.map(k=>c[k])) :
                (c) => JSON.stringify(c[queuedBy]) )
        this.commandQueue.addCommandHandler(actionName, async (command) => {
          const reportFinished = action.definition.waitForEvents ? 'command_'+command.id : undefined
          const flags = { commandId: command.id, reportFinished }
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, flags)
              : new SingleEmitQueue(this, flags)
          const routine = async () => {
            const result = await this.app.assertTime('command '+action.definition.name, 10000,
                () => action.runCommand(command, (...args) => emit.emit(...args)), command)
            const events = await emit.commit()
            if(action.definition.waitForEvents)
              await this.app.waitForEvents(reportFinished, events, action.definition.waitForEvents)
            return result
          }
          routine.key = keyFunction(command)
          return this.keyBasedCommandQueues.queue(routine)
        })
      } else {
        this.commandQueue.addCommandHandler(actionName, async (command) => {
          const reportFinished = action.definition.waitForEvents ? 'command_'+command.id : undefined
          const flags = { commandId: command.id, reportFinished }
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, flags)
              : new SingleEmitQueue(this, flags)
          const result = await this.app.assertTime('command '+action.definition.name, 10000,
              () => action.runCommand(command, (...args) => emit.emit(...args)), command)
          const events = await emit.commit()
          if(action.definition.waitForEvents)
            await this.app.waitForEvents(reportFinished, events, action.definition.waitForEvents)
          return result
        })
      }
    }

    await this.dao.request(['database', 'createTable'], this.databaseName, 'triggerRoutes').catch(e => 'ok')

    this.triggerQueue = new CommandQueue(this.dao, this.databaseName,
        this.app.splitTriggers ? `${this.name}_triggers` : 'triggers', this.name )
    this.keyBasedTriggerQueues = new KeyBasedExecutionQueues(r => r.key)
    for (let triggerName in this.triggers) {
      const trigger = this.triggers[triggerName]
      await this.dao.request(['database', 'put'], this.databaseName, 'triggerRoutes',
          { id: triggerName+'=>'+this.name, trigger: triggerName, service: this.name })
      if(trigger.definition.queuedBy) {
        const queuedBy = trigger.definition.queuedBy
        const keyFunction = typeof queuedBy == 'function' ? queuedBy : (
            Array.isArray(queuedBy) ? (c) => JSON.stringify(queuedBy.map(k=>c[k])) :
                (c) => JSON.stringify(c[queuedBy]) )
        this.triggerQueue.addCommandHandler(triggerName, async (trig) => {
          console.log("QUEUED TRIGGER STARTED", trig)
          const reportFinished = trigger.definition.waitForEvents ? 'trigger_'+trig.id : undefined
          const flags = { triggerId: trig.id, reportFinished }
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, flags)
              : new SingleEmitQueue(this, flags)
          const routine = async () => {
            let result
            try {
              console.log("TRIGGERED!!", trig)
              result = await this.app.assertTime('trigger '+trigger.definition.name, 10000,
                  () => trigger.execute(trig, (...args) => emit.emit(...args)), trig)
              console.log("TRIGGER DONE!", trig)
            } catch (e) {
              console.error(`TRIGGER ${triggerName} ERROR`, e.stack)
              throw e
            }
            const events = await emit.commit()
            if(trigger.definition.waitForEvents)
              await this.app.waitForEvents(reportFinished, events, trigger.definition.waitForEvents)
            return result
          }
          try {
            routine.key = keyFunction(trig)
          } catch(e) {
            console.error("QUEUE KEY FUNCTION ERROR", e)
          }
          console.log("TRIGGER QUEUE KEY", routine.key)
          return this.keyBasedTriggerQueues.queue(routine)
        })
      } else {
        this.triggerQueue.addCommandHandler(triggerName, async (trig) => {
          const reportFinished = trigger.definition.waitForEvents ? 'trigger_'+trig.id : undefined
          const flags = { triggerId: trig.id, reportFinished }
          const emit = this.app.splitEvents
              ? new SplitEmitQueue(this, flags)
              : new SingleEmitQueue(this, flags)
          let result
          try {
            result = await this.app.assertTime('trigger '+trigger.definition.name, 10000,
                () => trigger.execute(trig, (...args) => emit.emit(...args)), trig)
          } catch (e) {
            console.error(`TRIGGER ${triggerName} ERROR`, e.stack)
            throw e
          }
          const events = await emit.commit()
          if(trigger.definition.waitForEvents)
            await this.app.waitForEvents(reportFinished, events, trigger.definition.waitForEvents)
          return result
        })
      }
    }

    this.commandQueue.start()
    this.triggerQueue.start()
  }

  async startSearchIndexer() {
    console.log("starting search indexer!")
    await this.dao.request(['database', 'createTable'], this.databaseName, 'searchIndexes').catch(e => 'ok')

    this.searchIndexers = []

    const elasticsearch = this.app.connectToSearch()

    for(const modelName in this.models) {
      const model = this.models[modelName]
      const indexName = model.definition.searchIndex
      if(!indexName) continue
      const indexer = new SearchIndexer(
          this.dao, this.databaseName, 'Table', model.tableName, elasticsearch, indexName, model.definition
      )
      this.searchIndexers.push(indexer)
    }

    for(const indexName in this.indexes) {
      const index = this.indexes[indexName]
      const indexName = index.definition.searchIndex
      if(!indexName) continue
      const indexer = new SearchIndexer(
          this.dao, this.databaseName, 'Index', model.tableName, elasticsearch, indexName, index.definition
      )
      this.searchIndexers.push(indexer)
    }

    const promises = []
    for(const searchIndexer of this.searchIndexers) {
      promises.push(searchIndexer.start())
    }
    await Promise.all(promises)
    console.log("search indexer started!")
  }

}

class SplitEmitQueue {
  constructor(service, flags = {}) {
    this.service = service
    this.flags = flags
    this.emittedEvents = new Map()
    this.commited = false
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
    let events
    if(!this.commited) {
      events = this.emittedEvents.get(service)
      if(!events) {
        events = []
        this.emittedEvents.set(service, events)
      }
    } else {
      events = []
    }
    if(Array.isArray(event)) {
      for(let ev of event) ev.service = service
      events.push(...event)
    } else {
      event.service = service
      events.push(event)
    }
    if(this.commited) {
      this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          this.service.name+'_events', { type: 'bucket', events, ...this.flags })
    }
  }

  async commit() {
    let promises = []
    this.commited = true
    let allEvents = []
    for(const [service, events] of this.emittedEvents.keys()) {
      promises.push(this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          this.service.name+'_events', { type: 'bucket', events, ...this.flags }))
      allEvents.push(...events)
    }
    await Promise.all(promises)
    return events
  }
}

class SingleEmitQueue {
  constructor(service, flags = {}) {
    this.service = service
    this.flags = flags
    this.emittedEvents = []
    this.commited = false
  }

  emit(service, event) {
    if(!event) {
      event = service
      service = this.service.name
    }
    let events
    if(!this.commited) {
      events = this.emittedEvents
    } else {
      events = []
    }
    if(Array.isArray(event)) {
      for(let ev of event) if(!ev.service) ev.service = service
      events.push(...event)
    } else {
      if(!event.service) event.service = service
      events.push(event)
    }
    if(this.commited) {
      this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          'events', { type: 'bucket', events, ...this.flags })
    }
  }

  async commit() {
    this.commited = true
    await this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          'events', { type: 'bucket', events: this.emittedEvents, ...this.flags })
    return this.emittedEvents
  }
}



module.exports = Service
