const crypto = require("crypto")

const ReactiveDao = require("@live-change/dao")
const ReactiveDaoWebsocket = require("@live-change/dao-websocket")

const ServiceDefinition = require("./definition/ServiceDefinition.js")

const Service = require("./runtime/Service.js")

const profileLog = require("./utils/profileLog.js")

const RTCMSDao = require("./runtime/Dao.js")
const ApiServer = require("./runtime/ApiServer.js")
const SessionApiServer = require("./runtime/SessionApiServer.js")

const { Client: ElasticSearch } = require('@elastic/elasticsearch')

const AnalyticsWriter = require('./utils/AnalyticsWriter.js')

const reverseRelationProcessor = require("./processors/reverseRelation.js")
const indexListProcessor = require("./processors/indexList.js")
const crudGenerator = require("./processors/crudGenerator.js")
const draftGenerator = require("./processors/draftGenerator.js")
const searchIndex = require("./processors/searchIndex.js")
const daoPathView = require("./processors/daoPathView.js")
const fetchView = require("./processors/fetchView.js")
const accessControl = require("./processors/accessControl.js")
const autoValidation = require("./processors/autoValidation.js")
const indexCode = require("./processors/indexCode.js")

const databaseUpdater = require("./updaters/database.js")
const elasticSearchUpdater = require("./updaters/elasticsearch.js")

const accessControlFilter = require("./clientSideFilters/accessControlFilter.js")
const clientSideFilter = require("./clientSideFilters/clientSideFilter.js")

const utils = require('./utils.js')

class App {

  constructor(env = process.env) {
    this.env = env
    this.splitEvents = false

    this.requestTimeout = (+env.DB_REQUEST_TIMEOUT) || 10*1000

    this.defaultProcessors = [
        crudGenerator,
        draftGenerator,
        reverseRelationProcessor,
        indexListProcessor,
        daoPathView,
        fetchView,
        accessControl,
        autoValidation,
        indexCode
    ]
    if(env.SEARCH_INDEX_PREFIX) this.defaultProcessors.push(searchIndex)
    this.defaultUpdaters = [
        databaseUpdater
    ]
    if(env.SEARCH_INDEX_PREFIX) this.defaultUpdaters.push(elasticSearchUpdater)
    this.defaultClientSideFilters = [
        accessControlFilter,
        clientSideFilter
    ]
    this.defaultPath = "."
    const dbDao = new ReactiveDao(process.cwd()+' '+process.argv.join(' '), {
      remoteUrl: env.DB_URL || "http://localhost:9417/api/ws",
      protocols: {
        'ws': ReactiveDaoWebsocket.client
      },
      connectionSettings: {
        queueRequestsWhenDisconnected: true,
        requestSendTimeout: 2000,
        requestTimeout: this.requestTimeout,
        queueActiveRequestsOnDisconnect: false,
        autoReconnectDelay: 200,
        logLevel: 1,
        unobserveDebug: env.UNOBSERVE_DEBUG == "YES"
      },
      database: {
        type: 'remote',
        generator: ReactiveDao.ObservableList
      },
      store: {
        type: 'remote',
        generator: ReactiveDao.ObservableList
      }
    })

    this.dao = dbDao
    if(process.env.DB_CACHE == "YES") this.dao = new ReactiveDao.DaoCache(dbDao)

    this.profileLog = profileLog

    this.databaseName = env.DB_NAME || 'rtapp-test'
  }

  connectToSearch() {
    if(!this.env.SEARCH_INDEX_PREFIX) throw new Error("ElasticSearch not configured")
    if(this.search) return this.search
    this.searchIndexPrefix = this.env.SEARCH_INDEX_PREFIX
    this.search = new ElasticSearch({ node: this.env.SEARCH_URL || 'http://localhost:9200' })
    //this.search.info(console.log)
    return this.search
  }

  connectToAnalytics() {
    if(!this.env.ANALYTICS_INDEX_PREFIX) throw new Error("ElasticSearch analytics not configured")
    if(this.analytics) return this.analytics
    this.analytics = new AnalyticsWriter(this.env.ANALYTICS_INDEX_PREFIX)
    return this.analytics
  }

  createServiceDefinition( definition ) {
    return new ServiceDefinition(definition)
  }

  processServiceDefinition( sourceService, processors ) {
    if(!processors) processors = this.defaultProcessors
    for(let processor of processors) processor(sourceService, this)
  }

  computeChanges( oldServiceJson, newService ) {
    return newService.computeChanges(oldServiceJson)
  }

  async applyChanges(changes, service, updaters, force) {
    console.log("APPLY CHANGES", JSON.stringify(changes, null, '  '))
    updaters = updaters || this.defaultUpdaters
    for(let updater of updaters) {
      await updater(changes, service, this, force)
    }
  }

  async updateService( service, { updaters, force } = {}) {
    const profileOp = await this.profileLog.begin({
      operation: "updateService", serviceName: service.name, force
    })

    this.dao.request(['database', 'createTable'], this.databaseName, 'services').catch(e => 'ok')
    let oldServiceJson = await this.dao.get(['database', 'tableObject', this.databaseName, 'services', service.name])
    if(!oldServiceJson) {
      oldServiceJson = this.createServiceDefinition({name: service.name}).toJSON()
    }
    let changes = this.computeChanges(oldServiceJson, service)

    /// TODO: chceck for overwriting renames, solve by addeding temporary names

    await this.applyChanges(changes, service, updaters || this.defaultUpdaters, force)
    await this.dao.request(['database', 'put'], this.databaseName, 'services',
        { id: service.name , ...service })

    await this.profileLog.end(profileOp)
  }

  async startService( serviceDefinition, config ) {
    console.log("Starting service", serviceDefinition.name, "!")
    const profileOp = await this.profileLog.begin({
      operation: "startService", serviceName: serviceDefinition.name, config
    })
    if(!(serviceDefinition instanceof ServiceDefinition))
      serviceDefinition = new ServiceDefinition(serviceDefinition)
    let service = new Service(serviceDefinition, this)
    await service.start(config || {})
    console.log("service started", serviceDefinition.name, "!")
    await this.profileLog.end(profileOp)
    return service
  }

  async createReactiveDao( config, clientData ) {
    return new RTCMSDao(config, clientData)
  }

  async createApiServer( config ) {
    return new ApiServer({ ...config, app: this })
  }

  async createSessionApiServer( config ) {
    return new SessionApiServer({ ...config, app: this })
  }

  generateUid() {
    return crypto.randomBytes(16).toString("hex")
  }

  async clientSideDefinition( service, client, filters ) {
    let definition = JSON.parse(JSON.stringify(service.definition.toJSON()))
    if(!filters) filters = this.defaultClientSideFilters
    for(let filter of filters) await filter(service, definition, this, client)
    return definition
  }


  async trigger(data) {
    const profileOp = await this.profileLog.begin({
      operation: "callTrigger", triggerType: data.type, id: data.id, by: data.by
    })
    const routes = await this.dao.get(['database', 'tableRange', this.databaseName, 'triggerRoutes',
      { gte: data.type+'=', lte: data.type+'=\xFF\xFF\xFF\xFF' }])
    console.log("TRIGGER ROUTES", routes)
    let promises = []
    for(const route of routes) {
      promises.push(this.triggerService(route.service, { ...data }))
    }
    const promise = Promise.all(promises)
    await this.profileLog.endPromise(profileOp, promise)
    const result = await promise
    console.log("TRIGGER FINISHED!", result)
    return result
  }

  async triggerService(service, data) {

    if(!data.id) data.id = this.generateUid()
    data.service = service
    data.state = 'new'
    if(!data.timestamp) data.timestamp = (new Date()).toISOString()

    const profileOp = await this.profileLog.begin({
      operation: "callTriggerService", triggerType: data.type, service, triggerId: data.id, by: data.by
    })

    const triggersTable = this.splitCommands ? `${this.name}_triggers` : 'triggers'
    const objectObservable = this.dao.observable(
        ['database', 'tableObject', this.databaseName, triggersTable, data.id],
        ReactiveDao.ObservableValue
    )
    await this.dao.request(['database', 'update', this.databaseName, triggersTable, data.id, [
      { op: 'reverseMerge', value: data }
    ]])
    let observer
    const promise = new Promise((resolve, reject) => {
      observer = (signal, value) => {
        if(signal != 'set') return reject('unknownSignal')
        if(!value) return
        if(value.state == 'done') return resolve(value.result)
        if(value.state == 'failed') return reject(value.error)
      }
      objectObservable.observe(observer)
    }).finally(() => {
      objectObservable.unobserve(observer)
    })
    await this.profileLog.endPromise(profileOp, promise)
    return promise
  }

  async command(data, requestTimeout) {
    if(!data.id) data.id = this.generateUid()
    if(!data.service) throw new Error("command must have service")
    if(!data.type) throw new Error("command must have type")
    if(!data.timestamp) data.timestamp = (new Date()).toISOString()
    data.state = 'new'

    const profileOp = await this.profileLog.begin({
      operation: "callCommand", commandType: data.type, service: data.service,
      commandId: data.id, by: data.by, client: data.client
    })

    const commandsTable = this.splitCommands ? `${data.service}_commands` : 'commands'
    const objectObservable = this.dao.observable(
        ['database', 'tableObject', this.databaseName, commandsTable, data.id],
        ReactiveDao.ObservableValue
    )
    await this.dao.request(['database', 'update', this.databaseName, commandsTable, data.id, [
      { op: 'reverseMerge', value: data }
    ]])
    let observer
    const promise = new Promise((resolve, reject) => {
      observer = (signal, value) => {
        if(signal != 'set') return reject('unknownSignal')
        if(!value) return
        if(value.state == 'done') return resolve(value.result)
        if(value.state == 'failed') return reject(value.error)
      }
      objectObservable.observe(observer)
      if(!requestTimeout) {
        requestTimeout = this.requestTimeout
      }
      if(requestTimeout) {
        setTimeout(() => {
          reject('timeout')
        }, requestTimeout)
      }
    }).finally(() => {
      objectObservable.unobserve(observer)
    })

    await this.profileLog.endPromise(profileOp, promise)

    return promise
  }

  async emitEvents(service, events, flags = {}) {
    for(let event of events) {
      if(!event.service) event.service = service
    }
    if(this.splitEvents) {
      let promises = []
      const eventsByService = new Map()
      for(const event of events) {
        let serviceEvents = eventsByService.get(event.service)
        if(!serviceEvents) {
          serviceEvents = []
          eventsByService.set(event.service, serviceEvents)
        }
        serviceEvents.push(event)
      }
      for(const [service, serviceEvents] of eventsByService.entries()) {
        promises.push(this.dao.request(['database', 'putLog'], this.databaseName,
            service+'_events', { type: 'bucket', serviceEvents, ...flags }))
      }
      return Promise.all(promises)
    } else {
      return this.dao.request(['database', 'putLog'], this.databaseName,
          'events', { type: 'bucket', events, ...flags })
    }
  }

  async waitForEvents(reportId, events, timeout) {
    if(events.length == 0) {
      console.log("no events, no need to wait", reportId)
      return
    }
    const [action, id] = reportId.split('_')
    const triggerId = action == 'trigger' ? id : undefined
    const commandId = action == 'command' ? id : undefined
    const profileOp = await this.profileLog.begin({
      operation: "waitForEvents", action: action, commandId, triggerId, reportId, events, timeout
    })
    const promise = new Promise((resolve, reject) => {
      let done = false
      let finishedEvents = []
      const handleError = (message) => {
        console.error(`waitForEvents error: `, message)
        const eventsNotDone = events.filter(event => finished.find(e => e.id == event.id))
        if(eventsNotDone.length > 0) {
          console.error("  pending events:")
          for(const event of eventsNotDone) {
            console.error(`    ${event.id} - type: ${event.type}`)
          }
        }
        reject(message)
        done = true
      }
      const observable = this.dao.observable(
          ['database', 'tableObject', this.databaseName, 'eventReports', reportId]
      )
      const reportsObserver = (signal, data) => {
        if(signal != 'set') {
          handleError(`unknown signal ${signal} with data: ${data}`)
        }
        if(data == null) return /// wait for real data
        if(data.finished) {
          finishedEvents = data.finished
          if(finishedEvents.length >= events.length) {
            const eventsNotDone = events.filter(event => data.finished.find(e => e.id == event.id))
            if(eventsNotDone.length != 0) {
              const eventsDone = events.filter(event => !data.finished.find(e => e.id == event.id))
              console.error("waitForEvents - finished events does not match!")
              console.error("  finished events:")
              for(const event of eventsDone) {
                console.error(`    ${event.id} - type: ${event.type}`)
              }
              console.error("  pending events:")
              for(const event of eventsNotDone) {
                console.error(`    ${event.id} - type: ${event.type}`)
              }
            } else {
              console.log("waiting for events finished", reportId)
              resolve('finished')
              observable.unobserve(reportsObserver)
            }
          }
        }
      }
      console.log("waiting for events", reportId)
      observable.observe(reportsObserver)
      if(Number.isFinite(timeout)) {
        setTimeout(() => {
          if(done) return
          observable.unobserve(reportsObserver)
          handleError('timeout')
        }, timeout)
      }
    })
    await this.profileLog.endPromise(profileOp, promise)
    return promise
  }

  async assertTime(taskName, duration, task, ...data) {
    const profileOp = await this.profileLog.begin({ operation: 'assertTime', taskName })
    const taskTimeout = setTimeout(() => {
      console.log(`TASK ${taskName} TIMEOUT`, ...data)
      this.profileLog.end({ ...profileOp, result: "timeout" })
    }, duration)
    try {
      const result = await task()
      return result
    } finally {
      clearTimeout(taskTimeout)
      await this.profileLog.end({ ...profileOp, result: "done" })
    }
  }

  async close() {
    this.dao.dispose()
  }

}


module.exports = App
module.exports.rangeProperties = utils.rangeProperties
