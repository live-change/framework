const ReactiveDao = require("@live-change/dao")
const ReactiveDaoWebsocket = require("@live-change/dao-websocket")

const ServiceDefinition = require("./definition/ServiceDefinition.js")

const Service = require("./runtime/Service.js")

const RTCMSDao = require("./runtime/Dao.js")
const ApiServer = require("./runtime/ApiServer.js")
const SessionApiServer = require("./runtime/SessionApiServer.js")

const utils = require("./utils.js")

const { Client: ElasticSearch } = require('@elastic/elasticsearch')

const crypto = require("crypto")

const reverseRelationProcessor = require("./processors/reverseRelation.js")
const indexListProcessor = require("./processors/indexList.js")
const crudGenerator = require("./processors/crudGenerator.js")
const draftGenerator = require("./processors/draftGenerator.js")
const searchIndex = require("./processors/searchIndex.js")
const daoPathView = require("./processors/daoPathView.js")
const accessControl = require("./processors/accessControl.js")
const autoValidation = require("./processors/autoValidation.js")

const databaseUpdater = require("./updaters/database.js")
const elasticSearchUpdater = require("./updaters/elasticsearch.js")

const accessControlFilter = require("./clientSideFilters/accessControlFilter.js")
const clientSideFilter = require("./clientSideFilters/clientSideFilter.js")

class App {

  constructor(env = process.env) {
    this.defaultProcessors = [
        crudGenerator,
        draftGenerator,
        reverseRelationProcessor,
        indexListProcessor,
        daoPathView,
        accessControl,
        autoValidation
    ]
    if(process.env.SEARCH_INDEX_PREFIX) this.defaultProcessors.push(searchIndex)
    this.defaultUpdaters = [
        databaseUpdater
    ]
    if(process.env.SEARCH_INDEX_PREFIX) this.defaultUpdaters.push(elasticSearchUpdater)
    this.defaultClientSideFilters = [
        accessControlFilter,
        clientSideFilter
    ]
    this.defaultPath = "."
    this.dao = new ReactiveDao('app', {
      remoteUrl: env.DB_URL || "http://localhost:9417/api/ws",
      protocols: {
        'ws': ReactiveDaoWebsocket.client
      },
      connectionSettings: {
        queueRequestsWhenDisconnected: true,
        requestSendTimeout: 2000,
        requestTimeout: 5000,
        queueActiveRequestsOnDisconnect: false,
        autoReconnectDelay: 200,
        logLevel: 1
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

    this.databaseName = env.DB_NAME || 'rtapp-test'
  }

  connectToSearch() {
    if(!process.env.SEARCH_INDEX_PREFIX) throw new Error("ElasticSearch not configured")
    if(this.search) return this.search
    this.searchIndexPrefix = process.env.SEARCH_INDEX_PREFIX
    this.search = new ElasticSearch({ node: process.env.SEARCH_URL || 'http://localhost:9200' })
    this.search.info(console.log)
    return this.search
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

  async updateService( service, { path, updaters, force } = {}) {
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
  }

  async startService( serviceDefinition, config ) {
    console.log("Starting service", serviceDefinition.name, "!")
    if(!(serviceDefinition instanceof ServiceDefinition))
      serviceDefinition = new ServiceDefinition(serviceDefinition)
    let service = new Service(serviceDefinition, this)
    await service.start(config || {})
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
    const routes = await this.dao.get(['database', 'tableRange', this.databaseName, 'triggerRoutes',
      { gte: data.type+'_', lte: data.type+'_\xFF\xFF\xFF\xFF' }])
    let promises = []
    for(const route of routes) {
      promises.push(this.triggerService(route.service, data))
    }
    return Promise.all(promises)
  }

  async triggerService(service, data) {
    if(!data.id) data.id = this.generateUid()
    data.service = service
    data.state = 'new'
    if(!data.timestamp) data.timestamp = (new Date()).toISOString()
    const triggersTable = this.splitCommands ? `${this.name}_triggers` : 'triggers'
    const objectObservable = this.dao.observable(
        ['database', 'tableObject', this.databaseName, triggersTable, data.id],
        ReactiveDao.ObservableValue
    )
    await this.dao.request(['database', 'put'], this.databaseName, triggersTable, { ...data })
    let observer
    return new Promise((resolve, reject) => {
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
  }

  async command(data) {
    if(!data.id) data.id = this.generateUid()
    if(!data.service) throw new Error("command must have service")
    if(!data.type) throw new Error("command must have type")
    if(!data.timestamp) data.timestamp = (new Date()).toISOString()
    data.state = 'new'
    const commandsTable = this.splitCommands ? `${data.service}_commands` : 'commands'
    const objectObservable = this.dao.observable(['database', 'tableObject', this.databaseName, commandsTable, data.id],
        ReactiveDao.ObservableValue)
    await this.dao.request(['database', 'put'], this.databaseName, commandsTable, { ...data })
    let observer
    return new Promise((resolve, reject) => {
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
  }

}


module.exports = App
