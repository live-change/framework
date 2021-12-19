const LcDao = require("@live-change/dao")
const Dao = require("./Dao.js")

const { waitForSignal } = require('./utils.js')

class LiveDao extends LcDao.DaoProxy {
  constructor(config, credentials) {
    super(null)
    this.config = config
    this.initialCredentials = credentials

    this.authenticators = []
    if(this.config.authenticators) {
      this.authenticators = this.config.authenticators.filter(a => a.credentialsObservable)
    }

    this.currentDao = null
    this.disposed = false
    this.started = false
    this.credentials = JSON.parse(JSON.stringify(this.initialCredentials))
  }

  async refreshCredentials() {
    if(!this.started) return /// waiting for start
    const newCredentials = this.computeCredentials()
    if(JSON.stringify(newCredentials) != JSON.stringify(this.credentials)) {
      this.credentials = newCredentials
      this.buildDao()
    }
  }

  computeCredentials() {
    let credentials = JSON.parse(JSON.stringify(this.initialCredentials))
    for(const credentialsObserver of this.credentialsObservations) {
      credentials = {
        ...credentials,
        ...credentialsObserver.credentials,
        roles: [...credentials.roles, ...(credentialsObserver.credentials.roles || [])]
      }
    }
    return credentials
  }

  async start() {
    this.credentialsObservations = this.authenticators.map(authenticator => {
      const result = authenticator.credentialsObservable(this.initialCredentials)
      const observable = result.then ? new LcDao.ObservablePromiseProxy(result) : result
      const observer = {
        set: (data) => {
          console.log("NEW CREDENTIALS", data)
          if(data) {
            const { id, ...newCredentials } = data
            state.credentials = newCredentials
          } else {
            state.credentials = {}
          }
          this.refreshCredentials()
        }
      }
      observable.observe(observer)
      const promise = waitForSignal(observable)
      const state = {
        observable, observer, promise, credentials: {}
      }
      return state
    })

    await Promise.all(this.credentialsObservations.map(observation => observation.promise))

    const newCredentials = this.computeCredentials()
    if(JSON.stringify(newCredentials) != JSON.stringify(this.credentials)) {
      this.credentials = newCredentials
    }
    this.buildDao()
    this.started = true

    if(!this.dao) throw new Error("dao not created?!")
  }

  buildDao() {
    const oldDao = this.currentDao
    this.currentDao = new Dao(this.config, { ...this.credentials })
    this.setDao(this.currentDao)
    if(oldDao) oldDao.dispose()
  }
  dispose() {
    if(this.disposed) throw new Error("DAO dispose called twice!")
    this.disposed = true
    this.started = false
    for(const observation of this.credentialsObservations) {
      observation.observable.unobserver(observation.observer)
    }
    super.dispose()
  }
}

module.exports = LiveDao
