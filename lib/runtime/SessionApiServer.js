const ReactiveDao = require("@live-change/dao")
const Dao = require("./Dao.js")

const { getIp } = require("./utils.js")

function waitForSignal(observable, timeout = 1000, filter = () => true) {
  let observer
  let done = false
  return new Promise((resolve, reject) => {
    observer = (signal, value) => {
      if(done) return
      if(signal != 'set') return reject('unknownSignal')
      if(!value) return
      done = true
      if(filter(value)) resolve(value)
    }
    setTimeout(() => {
      done = true
      reject('timeout')
    }, timeout)
    observable.observe(observer)
  }).finally(() => {
    observable.unobserve(observer)
  })
}

class ApiServer {
  constructor(config) {
    this.config = config
    this.reactiveServer = new ReactiveDao.ReactiveServer( async (sessionId, connection) => {
      let ip = getIp(connection)

      let credentials = {sessionId, ip}

      let disposed = false

      const sessionObservable = config.app.dao.observable(
          ["database", "get", config.app.databaseName, "session", credentials.sessionId ])

      let sessionObserver = {
        set: (newSess) => {
          if(newSess && !disposed) {
            if(JSON.stringify(newSess.roles || []) != JSON.stringify(credentials.roles)
                || JSON.stringify(newSess.user || null) != JSON.stringify(credentials.user || null)) {
              /// User or roles changed, rebuilding dao
              credentials.roles = newSess.roles || []
              credentials.user = newSess.user || null
              console.log("session", sessionId, "  new roles", newSess.roles, "or user", newSess.user, "rebuilding dao!")
              const oldDao = currentDao
              const currentDao = new Dao(config, {...credentials})
              daoProxy.setDao(currentDao)
              oldDao.dispose()
            }
          }
        }
      }

      const daoProxy = new ReactiveDao.ReactiveDaoProxy(null)

      let oldDispose = daoProxy.dispose.bind(daoProxy)
      daoProxy.dispose = () => {
        if(disposed) throw new Error("DAO dispose called twice!")
        disposed = true
        oldDispose()
        sessionObservable.unobserve(sessionObserver)
      }

      sessionObservable.observe(sessionObserver)

      const sess = await waitForSignal(sessionObservable)

      if(!sess) {
        console.log("create session!")
        await config.app.command("session", "createSessionIfNotExists", {
          session: sessionId,
          client: credentials
        })
        console.log("session create returned!")
        await waitForSignal(sessionObservable, 2000, s => !!s)
      }

      if(!daoProxy.dao) throw new Error("internal race condition session error?!")

      return daoProxy
    }, config)
  }

  handleRequest() {

  }

  handleConnection(connection) {
    this.reactiveServer.handleConnection(connection)
  }
}

module.exports = ApiServer
