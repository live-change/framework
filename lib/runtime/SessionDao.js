const LcDao = require("@live-change/dao")
const Dao = require("./Dao.js")

const { waitForSignal } = require('./utils.js')

class SessionDao extends LcDao.DaoProxy {
  constructor(config, credentials) {
    super(null)
    this.config = config
    this.credentials = credentials
    this.sessionId = credentials.sessionId
    this.currentDao = null
    this.disposed = false
    this.sessionObservable = this.config.app.dao.observable(
        ["database", "tableObject", this.config.app.databaseName, "session_Session", credentials.sessionId ])
    this.sessionObserver = {
      set: (newSess) => {
        if(newSess && !this.disposed) {
          if(JSON.stringify(newSess.roles || []) != JSON.stringify(this.credentials.roles)
              || JSON.stringify(newSess.user || null) != JSON.stringify(this.credentials.user || null)) {
            /// User or roles changed, rebuilding dao
            console.log("session", this.sessionId, " old data roles:", this.credentials.roles,
                "user:", this.credentials.user, "rebuilding dao!")
            this.credentials.roles = newSess.roles || []
            credentials.user = newSess.user || null
            console.log("session", this.sessionId, " new roles", newSess.roles,
                "or user", newSess.user, "rebuilding dao!")
            const oldDao = this.currentDao
            this.currentDao = new Dao(config, { ...credentials })
            this.setDao(this.currentDao)
            if(oldDao) oldDao.dispose()
          }
        }
      }
    }
  }
  async start() {
    this.sessionObservable.observe(this.sessionObserver)
    const sess = await waitForSignal(this.sessionObservable)

    if(!sess) {
      console.log("create session!")
      await this.config.app.command({
        service: "session",
        type: "createSessionIfNotExists",
        parameters: {
          session: this.sessionId
        },
        client: this.credentials
      })
      console.log("session create returned!")
      await waitForSignal(this.sessionObservable, 2000, s => !!s)
    }

    if(!this.dao) throw new Error("internal race condition session error?!")
  }
  dispose() {
    if(this.disposed) throw new Error("DAO dispose called twice!")
    this.disposed = true
    this.sessionObservable.unobserve(this.sessionObserver)
    super.dispose()
  }
}

module.exports = SessionDao
