const LcDao = require("@live-change/dao")
const Dao = require("./Dao.js")

const { waitForSignal } = require('./utils.js')

class SessionDao extends LcDao.DaoProxy {
  constructor(config, credentials) {
    super(null)
    this.config = config
    this.credentials = credentials
  }
  async start() {    
    this.sessionId = this.config.fetchSessionId
     ? await this.config.fetchSessionId(this.credentials)
      : this.credentials.sessionId
    console.log("SESSION ID", this.sessionId)
    this.currentDao = null
    this.disposed = false
    this.sessionObservable = this.config.app.dao.observable(
        ["database", "tableObject", this.config.app.databaseName, "session_Session", this.sessionId ])
    this.sessionObserver = {
      set: (newSess) => {
        if(newSess && !this.disposed) {
          if(!this.currentDao
              || JSON.stringify(newSess.roles || []) != JSON.stringify(this.credentials.roles)
              || JSON.stringify(newSess.user || null) != JSON.stringify(this.credentials.user || null)) {
            /// User or roles changed, rebuilding dao
            console.log("session", this.sessionId, " old data roles:", this.credentials.roles,
                "user:", this.credentials.user, "rebuilding dao!")
            this.credentials.roles = newSess.roles || []
            this.credentials.user = newSess.user || null
            console.log("session", this.sessionId, " new roles", newSess.roles,
                "or user", newSess.user, "rebuilding dao!")
            const oldDao = this.currentDao
            this.currentDao = new Dao(this.config, { ...this.credentials })
            this.setDao(this.currentDao)
            if(oldDao) oldDao.dispose()
          }
        }
      }
    }

    this.sessionObservable.observe(this.sessionObserver)
    let sess = await waitForSignal(this.sessionObservable)

    if(!sess) {
      console.log("create session!")
      console.log("CREATE SESSION", this.sessionId, this.credentials)
      await this.config.app.command({
        service: "session",
        type: "createSessionIfNotExists",
        parameters: {
          session: this.sessionId
        },
        client: this.credentials
      })
      console.log("session create returned!")
      sess = await waitForSignal(this.sessionObservable, 2000, s => !!s)
      console.log("session signaled", sess)
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
