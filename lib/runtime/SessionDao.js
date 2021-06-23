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
    const sessionDefaults = this.config.sessionDefaults
    this.session = this.credentials.session || this.credentials.sessionId
    console.log("SESSION ID", this.session)
    this.currentDao = null
    this.disposed = false
    if(this.config.createSessionOnUpdate) {
      this.sessionObservable = this.config.app.dao.observable(
        ['database', 'queryObject', this.config.app.databaseName, `(${
          async (input, output, { session, tableName, sessionDefaults }) => {
            const mapper = (obj) => (obj || {
              id: session,
              ...sessionDefaults,
              user: null,
              roles: []
            })
            let storedObj = undefined
            await input.table(tableName).object(session).onChange(async (obj, oldObj) => {              
              const mappedObj = mapper(obj)
              //output.debug("MAPPED DATA", session, "OBJ", mappedObj, "OLD OBJ", storedObj)
              await output.change(mappedObj, storedObj)
              storedObj = mappedObj
            })
          }
        })`, { session: this.session, tableName: 'session_Session', sessionDefaults }]
      )      
    } else {
      this.sessionObservable = this.config.app.dao.observable(
        ["database", "tableObject", this.config.app.databaseName, "session_Session", this.session])
    }
    this.sessionObserver = {
      set: (newSess) => {
        if(newSess && !this.disposed) {
          if(!this.currentDao
              || JSON.stringify(newSess.roles || []) != JSON.stringify(this.credentials.roles)
              || JSON.stringify(newSess.user || null) != JSON.stringify(this.credentials.user || null)) {
            /// User or roles changed, rebuilding dao
            console.log("session", this.session, " old data roles:", this.credentials.roles,
                "user:", this.credentials.user, "rebuilding dao!")
            this.credentials.roles = newSess.roles || []
            this.credentials.user = newSess.user || null
            console.log("session", this.session, " new roles", newSess.roles,
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
      console.log("CREATE SESSION", this.session, this.credentials)
      await this.config.app.command({
        service: "session",
        type: "createSessionIfNotExists",
        parameters: {
          session: this.session
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
