const LcDao = require("@live-change/dao")
const SessionDao = require("./SessionDao.js")

const { getIp } = require("./utils.js")

class SessionApiServer {
  constructor(config) {
    this.config = config
    this.reactiveServer = new LcDao.ReactiveServer(
        (sessionId, connection) => this.daoFactory(sessionId, connection) , config)
  }

  async daoFactory(sessionId, connection) {
    let ip = getIp(connection)
    let credentials = { sessionId, ip, roles: [] }
    if(this.config.authentication) {
      let auth = Array.isArray(this.config.authentication)
          ? this.config.authentication : [this.config.authentication]
      for(let authenticator of auth) {
        if(authenticator) await authenticator(credentials, this.config)
      }
    }
    const dao = new SessionDao(this.config, { ...credentials })
    await dao.start()
    return dao
  }

  handleConnection(connection) {
    this.reactiveServer.handleConnection(connection)
  }
}

module.exports = SessionApiServer
