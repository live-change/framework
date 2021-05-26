const ReactiveDao = require("@live-change/dao")
const Dao = require("./Dao.js")

const { getIp } = require("./utils.js")

class ApiServer {
  constructor(config) {
    this.config = config
    this.reactiveServer = new ReactiveDao.ReactiveServer(
        (sessionId, connection) => this.daoFactory(sessionId, connection), config)
  }

  async daoFactory(sessionId, connection)  {
    let ip = getIp(connection)
    let credentials = { sessionId, ip, roles: [] }
    if(this.config.authentication) {
      let auth = Array.isArray(this.config.authentication)
          ? this.config.authentication : [this.config.authentication]
      for(let authenticator of auth) {
        if(authenticator) await authenticator(credentials, this.config)
      }
    }
    return new Dao(this.config, { ...credentials })
  }

  handleConnection(connection) {
    this.reactiveServer.handleConnection(connection)
  }
}

module.exports = ApiServer
