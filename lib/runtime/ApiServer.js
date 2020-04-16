const ReactiveDao = require("@live-change/dao")
const Dao = require("./Dao.js")

const { getIp } = require("./utils.js")

class ApiServer {
  constructor(config) {
    this.config = config
    this.reactiveServer = new ReactiveDao.ReactiveServer( async (sessionId, connection) => {
      let ip = getIp(connection)
      let auth = Array.isArray(this.config.authentication)
          ? this.config.authentication : [this.config.authentication]
      let credentials = { sessionId, ip }
      for(let authenticator of auth) {
        await authenticator(credentials, config)
      }
      return new Dao(config, { ...credentials })
    }, config)
  }

  handleConnection(connection) {
    this.reactiveServer.handleConnection(connection)
  }
}

module.exports = ApiServer
