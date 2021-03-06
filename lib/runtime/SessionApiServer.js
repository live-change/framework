const LcDao = require("@live-change/dao")
const SessionDao = require("./SessionDao.js")
const cookie = require('cookie')

const { getIp } = require("./utils.js")

class SessionApiServer {
  constructor(config) {
    this.config = config
    this.reactiveServer = new LcDao.ReactiveServer(
      (credentials, connection) => {
        const ip = getIp(connection)
        if(this.config.fastAuth) {
          if(typeof this.config.fastAuth == 'function') {
            credentials = this.config.fastAuth(connection)
          } else {
            const cookies = cookie.parse(connection.headers.cookie || '')
            const sessionKey = cookies.sessionKey
            if(!sessionKey) throw new Error('session key undefined')
            credentials = { sessionKey }
          }
        }
        return this.daoFactory(credentials, ip)
      }, config)
  }

  async daoFactory(credentialsp, ip) {
    let credentials = { ...credentialsp, ip, roles: [] }
    if(this.config.authenticators) {
      const auth = Array.isArray(this.config.authenticators)
          ? this.config.authenticators : [this.config.authenticators]
      for(const authenticator of auth) {
        if(authenticator) await authenticator(credentials, this.config)
      }
    }  
    for(const service of this.config.services) {
      console.log("SERIVCE AUTH", service.name, service.authenticators)
      if(service.authenticators) {
        for(const authenticator of service.authenticators) {
          if(authenticator) await authenticator(credentials, this.config)
        }
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
