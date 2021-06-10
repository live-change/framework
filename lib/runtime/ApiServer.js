const ReactiveDao = require("@live-change/dao")
const Dao = require("./Dao.js")
const cookie = require('cookie')

const { getIp } = require("./utils.js")

class ApiServer {
  constructor(config) {
    this.config = config
    this.reactiveServer = new ReactiveDao.ReactiveServer(
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

  async daoFactory(credentialsp, ip)  {
    let credentials = { ...credentialsp, ip, roles: [] }
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
