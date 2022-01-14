const ReactiveDao = require('@live-change/dao')

class CommandQueue {
  constructor(connection, database, tableName, serviceName) {
    this.connection = connection
    this.database = database
    this.tableName = tableName
    this.indexName = tableName + '_new'
    this.serviceName = serviceName
    this.observable = null
    this.disposed = false
    this.resolveStart = null
    this.commandsStarted = new Map()
    this.allCommandHandlers = []
    this.commandTypeHandlers = new Map()
  }
  async dispose() {
    this.disposed = true
    if(this.observable) {
      this.observable.unobserve(this)
      this.observable = null
    }
  }
  async start() {
    //console.log("START QUEUE", this.tableName, this.indexName)
    await this.connection.request(['database', 'createTable'], this.database, this.tableName).catch(e => 'ok')
    await this.connection.request(['database', 'createIndex'], this.database, this.indexName, `(${
        async function(input, output, { tableName }) {
          await input.table(tableName).onChange(async (obj, oldObj) => {
            const res = obj && obj.state == 'new' ? { ...obj, id: obj.service + '_' + obj.id } : null
            const oldRes = oldObj && oldObj.state == 'new' ? { ...oldObj, id: oldObj.service + '_' + oldObj.id } : null
            //output.debug("CHANGE ", tableName, "INDEX", res, "OLD:", oldRes)
            await output.change(res, oldRes)
          })
        }
    })`, { tableName: this.tableName }).catch(e => 'ok')
    this.observable = this.connection.observable(
        ['database', 'indexRange', this.database, this.indexName, {
          gt: this.serviceName+'_',
          lt: this.serviceName+'_\xFF',
          limit: 128
        }],
        ReactiveDao.ObservableList)
    this.observable.observe(this)

    return new Promise((resolve, reject) => {
      this.resolveStart = { resolve, reject }
    })
  }
  async handleCommand(command) {
    //console.log("COMMNAD HANDLE!", command)
    if(command.state != 'new') return
    if(this.commandsStarted.has(command.id)) return
    this.commandsStarted.set(command.id, command)
    const started = new Date()
    try {
      let handled = false
      let result = null
      let commandHandlers = this.commandTypeHandlers.get(command.type) || []
      for(let handler of commandHandlers) {
        result = handler(command)
        if(result != 'ignored') {
          handled = true
          break
        }
      }
      if(!handled) {
        for(let handler of this.allCommandHandlers) {
          const result = handler(command)
          if(result != 'ignored') {
            handled = true
            break
          }
        }
      }
      if(!handled) {
        console.error(`Command handler ${this.tableName} for type ${command.type} not found`)
        throw new Error("notHandled")
      }
      return Promise.resolve(result).then(async result => {
        const finished = new Date()
        const stats = {
          started,
          finished,
          delay: command.timestamp  && (started.getTime() - (new Date(command.timestamp)).getTime()),
          execution: finished.getTime() - started.getTime()
        }
        console.log("UPDATE DATABASE", ['database', 'update'], this.database, this.tableName, command.id, [
          { op: 'merge', property: null, value: { state: 'done', result, stats } }
        ])
        await this.connection.request(['database', 'update'], this.database, this.tableName, command.id, [
          { op: 'merge', property: null, value: { state: 'done', result, stats } }
        ])
        // hold it for one second in case of delayed event:
        setTimeout(() => this.commandsStarted.delete(command.id), 1000)
      }).catch(async error => {
        return this.connection.request(['database', 'update'], this.database, this.tableName, command.id, [
          {
            op: 'merge', property: null,
            value: { state: 'failed', error: (error && (error.stack || error.message)) || error }
          }
        ])
        // hold it for one second in case of delayed event:
        setTimeout(() => this.commandsStarted.delete(command.id), 1000)
      })
    } catch(e) {
      console.error(`COMMAND ${JSON.stringify(command, null, "  ")} HANDLING ERROR`, e, ' => STOPPING!')
      this.dispose()
      throw e
    }
    return 'ok'
  }
  set(value) {
    if(this.resolveStart) {
      this.resolveStart.resolve()
      this.resolveStart = null
    }
    if(this.disposed) return
    const commands = value.slice()
    for(let command of commands) {
      this.handleCommand({ ...command, id: command.id.slice(command.id.indexOf('_')+1) })
    }
  }
  putByField(field, id, command, oldObject) {
    if(this.disposed) return
    if(oldObject) return // Object changes ignored
    this.handleCommand({ ...command, id: command.id.slice(command.id.indexOf('_')+1) })
  }
  push(command) {
    if(this.disposed) return
    this.handleCommand({ ...command, id: command.id.slice(command.id.indexOf('_')+1) })
  }
  removeByField() {
    /// Ignore
  }
  error(error) {
    if(this.resolveStart) {
      this.resolveStart.reject(error)
      this.resolveStart = null
    }
    console.error(`COMMAND QUEUE ${this.tableName} READ ERROR`, error)
    this.dispose()
  }
  addAllCommandsHandler(handler) {
    this.allCommandHandlers.push(handler)
  }
  addCommandHandler(commandType, handler) {
    let handlers = this.commandTypeHandlers.get(commandType)
    if(!handlers) {
      handlers = []
      this.commandTypeHandlers.set(commandType, handlers)
    }
    handlers.push(handler)
  }
}

module.exports = CommandQueue
