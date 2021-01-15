const ExecutionQueue = require('./ExecutionQueue.js')

class KeyBasedExecutionQueues {
  constructor(keyFunction) {
    this.keyFunction = typeof keyFunction == 'function' ? keyFunction : (
        Array.isArray(keyFunction) ? (o) => JSON.stringify(keyFunction.map(k=>o[k])) :
            (o) => JSON.stringify(o[keyFunction]) )
    this.queues = new Map()
  }
  async queue(routine) {
    const key = this.keyFunction(routine)
    if(!key) {
      return routine()
    } else {
      let queue = this.queues.get(key)
      if(!queue) {
        queue = new ExecutionQueue(this.queues, key)
        this.queues.set(key, queue)
      }
      return queue.queue(routine)
    }
  }
}

module.exports = KeyBasedExecutionQueues
