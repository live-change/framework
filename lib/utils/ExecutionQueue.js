class ExecutionQueue {
  constructor(queues, key) {
    this.key = key
    this.queues = queues
    this.running = false
    this.nextRoutines = []
  }

  queue(routine) {
    return new Promise((resolve, reject) => {
      this.nextRoutines.push({ routine, resolve, reject })
      if(!this.running) this.runNext()
    })
  }

  handleResult(result, routine) {
    if(result && result.then) {
      result.then(r => this.handleResult(r, routine)).catch(e => this.handleError(e, routine))
      return
    }
    routine.resolve(result)
    this.runNext()
  }
  handleError(error, routine) {
    console.error("Execution queue error", error)
    routine.reject(error)
    this.runNext()
  }

  runNext() {
    if(this.nextRoutines.length == 0) {
      this.running = false
      setTimeout(() => { if(!this.running) this.queues.delete(this.key) }, 500)
      return;
    }
    this.running = true
    const routine = this.nextRoutines.shift()
    try {
      const result = routine.routine()
      this.handleResult(result, routine)
    } catch(e) {
      this.handleError(e, routine)
    }
  }

}

module.exports = ExecutionQueue
