const fs = require('fs')
const { once } = require('events')

class ProfileLog {
  constructor(path) {
    this.profileLogStream = null
    this.profileLogStreamDrainPromise = null

    if(process.env.PROFILE_LOG_PATH) {
      const dateString = new Date().toISOString().slice(0, -1).replaceAll(/[T:\\.-]/gi, '_')
      const serviceName = process.cwd().split('/').pop()
      const logPath = process.env.PROFILE_LOG_PATH + serviceName + '@' + dateString + '.prof.log'
      this.profileLogStream = fs.createWriteStream(logPath)
    }
    if(path) {
      this.profileLogStream = fs.createWriteStream(path)
    }
  }

  async log(operation) {
    if(!this.profileLogStream) return;
    const msg = {
      time: (new Date()).toISOString(),
      ...operation
    }
    if(!this.profileLogStream.write(JSON.stringify(msg)+'\n')) {
      if(!this.profileLogStreamDrainPromise) {
        this.profileLogStreamDrainPromise = once(this.profileLogStream, 'drain')
      }
      await this.profileLogStreamDrainPromise
      this.profileLogStreamDrainPromise = null
    }
  }

  async begin(operation) {
    const now = new Date()
    const op = { ...operation, start: now, time: now, type: "started"}
    await this.log(op)
    return op
  }

  async end(op) {
    const now = new Date()
    if(!op.start) console.error("NO OP START IN", op)
    op.type = 'finished'
    op.end = now
    op.duration = now.getTime() - op.start.getTime()
    await this.log(op)
    return op
  }

  async endPromise(op, promise) {
    if(!op.start) throw new Error("no op start")
    await promise.then(res => {
      this.end({ ...op, result: 'done' })
    }).catch(error => {
      this.end({ ...op, result: 'error', error })
    })
    return promise
  }

  async profile(operation, code) {
    const op = await this.begin(operation)
    try {
      return await code()
    } finally {
      await this.end(op)
    }
  }
}

const profileLog = new ProfileLog()
profileLog.ProfileLog = ProfileLog

module.exports = profileLog