const fs = require('fs')
const os = require('os')
const { once } = require('events')

class ProfileLogFilesystemWriter {
  constructor(path) {
    this.profileLogStream = null
    this.profileLogStreamDrainPromise = null

    if(path) {
      this.profileLogStream = fs.createWriteStream(path)
    } else if(process.env.PROFILE_LOG_PATH) {
      const dateString = new Date().toISOString().slice(0, -1).replace(/[T:\\.-]/gi, '_')
      const serviceName = process.cwd().split('/').pop()
      const hostname = os.hostname()
      const username = os.userInfo().username

      const logPath = process.env.PROFILE_LOG_PATH + serviceName +
          '@' + username + '@' + hostname + '@' + dateString + '.prof.log'
      this.profileLogStream = fs.createWriteStream(logPath)
    }
  }

  async write(operation) {
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
}

module.exports = ProfileLogFilesystemWriter
