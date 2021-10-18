class ProfileLog {
  constructor() {
    this.writers = []
  }

  async log(operation) {
    await Promise.all(this.writers.map(writer => writer.write(operation)))
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

  profileFunctions(functions, mapper = x=>x) {
    for(const funcName of functions) {
      const target = functions[funcName]
      const paramNames = getParamNames(target)
      functions[funcName] = function(...args) {
        const params = {}
        for(let i = 0; i < paramNames.length; i++) {
          params[paramNames[i]] = args[i]
        }
        return this.profile(
            mapper({ operation: funcName, ...params }),
            function() {
              return target.apply(functions, args)
            })
      }
    }
  }
}

const STRIP_COMMENTS = /((\/\/.*$)|(\/\*[\s\S]*?\*\/))/mg
const ARGUMENT_NAMES = /([^\s,]+)/g
function getParamNames(func) {
  const fnStr = func.toString().replace(STRIP_COMMENTS, '')
  const result = fnStr.slice(fnStr.indexOf('(')+1, fnStr.indexOf(')')).match(ARGUMENT_NAMES)
  if(result === null) return []
  return result
}

const profileLog = new ProfileLog()
profileLog.ProfileLog = ProfileLog

module.exports = profileLog