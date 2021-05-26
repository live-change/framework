function getIp(connection) {
  let ip =
      connection.headers['x-real-ip'] ||
      connection.headers['x-forwarded-for'] ||
      connection.remoteAddress
  ip = ip.split(',')[0]
  ip = ip.split(':').slice(-1)[0] //in case the ip returned in a format: "::ffff:146.xxx.xxx.xxx"
  return ip
}

function waitForSignal(observable, timeout = 1000, filter = () => true) {
  let observer
  let done = false
  return new Promise((resolve, reject) => {
    observer = (signal, value) => {
      console.log("SIGNAL", signal, value)
      if(done) return
      if(signal != 'set') return reject('unknownSignal')
      if(filter(value)) {
        done = true
        resolve(value)
      }
    }
    setTimeout(() => {
      if(done) return
      done = true
      reject('session observable timeout')
    }, timeout)
    observable.observe(observer)
  }).finally(() => {
    observable.unobserve(observer)
  })
}


module.exports.getIp = getIp
module.exports.waitForSignal = waitForSignal
