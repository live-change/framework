function getIp(connection) {
  let ip =
      connection.headers['x-real-ip'] ||
      connection.headers['x-forwarded-for'] ||
      connection.remoteAddress
  ip = ip.split(',')[0]
  ip = ip.split(':').slice(-1)[0] //in case the ip returned in a format: "::ffff:146.xxx.xxx.xxx"
  return ip
}


module.exports.getIp = getIp
