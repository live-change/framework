
function legacyEnvConfig(env = process.env) {
  return {
    db: {
      url: env.DB_URL,
      name: env.DB_NAME,
      requestTimeout: (+env.DB_REQUEST_TIMEOUT),
      cache: env.DB_CACHE == "YES",
      //unobserveDebug: env.UNOBSERVE_DEBUG == "YES",
    }
  }
}

module.exports = legacyEnvConfig
