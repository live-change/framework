function getAccessMethod(access) {
  if(typeof access == 'function') {
    return access
  } else if(Array.isArray(access)) {
    return (params, {service, client}) => {
      for(let role of access) if(client.roles.includes('admin')) return true
      return false
    }
  } else throw new Error("unknown view access definition "+access)
}

module.exports = getAccessMethod