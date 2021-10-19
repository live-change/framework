
class SplitEmitQueue {
  constructor(service, flags = {}) {
    this.service = service
    this.flags = flags
    this.emittedEvents = new Map()
    this.commited = false
  }

  emit(service, event) {
    if(!event) {
      event = service
      if(Array.isArray(event)) {
        let hasServices = false
        for(let ev of event) {
          if(ev.service) hasServices = true
        }
        if(hasServices) {
          for(let ev of event) {
            this.emit(ev)
          }
          return
        }
      } else {
        service = event.service || this.service.name
      }
    }
    let events
    if(!this.commited) {
      events = this.emittedEvents.get(service)
      if(!events) {
        events = []
        this.emittedEvents.set(service, events)
      }
    } else {
      events = []
    }
    if(Array.isArray(event)) {
      for(let ev of event) ev.service = service
      events.push(...event)
    } else {
      event.service = service
      events.push(event)
    }
    if(this.commited) {
      if(events.length == 0) return
      this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          this.service.name+'_events', { type: 'bucket', events, ...this.flags })
    }
  }

  async commit() {
    let promises = []
    this.commited = true
    if(this.emittedEvents.length == 0) return []
    let allEvents = []
    for(const [service, events] of this.emittedEvents.keys()) {
      promises.push(this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          this.service.name+'_events', { type: 'bucket', events, ...this.flags }))
      allEvents.push(...events)
    }
    await Promise.all(promises)
    return allEvents
  }
}

module.exports = SplitEmitQueue
