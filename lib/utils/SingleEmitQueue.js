
class SingleEmitQueue {
  constructor(service, flags = {}) {
    this.service = service
    this.flags = flags
    this.emittedEvents = []
    this.commited = false
  }

  emit(service, event) {
    if(!event) {
      event = service
      service = this.service.name
    }
    let events
    if(!this.commited) {
      events = this.emittedEvents
    } else {
      events = []
    }
    if(Array.isArray(event)) {
      for(let ev of event) if(!ev.service) ev.service = service
      events.push(...event)
    } else {
      if(!event.service) event.service = service
      events.push(event)
    }
    if(this.commited) {
      if(events.length == 0) return
      this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          'events', { type: 'bucket', events, ...this.flags })
    }
  }

  async commit() {
    this.commited = true
    if(this.emittedEvents.length == 0) return []
    await this.service.dao.request(['database', 'putLog'], this.service.databaseName,
          'events', { type: 'bucket', events: this.emittedEvents, ...this.flags })
    return this.emittedEvents
  }
}

module.exports = SingleEmitQueue
