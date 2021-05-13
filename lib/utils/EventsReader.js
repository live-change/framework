
class EventsReader {
  constructor(startPosition, observableCallback, eventCallback, savePositionCallback, step = 5) {
    this.position = startPosition
    this.observableCallback = observableCallback
    this.eventCallback = eventCallback
    this.savePositionCallback = savePositionCallback
    this.step = step

    this.readPosition = this.position
    this.currentEvents = []
    this.eventsObservable = null

    this.readCount = 0
    this.resolveStart = null
    this.disposed = false
  }
  start() {
    return new Promise((resolve, reject) => {
      this.resolveStart = { resolve, reject }
      this.readMore()
    })
  }
  dispose() {
    this.disposed = true
    if(this.eventsObservable) this.eventsObservable.unobserve(this)
    this.eventsObservable = null
  }
  readMore() {
    if(this.eventsObservable) this.eventsObservable.unobserve(this)
    this.readCount += this.step
    this.eventsObservable = this.observableCallback(this.readPosition, this.step)
    this.eventsObservable.observe(this)
  }
  handleEvent(event) {
    if(event.id > this.readPosition) { // Simple deduplication
      this.readPosition = event.id
      const eventState = { event, done: false }
      this.currentEvents.push(eventState)
      this.eventCallback(event).then(result => {
        eventState.done = true
        this.movePositionForward()
      }).catch(error => {
        console.error("EVENT PROCESSING ERROR", error, "AT EVENT", event.id,
            " -> EVENT PROCESSING STOPPED AT", this.position)
        if(this.eventsObservable) this.eventsObservable.unobserve(this)
      })
    }
    this.readCount--
    if(this.readCount == 0) this.readMore()
  }
  movePositionForward() {
    let i
    for(i = 0; i < this.currentEvents.length; i++) {
      const event = this.currentEvents[i]
      if(!event.done) break
      this.position = event.event.id
    }
    this.currentEvents.splice(0, i)
    this.savePositionCallback(this.position)
  }
  error(error) {
    if(this.resolveStart) {
      this.resolveStart.reject(error)
      this.resolveStart = null
    }
    console.error("Events reader error", error)
    this.dispose()
  }
  set(value) {
    if(this.resolveStart) {
      this.resolveStart.resolve()
      this.resolveStart = null
    }
    if(this.disposed) return
    const events = value.slice()
    for(let event of events) {
      this.handleEvent(event)
    }
  }
  putByField(field, id, object, oldObject) {
    if(this.disposed) return
    if(oldObject) return // Object changes ignored
    this.handleEvent(object)
  }
  push(object) {
    if(this.disposed) return
    this.handleEvent(object)
  }

}

module.exports = EventsReader
