const EventsReader = require('./EventsReader.js')
const ReactiveDao = require('@live-change/dao')

function sleep(ms) {
  return new Promise((resolve, reject) => setTimeout(resolve, ms))
}

class EventSourcing {
  constructor(connection, database, logName, consumerName, config = {}) {
    this.connection = connection
    this.database = database
    this.logName = logName
    this.consumerName = consumerName
    this.consumerId = this.logName + '.' + this.consumerName
    this.config = config
    this.reader = null
    this.lastPositionSave = Date.now()
    this.lastSavedPosition = 0
    this.allEventHandlers = []
    this.eventTypeHandlers = new Map()
    this.onBucketEnd = () => {}
  }
  async dispose() {
    if(this.reader) this.reader.dispose()
    await this.saveState()
  }
  async start() {
    await this.connection.request(['database', 'createTable'], this.database, 'eventConsumers').catch(e => 'ok')
    await this.connection.request(['database', 'createLog'], this.database, this.logName).catch(e => 'ok')
    this.state = await this.connection.get(
        ['database', 'tableObject', this.database, 'eventConsumers', this.consumerId])
    //console.log("GOT CONSUMER STATE", this.state)
    if(!this.state) {
      this.state = {
        id: this.consumerId,
        position: ''
      }
      await this.saveState()
    }
    this.reader = new EventsReader(
        this.state.position,
        (position, limit) => {
          // console.log("READ RANGE", { gt: position, limit })
          return this.connection.observable(
            ['database', 'logRange', this.database, this.logName, { gt: position, limit }],
            ReactiveDao.ObservableList)
        },
        async (event) => {
          const handledEvents = await this.handleEvent(event, event)
          this.onBucketEnd(event, handledEvents)
        },
        (position) => {
          this.state.position = position
          this.savePosition(position)
        },
        this.config.fetchSize || 100
    )
    await this.reader.start()
  }
  async saveState() {
    //console.log("SAVE CONSUMER STATE", this.state)
    this.lastPositionSave = Date.now()
    const savedPosition = this.state.position
    await this.connection.request(
        ['database', 'put'], this.database, 'eventConsumers', this.state)
    this.lastSavedPosition = savedPosition
  }
  async handleEvent(event, mainEvent) {
    if(event.type == 'bucket') { // Handle buckets
      let handledEvents = []
      for(let i = 0; i < event.events.length; i++) {
        const subEvent = event.events[i]
        if(!subEvent.id) subEvent.id = event.id + "." + i
        handledEvents.push(...(await this.handleEvent(subEvent, mainEvent)))
      }
      return handledEvents
    }
    if (this.config.filter && !this.config.filter(event)) return []
    let done = false
    let retry = 0
    const maxRetry = this.config.maxRetryCount || 10
    while(!done && maxRetry) {
      try {
        await this.doHandleEvent(event, mainEvent)
        done = true
      } catch(e) {
        if(e == 'timeout' && retry < maxRetry) {
          retry++
          const sleepTime = Math.pow(2, retry) * 100
          console.error(`Event \n${JSON.stringify(event, null, "  ")}\n handling timeout, will retry `,
            retry, ' time after ', sleepTime, 'ms sleep')
          sleep(retry)
        } else {
          console.error(`EVENT \n${JSON.stringify(event, null, "  ")}\n HANDLING ERROR`, e, ' => STOPPING!')
          this.dispose()
          throw e
        }
      }
    }
    return [event]
  }
  async doHandleEvent(event, mainEvent) {
    let handled = false
    let eventHandlers = this.eventTypeHandlers.get(event.type) || []
    for (let handler of eventHandlers) {
      const result = await handler(event, mainEvent)
      if (result != 'ignored')
        handled = true
    }
    for (let handler of this.allEventHandlers) {
      const result = await handler(event, mainEvent)
      if (result != 'ignored')
        handled = true
    }
    if (!handled) {
      throw new Error("notHandled")
    }
  }

  async savePosition() {
    if(this.lastSavedPosition == this.state.position) return
    //console.log("SAVE POSITION", Date.now(), this.lastPositionSave )
    if(Date.now() - this.lastPositionSave <= (this.config.saveThrottle || 1000)) {
      setTimeout(() => this.savePosition(), this.config.saveThrottle || 1000)
      return
    }
    this.saveState()
  }
  addAllEventsHandler(handler) {
    this.allEventHandlers.push(handler)
  }
  addEventHandler(eventType, handler) {
    let handlers = this.eventTypeHandlers.get(eventType)
    if(!handlers) {
      handlers = []
      this.eventTypeHandlers.set(eventType, handlers)
    }
    handlers.push(handler)
  }
}

module.exports = EventSourcing
