const EventSourcing = require('../utils/EventSourcing.js')

async function startEventListener(service, config) {
  if(!config.handleEvents) return
    
  if(service.app.splitEvents) {
    service.eventSourcing = new EventSourcing(service.dao, service.databaseName,
        'events_'+service.name, service.name,
        { filter: (event) => event.service == service.name })
  } else {
    service.eventSourcing = new EventSourcing(service.dao, service.databaseName,
        'events', service.name,
        { filter: (event) => event.service == service.name })
  }


  for (let eventName in service.events) {
    const event = service.events[eventName]
    service.eventSourcing.addEventHandler(eventName, async (ev, bucket) => {
      return await service.profileLog.profile({ operation: "handleEvent", eventName, id: ev.id,
            bucketId: bucket.id, triggerId: bucket.triggerId, commandId: bucket.commandId },
          () => {
            console.log("EXECUTING EVENT", ev)
            return event.execute(ev, bucket)
          }
      )
    })
    service.eventSourcing.onBucketEnd = async (bucket, handledEvents) => {
      if(bucket.reportFinished && handledEvents.length > 0) {
        await service.dao.request(['database', 'update'], service.databaseName, 'eventReports', bucket.reportFinished,[
          { op: "mergeSets", property: 'finished', values: handledEvents.map(ev => ({ id: ev.id, type: ev.type })) }
        ])
      }
    }
  }

  service.eventSourcing.start()
}

module.exports = startEventListener