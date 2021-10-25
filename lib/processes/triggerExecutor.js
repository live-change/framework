const KeyBasedExecutionQueues = require('../utils/KeyBasedExecutionQueues.js')
const CommandQueue = require('../utils/CommandQueue.js')
const SingleEmitQueue = require('../utils/SingleEmitQueue.js')
const SplitEmitQueue = require('../utils/SplitEmitQueue.js')

async function startTriggerExecutor(service, config) {
  if(!config.runCommands) return
  
  service.keyBasedExecutionQueues = service.keyBasedExecutionQueues || new KeyBasedExecutionQueues(r => r.key)

  await service.dao.request(['database', 'createTable'], service.databaseName, 'triggerRoutes').catch(e => 'ok')

  service.triggerQueue = new CommandQueue(service.dao, service.databaseName,
      service.app.splitTriggers ? `${service.name}_triggers` : 'triggers', service.name )
  for (let triggerName in service.triggers) {
    const trigger = service.triggers[triggerName]
    await service.dao.request(['database', 'put'], service.databaseName, 'triggerRoutes',
        { id: triggerName + '=>' + service.name, trigger: triggerName, service: service.name })
    if(trigger.definition.queuedBy) {
      const queuedBy = trigger.definition.queuedBy
      const keyFunction = typeof queuedBy == 'function' ? queuedBy : (
          Array.isArray(queuedBy) ? (c) => JSON.stringify(queuedBy.map(k=>c[k])) :
              (c) => JSON.stringify(c[queuedBy]) )
      service.triggerQueue.addCommandHandler(triggerName, async (trig) => {
        const profileOp = await service.profileLog.begin({ operation: 'queueTrigger', triggerType: triggerName,
          triggerId: trig.id, by: trig.by })
        console.log("QUEUED TRIGGER STARTED", trig)
        const reportFinished = trigger.definition.waitForEvents ? 'trigger_'+trig.id : undefined
        const flags = { triggerId: trig.id, reportFinished }
        const emit = service.app.splitEvents
            ? new SplitEmitQueue(service, flags)
            : new SingleEmitQueue(service, flags)
        const routine = () => service.profileLog.profile({ operation: 'runTrigger', triggerType: triggerName,
          commandId: trig.id, by: trig.by }, async () => {
          let result
          try {
            console.log("TRIGGERED!!", trig)
            result = await service.app.assertTime('trigger '+trigger.definition.name,
                trigger.definition.timeout || 10000,
                () => trigger.execute(trig, (...args) => emit.emit(...args)), trig)
            console.log("TRIGGER DONE!", trig)
          } catch (e) {
            console.error(`TRIGGER ${triggerName} ERROR`, e.stack)
            throw e
          }
          const events = await emit.commit()
          if(trigger.definition.waitForEvents)
            await service.app.waitForEvents(reportFinished, events, trigger.definition.waitForEvents)
          return result
        })
        try {
          routine.key = keyFunction(trig)
        } catch(e) {
          console.error("QUEUE KEY FUNCTION ERROR", e)
        }
        console.log("TRIGGER QUEUE KEY", routine.key)
        const promise = service.keyBasedExecutionQueues.queue(routine)
        await service.profileLog.endPromise(profileOp, promise)
        return promise
      })
    } else {
      service.triggerQueue.addCommandHandler(triggerName,
          (trig) => service.profileLog.profile({ operation: 'runTrigger', triggerType: triggerName,
            commandId: trig.id, by: trig.by }, async () => {
            console.log("NOT QUEUED TRIGGER STARTED", trig)
            const reportFinished = trigger.definition.waitForEvents ? 'trigger_'+trig.id : undefined
            const flags = { triggerId: trig.id, reportFinished }
            const emit = service.app.splitEvents
                ? new SplitEmitQueue(service, flags)
                : new SingleEmitQueue(service, flags)
            let result
            try {
              result = await service.app.assertTime('trigger '+trigger.definition.name,
                  trigger.definition.timeout || 10000,
                  () => trigger.execute(trig, (...args) => emit.emit(...args)), trig)
              console.log("TRIGGER DONE!", trig)
            } catch (e) {
              console.error(`TRIGGER ${triggerName} ERROR`, e.stack)
              throw e
            }
            const events = await emit.commit()
            if(trigger.definition.waitForEvents)
              await service.app.waitForEvents(reportFinished, events, trigger.definition.waitForEvents)
            return result
          })
      )
    }
  }

  service.triggerQueue.start()
}

module.exports = startTriggerExecutor
