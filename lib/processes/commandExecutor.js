const KeyBasedExecutionQueues = require('../utils/KeyBasedExecutionQueues.js')
const CommandQueue = require('../utils/CommandQueue.js')
const SingleEmitQueue = require('../utils/SingleEmitQueue.js')
const SplitEmitQueue = require('../utils/SplitEmitQueue.js')

async function startCommandExecutor(service, config) {
  if(!config.runCommands) return

  service.keyBasedExecutionQueues = service.keyBasedExecutionQueues || new KeyBasedExecutionQueues(r => r.key)
  
  service.commandQueue = new CommandQueue(service.dao, service.databaseName,
      service.app.splitCommands ? `${service.name}_commands` : 'commands', service.name)
  for (let actionName in service.actions) {
    const action = service.actions[actionName]
    if (action.definition.queuedBy) {
      const queuedBy = action.definition.queuedBy
      const keyFunction = typeof queuedBy == 'function' ? queuedBy : (
          Array.isArray(queuedBy) ? (c) => JSON.stringify(queuedBy.map(k => c[k])) :
              (c) => JSON.stringify(c[queuedBy]))
      service.commandQueue.addCommandHandler(actionName, async (command) => {
        const profileOp = await service.profileLog.begin({
          operation: 'queueCommand', commandType: actionName,
          commandId: command.id, client: command.client
        })
        const reportFinished = action.definition.waitForEvents ? 'command_' + command.id : undefined
        const flags = {commandId: command.id, reportFinished}
        const emit = service.app.splitEvents
            ? new SplitEmitQueue(service, flags)
            : new SingleEmitQueue(service, flags)
        const routine = () => service.profileLog.profile({
          operation: 'runCommand', commandType: actionName,
          commandId: command.id, client: command.client
        }, async () => {
          const result = await service.app.assertTime('command ' + action.definition.name,
              action.definition.timeout || 10000,
              () => action.runCommand(command, (...args) => emit.emit(...args)), command)
          const events = await emit.commit()
          if (action.definition.waitForEvents)
            await service.app.waitForEvents(reportFinished, events, action.definition.waitForEvents)
          return result
        })
        routine.key = keyFunction(command)
        const promise = service.keyBasedExecutionQueues.queue(routine)
        await service.profileLog.endPromise(profileOp, promise)
        return promise
      })
    } else {
      service.commandQueue.addCommandHandler(actionName,
          (command) => service.profileLog.profile({
            operation: 'runCommand', commandType: actionName,
            commandId: command.id, client: command.client
          }, async () => {
            const reportFinished = action.definition.waitForEvents ? 'command_' + command.id : undefined
            const flags = {commandId: command.id, reportFinished}
            const emit = service.app.splitEvents
                ? new SplitEmitQueue(service, flags)
                : new SingleEmitQueue(service, flags)
            const result = await service.app.assertTime('command ' + action.definition.name,
                action.definition.timeout || 10000,
                () => action.runCommand(command, (...args) => emit.emit(...args)), command)
            const events = await emit.commit()
            if (action.definition.waitForEvents)
              await service.app.waitForEvents(reportFinished, events, action.definition.waitForEvents)
            return result
          })
      )

    }
  }

  service.commandQueue.start()
}

module.exports = startCommandExecutor
