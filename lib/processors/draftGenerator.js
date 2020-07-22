const utils = require("../utils.js")

function propertyWithoutValidation(property) {
  let prop = { ...property }
  delete prop.validation
  if(prop.draftValidation) prop.validation = prop.draftValidation
  if(prop.of) prop.of = withoutValidation(prop.of)
  if(prop.properties) prop.properties = propertiesWithoutValidation(prop)
  return prop
}

function propertiesWithoutValidation(properties, validateFields) {
  let propertiesWV = {}
  for(let k in properties) {
    propertiesWV[k] =
        (validateFields && validateFields.indexOf(k) != -1)
        ? properties[k]
        : propertyWithoutValidation(properties[k])
  }
  return propertiesWV
}

module.exports = function(service, app) {
  if(!service) throw new Error("no service")
  if(!app) throw new Error("no service")
  for(let actionName in service.actions) {
    const action = service.actions[actionName]
    if (!action.draft) continue

    const actionExecute = action.execute
    const draft = action.draft
    const steps = draft.steps

    const modelName = `${actionName}_draft`
    let indexes = {}
    let properties = {
      ...action.properties
    }

    if(draft.identification) {
      properties = {
        ...(draft.identification),
        ...properties
      }
      indexes.identifier = {
        property: Object.keys(draft.identification)
      }
    }

    if(draft.steps) {
      properties = {
        draftStep: {
          type: String
        },
        ...properties
      }
    }

    const DraftModel = service.model({
      name: modelName,
      properties,
      indexes
    })
    function modelRuntime() {
      return service._runtime.models[modelName]
    }

    const propertiesWV = propertiesWithoutValidation(properties, draft.validateFields)

    service.action({
      name: `${actionName}_saveDraft`,
      properties: {
        ...propertiesWV,
        draft: {
          type: String
        }
      },
      access: draft.saveAccess || draft.access || action.access,
      async execute(params, {service, client}, emit) {
        let draft = params.draft
        if(!draft) draft = app.generateUid()
        let data = {}
        for(let k in properties) data[k] = params[k]
        emit({
          type: `${actionName}_draftSaved`,
          draft, data
        })
        return draft
      }
    })
    service.event({
      name: `${actionName}_draftSaved`,
      async execute(props) {
        await modelRuntime().create({...props.data, id: props.draft})
      }
    })

    service.action({
      name: `${actionName}_deleteDraft`,
      properties: {
        draft: {
          type: String,
          validation: ['nonEmpty']
        }
      },
      access: draft.deleteAccess || draft.access || action.access,
      async execute(params, {service, client}, emit) {
        emit({
          type: `${actionName}_draftDeleted`,
          draft: params.draft
        })
      }
    })
    service.event({
      name: `${actionName}_draftDeleted`,
      async execute({draft}) {
        await modelRuntime().delete(draft)
      }
    })

    service.action({
      name: `${actionName}_finishDraft`,
      properties: {
        ...propertiesWV,
        draft: {
          type: String
        }
      },
      access: draft.finishAccess || draft.access || action.access,
      async execute(params, context, emit) {
        let draft = params.draft
        if(!draft) draft = app.generateUid()
        let actionProps = {}
        for(let k in action.properties) actionProps[k] = params[k]
        const result = await actionExecute.call(action, actionProps, context, emit)
        emit({
          type: `${actionName}_draftFinished`,
          draft
        })
        return result
      }
    })
    service.event({
      name: `${actionName}_draftFinished`,
      async execute({draft}) {
        await modelRuntime().delete(draft)
      }
    })

    service.view({
      name: `${actionName}_draft`,
      properties: {
        draft: {
          type: String,
          validation: ['nonEmpty']
        }
      },
      returns: {
        type: DraftModel
      },
      access: draft.readAccess || draft.access || action.access,
      daoPath({ draft }) {
        if(!draft) return null
        return modelRuntime().path(draft)
      }
    })

    if(draft.identification) {
      service.view({
        name: `${actionName}_drafts`,
        properties: {
          ...draft.identification
        },
        access: draft.listAccess || draft.access || action.access,
        daoPath(params) {
          const ident = Object.keys(draft.identification).map(p => params[p])
          return modelRuntime().indexRangePath('identifier', ident)
        }
      })
    }

    if(steps) {
      for(let i = 0; i < steps.length; i++) {
        const step = steps[i]
        const nextStep = steps[i + 1]

        //console.log("ACTION PROPERTIES", action.properties)
        const stepProperties = {}
        for(let fieldName of step.fields) {
          utils.setProperty({ properties: stepProperties }, fieldName, utils.getProperty(action, fieldName))
        }
        const stepPropertiesVW = propertyWithoutValidation(stepProperties)

        //console.log(`STEP ${step.name} PROPERTIES`, stepProperties)
        //console.log(`STEP ${step.name} PROPERTIES VW`, stepPropertiesVW)

        service.action({
          name: `${actionName}_saveStepDraft_${step.name || i}`,
          properties: stepPropertiesVW,
          async execute(params, {service, client}, emit) {
            let draft = params.draft
            if(!draft) draft = app.generateUid()
            let data = {}
            for(let k in properties) data[k] = params[k]
            emit({
              type: `${actionName}_stepDraftSaved`,
              draft, data,
              draftStep: step.name || i
            })
            return draft
          }
        })
        service.event({
          name: `${actionName}_stepDraftSaved`,
          async execute(props) {
            await app.dao.request(['database', 'query', app.databaseName, `(${
                async (input, output, { table, id, data, draftStep }) => {
                  const value = await input.table(table).object(id).get()
                  if(!value) {
                    return await output.table(table).put({ ...data, id, draftStep })
                  } else {
                    return await output.table(table).update(id, [
                      { op:'merge', property: null, value: { ...data, draftStep} }
                    ])
                  }
                }
            })`, { table: modelRuntime().tableName, id: props.draft, data: props.data, draftStep: props.draftStep }])
          }
        })

        service.action({
          name: `${actionName}_finishStep_${step.name || i}`,
          properties: stepProperties,
          async execute(params, context, emit) {
            let data = {}
            console.log("PARAMS", params)
            for (let k in stepProperties) data[k] = params[k]
            if(nextStep) {
              let draft = params.draft
              if(!draft) draft = app.generateUid()
              delete data.draftStep
              emit({
                type: `${actionName}_stepSaved`,
                draft, data,
                draftStep: step.name || i,
                draftNextStep: nextStep.name || (i + 1)
              })
              return draft
            } else {
              console.log("PM", params)
              let actionProps = params.draft ? await modelRuntime().get(params.draft) : {}
              console.log("AP", actionProps)
              console.log("DT", data)
              delete actionProps.draft
              delete actionProps.draftStep
              utils.mergeDeep(actionProps, data)
              const result = await actionExecute.call(action, actionProps, context, emit)
              if(params.draft) {
                emit({
                  type: `${actionName}_draftFinished`,
                  draft: params.draft
                })
              }
              return result
            }
          }
        })
        service.event({
          name: `${actionName}_stepSaved`,
          async execute(props) {
            await app.dao.request(['database', 'query', app.databaseName,`(${
                async (input, output, { table, id, data, draftStep }) => {
                  const value = await input.table(table).object(id).get()
                  if(!value) {
                    return await output.table(table).put({ ...data, id, draftStep })
                  } else {
                    return await output.table(table).update(id, [
                        { op:'merge', property: null, value: { ...data, draftStep} }
                        ])
                  }
                }
            })`, { table: modelRuntime().tableName, id: props.draft, data: props.data, draftStep: props.draftStep }])
            //await modelRuntime().create({...props.data, id: props.draft, draftStep: props.draftStep}, { conflict: 'update' })
          }
        })

      }
    }

  }
}