const utils = require("../utils.js")

function ignoreValidation(prop) {
  delete prop.validation
  if(prop.properties) {
    for(let propName in prop.properties) {
      ignoreValidation(prop.properties[propName])
    }
  }
  if(prop.of) {
    delete ignoreValidation(prop.of)
  }
}

module.exports = function(service, app) {
  if(!service) throw new Error("no service")
  if(!app) throw new Error("no service")
  for(let modelName in service.models) {
    const model = service.models[modelName]
    if(!model.crud) continue
    const crud = model.crud
    const generateId = crud.id || (() => app.generateUid())
    const options = crud.options || {}
    const writeOptions = crud.writeOptions || {}
    const readOptions = crud.readOptions || {}
    const idName = model.name.slice(0, 1).toLowerCase() + model.name.slice(1)
    let properties = {}
    for(let propName in model.properties) {
      let property = model.properties[propName]
      let typeName = utils.typeName(property.type)
      properties[propName] = {
        ...property,
        idOnly: !!service.models[typeName] // we need only id of entity, nothing more
      }
      if(crud.ignoreValidation) {
        ignoreValidation(properties[propName])
      }
    }
    let idProp = {}
    idProp[idName] = {
      type: model,
      idOnly: true
    }
    function genName(name) {
      return (crud.prefix || "") + model.name + name
    }
    function modelRuntime() {
      return service._runtime.models[modelName]
    }

    const defaults = utils.generateDefault(model.properties)

    if(!service.events[genName("Created")]) { // Events:
      service.event({
        name: genName("Created"),
        async execute(props) {
          console.log("CREATE !!!!")
          const data = utils.mergeDeep({}, defaults, props.data)
          console.log("DEFAULTS", JSON.stringify(defaults, null, "  "))
          console.log(`CREATE ${modelName} WITH DATA ${JSON.stringify(data, null, "  ")}`)
          await modelRuntime().update(props[idName], { ...data, id: props[idName] })
        }
      })
    }
    if(!service.actions[genName("Create")]) {
      service.action({
        name: genName("Create"),
        properties,
        returns: {
          type: model,
          idOnly: true
        },
        ...options,
        ...writeOptions,
        execute: async function(props, {service}, emit) {
          const id = generateId(props)
          let event = {
            data: props || {},
            type: genName("Created")
          }
          event[idName] = id
          emit(event)
          return id
        }
      })
    }

    if(!service.events[genName("Updated")]) { // Events:
      service.event({
        name: genName("Updated"),
        async execute(props) {
          if(crud.updateMethod == 'create') {
            await modelRuntime().create({ ...props.data, id: props[idName] })
          } else {
            await modelRuntime().update(props[idName], { ...props.data, id: props[idName] })
          }
        }
      })
    }
    if(!service.actions[genName("Update")]) {
      service.action({
        name: genName("Update"),
        properties: {
          ...idProp,
          ...properties
        },
        returns: {
          type: model,
          idOnly: true
        },
        ...options,
        ...writeOptions,
        execute: async function (props, {service}, emit) {
          let event = {
            data: {...props},
            type: genName("Updated")
          }
          event[idName] = props[idName]
          delete event.data[idName]
          emit(event)
          return props[idName]
        }
      })
    }

    if(!service.events[genName("Deleted")]) { // Events:
      service.event({
        name: genName("Deleted"),
        async execute(props) {
          await modelRuntime().delete(props[idName])
        }
      })
    }

    if(!service.actions[genName("Delete")]) {
      service.action({
        name: genName("Delete"),
        properties: idProp,
        returns: {
          type: null
        },
        ...options,
        ...writeOptions,
        execute: async function (props, {service}, emit) {
          if(crud.deleteTrigger || crud.triggers) {
            let trig = { type: genName("Deleted") }
            trig[idName] = props[idName]
            await service.trigger(trig)
          }
          emit({
            ...props,
            type: genName("Deleted")
          })
        }
      })
    }

    service.view({
      name: genName("One"),
      properties: idProp,
      ...options,
      ...readOptions,
      returns: {
        type: model
      },
      daoPath(props) {
        const id = props[idName]
        if(!id) throw new Error(idName + " is required")
        return modelRuntime().path(id)
      }
    })

    service.view({
      name: genName("Range"),
      properties: {
        gt: {
          type: String,
        },
        lt: {
          type: String,
        },
        gte: {
          type: String,
        },
        lte: {
          type: String,
        },
        limit: {
          type: Number
        },
        reverse: {
          type: Boolean
        }
      },
      returns: {
        type: Array,
        of: {
          type: model
        }
      },
      ...options,
      ...readOptions,
      daoPath(props) {
        return modelRuntime().rangePath(props)
      }
    })
  }
}