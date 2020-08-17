
module.exports = function(module, app) {
  for(let modelName in module.models) {
    const model = module.models[modelName]
    for(let indexName in model.indexes) {
      const index = model.indexes[indexName]
      if(index.function) index.code = index.function.toString()
    }
  }
  for(let indexName in module.indexes) {
    const index = module.indexes[indexName]
    if(index.function) index.code = index.function.toString()
  }
}
