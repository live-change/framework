

const SEARCH_INDEX_NOTSTARTED = 0
const SEARCH_INDEX_CREATING = 1
const SEARCH_INDEX_UPDATING = 2
const SEARCH_INDEX_READY = 3

const bucketSize = 256
const saveStateThrottle = 2000
const saveStateDelay = saveStateThrottle + 200

function prepareArray(data, of) {
  if(!data) return
  if(of.properties) for(let i = 0; i < data.length; i++) prepareObject(data[i], of)
  if(of.of) for(let i = 0; i < data.length; i++) prepareArray(data[i], of.of)
}

function prepareObject(data, props) {
  if(!data) return
  for(const propName in props) {
    if(!data.hasOwnProperty(propName)) continue
    const prop = props[propName]
    if(prop.properties) {
      prepareObject(data[propName], prop.properties)
    }
    if(prop.of) {
      prepareArray(data[propName], prop.of)
    }
    if(prop.search) {
      if(Array.isArray(prop.search)) {
        for(const search of prop.search) {
          if(search.name) data[search.name] = data[propName]
        }
      } else {
        if(prop.search.name) data[prop.search.name] = data[propName]
      }
    }
  }
}

class SearchIndexer {
  constructor(dao, databaseName, sourceType, sourceName, elasticsearch, indexName, model ) {
    this.dao = dao
    this.databaseName = databaseName
    this.sourceType = sourceType
    this.sourceName = sourceName
    this.elasticsearch = elasticsearch
    this.indexName = indexName
    this.model = model
    this.state = SEARCH_INDEX_NOTSTARTED
    this.lastUpdateId = ''
    this.lastSavedId = ''

    this.observable = null

    this.queue = []
    this.queueWriteResolve = []
    this.queueLastUpdateId = ''
    this.queueWritePromise = null
    this.queueWriteResolve = null
    this.currentWritePromise = null

    this.readingMore = true

    this.lastStateSave = 0
    this.saveStateTimer = null
  }

  prepareObject(object) {
    prepareObject(object, this.model.properties)
  }

  async start() {
    const searchIndexState = await this.dao.get(
        ['database','tableObject', this.databaseName, 'searchIndexes', this.indexName])
    const firstSourceOperation = (await this.dao.get(
        ['database', this.sourceType.toLowerCase()+'OpLogRange', this.databaseName, this.sourceName, {
          limit: 1
        }]))[0]

    console.log("Index State", searchIndexState)
    console.log("first Source Operation", firstSourceOperation && firstSourceOperation.id)

    let lastUpdateTimestamp = 0
    if(!searchIndexState || (firstSourceOperation && firstSourceOperation.id > searchIndexState.lastOpLogId)) {
      const indexCreateTimestamp = Date.now()
      this.state = SEARCH_INDEX_CREATING
      console.log("CREATING SEARCH INDEX")
      await this.copyAll()
      lastUpdateTimestamp = indexCreateTimestamp - 1000 // one second overlay
      this.lastUpdateId = (''+(lastUpdateTimestamp - 1000)).padStart(16,'0')
    } else {
      this.state = SEARCH_INDEX_UPDATING
      console.log("UPDATING SEARCH INDEX")
      lastUpdateTimestamp = (+searchIndexState.lastOpLogId.split(':')[0]) - 1000 // one second overlap
      this.lastUpdateId = searchIndexState.lastOpLogId
      await this.updateAll()
    }

    await this.dao.request(['database', 'put', this.databaseName, 'searchIndexes', {
      id: this.indexName,
      lastOpLogId: this.lastUpdateId
    }])
    this.lastStateSave = Date.now()

    console.log("SEARCH INDEX READY")
    this.state = SEARCH_INDEX_READY

    this.observeMore()
  }

  async saveState() {
    if(this.lastSavedId == this.lastUpdateId) return
    if(Date.now() - this.lastStateSave < saveStateThrottle) {
      if(this.saveStateTimer === null) {
        setTimeout(() => this.saveState(), saveStateDelay)
      }
      return
    }
    console.log("SAVE INDEXER STATE", this.lastUpdateId)
    this.lastSavedId = this.lastUpdateId
    this.lastStateSave = Date.now()
    await this.dao.request(['database', 'put', this.databaseName, 'searchIndexes', {
      id: this.indexName,
      lastOpLogId: this.lastUpdateId
    }])
  }

  doWrite() {
    if(this.queueWritePromise) {
      return this.queueWritePromise
    }
    const operations = this.queue
    if(operations.length == 0) {
      this.lastUpdateId = this.queueLastUpdateId
      return
    }
    this.queueWritePromise = new Promise((resolve, reject) => {
      this.queueWriteResolve = { resolve, reject }
    })
    const queueResolve = this.queueWriteResolve
    this.queueWriteResolve = null
    this.queueWritePromise = null
    this.queue = []
    //console.log("WRITE BULK", operations)
    this.elasticsearch.bulk({
      index: this.indexName,
      body: operations
    }).catch(error => {
      error = (error && error.meta && error.meta.body && error.meta.body.error) || error
      console.error("ES ERROR:", error)
      queueResolve.reject(error)
      throw error
    }).then(result => {
      //console.log("BULK RESULT", result)
      if(result.body.errors) {
        for(const item of result.body.items) {
          if(item.index.error) {
            const opId = operations.findIndex(op =>
                (op.index && op.index._id == item.index._id)
                || (op.delete && op.delete._id == item.delete._id)
            )
            const op = operations[opId]
            const data = op.index ? operations[opId + 1] : null
            console.error("INDEX ERROR", item.index.error, "OP", op, "D", data)
          }
        }
        //throw result.body.items.find(item => item.error)
        throw new Error("INDEXING ERROR "+ result.body.errors)
      }
    }).then(written => {
      this.lastUpdateId = this.queueLastUpdateId
      queueResolve.resolve()
      this.currentWritePromise = null
      this.saveState()
      if(this.queue.length) {
        this.doWrite()
      }
    })
    return this.queueWritePromise
  }

  async applyOps(ops) {
    //console.trace(`apply ${ops.length} ops`)
    if(ops.length == 0) return;
    let size = 0
    for(const op of ops) {
      if(op.operation.type == 'put') size += 2
      if(op.operation.type == 'delete') size += 1
    }
    const operations = new Array(size)
    let pos = 0
    for(const op of ops) {
      if(op.operation.type == 'put') {
        operations[pos++] = { index: { _id: op.operation.object.id } }
        this.prepareObject(op.operation.object)
        operations[pos++] = op.operation.object
      }
      if(op.operation.type == 'delete') {
        operations[pos++] = { delete: { _id: op.operation.object.id } }
      }
    }
    const lastUpdateId = ops[ops.length-1].id
    //console.log("ES OPS", operations)
    this.queue = this.queue.length ? this.queue.concat(operations) : operations
    this.queueLastUpdateId = lastUpdateId
    //console.log("WRITE OPS!")
    await this.doWrite()
    //console.log("OPS WRITTEN!")
  }

  observeMore() {
    this.readingMore = true
    if(this.observable) this.observable.unobserve(this)
    this.observable = this.dao.observable(
        ['database', this.sourceType.toLowerCase()+'OpLogRange', this.databaseName, this.sourceName, {
          gt: this.lastUpdateId,
          limit: bucketSize
        }])
    this.observable.observe(this)
  }
  tryObserveMore() {
    if(!this.readingMore && this.observable.list.length == bucketSize) {
      this.observeMore()
    }
  }
  async set(ops) {
    this.readingMore = false
    console.log("SET", this.lastUpdateId, ops.length)
    await this.applyOps(ops)
    this.tryObserveMore()
  }
  async putByField(_fd, id, op, _reverse, oldObject) {
    this.readingMore = false
    await this.applyOps([ op ])
    this.tryObserveMore()
  }

  async updateAll() {
    let ops
    do {
      console.log("UPDATE FROM", this.lastUpdateId)
      ops = await this.dao.get(
          ['database', this.sourceType.toLowerCase()+'OpLogRange', this.databaseName, this.sourceName, {
            gt: this.lastUpdateId,
            limit: bucketSize
          }])
      console.log("OPS", ops.length)
      await this.applyOps(ops)
      console.log("OPS APPLIED", ops.length)
    } while(ops.length >= bucketSize)
  }

  async copyAll() {
    const search = this.elasticsearch
    let position = ""
    let more = true

    console.log("DELETE OLD DATA")
    await search.delete_by_query({
      index: this.indexName,
      body: {
        query: {
          match_all: {}
        }
      }
    })

    console.log(`INDEXING ${this.sourceType} ${this.sourceName}`)
    do {
      const rows = await this.dao.get(
          ['database', this.sourceType.toLowerCase()+'Range', this.databaseName, this.sourceName, {
            gt: position,
            limit: bucketSize
          }])
      position = rows.length ? rows[rows.length-1].id : "\xFF"
      more = (rows.length == bucketSize)

      if(more) console.log(`READ ${rows.length} ROWS`)
        else console.log(`READ LAST ${rows.length} ROWS`)

      if(rows.length > 0) {
        console.log(`WRITING ${rows.length} ROWS`)
        let operations = new Array(rows.length * 2)
        for(let i = 0; i < rows.length; i++) {
          operations[i * 2] = { index: { _id: rows[i].id } }
          this.prepareObject(rows[i])
          operations[i * 2 + 1] = rows[i]
        }
        await search.bulk({
          index: this.indexName,
          body: operations
        }).then(result => {
          if(result.body.errors) {
            for(const item of result.body.items) {
              if(item.index && item.index.error) {
                console.error("ES ERROR:", item.index.error)
                console.error("WHEN INDEXING", rows.find(row => row.id == item.index._id))
              }
            }
            throw new Error("ES ERRORS")
          }
        }).catch(error => {
          error = (error && error.meta && error.meta.body && error.meta.body.error) || error
          console.error("ES ERROR:", error)
          throw error
        })
      }
      console.log("ES INDEXED!")

    } while (more)
  }

}

module.exports = SearchIndexer