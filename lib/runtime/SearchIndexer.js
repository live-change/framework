

const SEARCH_INDEX_NOTSTARTED = 0
const SEARCH_INDEX_CREATING = 1
const SEARCH_INDEX_UPDATING = 2
const SEARCH_INDEX_READY = 3

const bucketSize = 256
const saveStateThrottle = 2000
const saveStateDelay = saveStateThrottle + 200

class SearchIndexer {
  constructor(dao, databaseName, sourceType, sourceName, elasticsearch, indexName ) {
    this.dao = dao
    this.databaseName = databaseName
    this.sourceType = sourceType
    this.sourceName = sourceName
    this.elasticsearch = elasticsearch
    this.indexName = indexName
    this.state = SEARCH_INDEX_NOTSTARTED
    this.lastUpdateId = ''

    this.observable = null

    this.queue = []
    this.queueWriteResolve = []
    this.queueLastUpdateId = ''
    this.queueWritePromise = null
    this.queueWriteResolve = null
    this.currentWritePromise = null

    this.lastStateSave = 0
    this.saveStateTimer = null
  }

  async start() {
    const searchIndexState = await this.dao.get(
        ['database','tableObject', this.databaseName, 'searchIndexes', this.indexName])
    const firstSourceOperation = await this.dao.get(
        ['database', this.sourceType.toLowerCase()+'OpLogRange', this.databaseName, this.sourceName, {
          limit: 1
        }])

    let lastUpdateTimestamp = 0
    if(!searchIndexState || (firstSourceOperation && firstSourceOperation.id > searchIndexState.lastOpLogId)) {
      const indexCreateTimestamp = Date.now()
      this.state = SEARCH_INDEX_CREATING
      await this.copyAll()
      lastUpdateTimestamp = indexCreateTimestamp - 1000 // one second overlay
    } else {
      this.state = SEARCH_INDEX_UPDATING
      await this.updateAll()
      lastUpdateTimestamp = (+searchIndexState.lastOpLogId.split(':')[0]) - 1000 // one second overlap
    }

    this.position = (''+(lastUpdateTimestamp - 1000)).padStart(16,'0')

    await this.dao.request(['database', 'put', this.databaseName, 'searchIndexes', {
      id: this.indexName,
      lastOpLogId: this.lastUpdateId
    }])
    this.lastStateSave = Date.now()

    this.state = SEARCH_INDEX_READY

    this.observeMore()
  }

  async saveState() {
    if(Date.now() - this.lastStateSave < saveStateThrottle) {
      if(this.saveStateTimer === null) {
        setTimeout(() => this.saveState(), saveStateDelay)
      }
      return
    }
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
    this.queueWritePromise = new Promise((resolve, reject) => {
      this.queueWriteResolve = { resolve, reject }
    })
    const operations = this.queue
    const queueResolve = this.queueWriteResolve
    this.queueWriteResolve = null
    this.queueWritePromise = null
    this.queue = []
    this.elasticsearch.bulk({
      index: this.indexName,
      body: operations
    }).catch(error => {
      error = (error && error.meta && error.meta.body && error.meta.body.error) || error
      console.error("ES ERROR:", error)
      queueResolve.reject(error)
      throw error
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
        operations[pos++] = op.operation.object
      }
      if(op.operation.type == 'delete') {
        operations[pos++] = { delete: { _id: op.operation.object.id } }
      }
    }
    const lastUpdateId = ops[ops.length-1].id
    this.queue = this.queue.length ? this.queue.concat(operations) : operations
    this.queueLastUpdateId = lastUpdateId
    await this.doWrite()
  }

  observeMore() {
    if(this.observable) this.observable.unobserve(this)
    this.observable = this.dao.observable(
        ['database', this.sourceType.toLowerCase()+'OpLogRange', this.databaseName, this.sourceName, {
          gt: this.lastUpdateId,
          limit: bucketSize
        }])
    this.observable.observe(this)
  }
  async set(ops) {
    await this.applyOps(ops)
    if(this.observable.list.length == bucketSize) {
      this.observeMore()
    }
  }
  async putByField(_fd, id, op, _reverse, oldObject) {
    await this.applyOps([ op ])
    if(this.observable.list.length == bucketSize) {
      this.observeMore()
    }
  }

  async updateAll() {
    let ops
    do {
      ops = await this.dao.get(
          ['database', this.sourceType.toLowerCase()+'OpLogRange', this.databaseName, this.sourceName, {
            gt: this.lastUpdateId,
            limit: bucketSize
          }])
      //console.log("GOT TABLE OPS", this.databaseName, this.tableName, ":", JSON.stringify(ops, null, "  "))
      await this.applyOps(ops)
    } while(ops.length >= bucketSize && this.state != TABLE_CLOSED)
  }

  async copyAll() {
    const search = this.elasticsearch
    let position = ""
    let more = true

    console.log(`INDEXING ${this.sourceType} ${this.sourceName}`)

    do {
      const rows = await this.dao.get(
          ['database', this.sourceType.toLowerCase()+'Range', this.databaseName, this.sourceName, {
            gt: position,
            limit: bucketSize
          }])
      position = rows.length ? rows[rows.length-1].id : "\xFF"
      more = (rows.length == bucketSize)

      if(more) console.log(`REED ${rows.length} ROWS`)
        else console.log(`REED LAST ${rows.length} ROWS`)

      console.log(`WRITING ${rows.length} ROWS`)
      let operations = new Array(rows.length*2)
      for(let i = 0; i < rows.length; i++) {
        operations[i * 2] = { index: { _id: rows[i].id } }
        operations[i * 2 + 1] = rows[i]
      }
      await search.bulk({
        index: this.indexName,
        body: operations
      }).catch(error => {
        error = (error && error.meta && error.meta.body && error.meta.body.error) || error
        console.error("ES ERROR:", error)
        throw error
      })

    } while (more)
  }

}

module.exports = SearchIndexer