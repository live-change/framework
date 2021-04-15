const { Client: ElasticSearch } = require('@elastic/elasticsearch')

class AnalyticsWriter {

  constructor(indexPrefix) {
    this.dbPromise = null
    this.currentIndex = null
    this.isWriting = false
    this.indexPrefix = indexPrefix
    this.queue = []
  }

  initDb(index) {
    if(this.currentIndex == index && this.dbPromise) return this.dbPromise
    this.dbPromise = new Promise(async (resolve, reject) => {
      const db = new ElasticSearch({ node: process.env.ANALYTICS_URL || 'http://localhost:9200' })
      this.currentIndex = index
      await db.indices.create({
        index: this.currentIndex,
        body: {
          mappings: {
            properties: {
              timestamp: {
                type: "date"
              },
              clientTS: {
                type: "date"
              }
            }
          }
        }
      }).catch(err => {
        if(err.meta.body) {
          console.error("ES ERR: ", err.meta.body)
          if(err.meta.body.error.type == 'resource_already_exists_exception') return db
        } else {
          console.error("ES ERROR: ", err)
        }
        throw err
      })
      db.info(console.log)
      resolve(db)
    })
    return this.dbPromise
  }

  saveEvents(events) {
    this.queue = this.queue.concat(events)
    this.writeEvents()
  }

  writeEvents() {
    if(this.isWriting) return
    if(this.queue.length == 0) return
    this.isWriting = true
    const index = this.indexPrefix+(new Date()).toISOString().slice(0, 7)
    const data = this.queue.slice()
    this.queue = []
    let operations = new Array(data.length*2)
    for(let i = 0; i < data.length; i++) {
      operations[i * 2] = { index: { } }
      operations[i * 2 + 1] = data[i]
    }
    this.initDb(index).then(db => db.bulk({
      index,
      body: operations
    })).then(result => {
      this.isWriting = false
      if(this.queue.length > 0) setTimeout(() => this.writeEvents(), 10)
    }).catch(error => {
      console.error("COULD NOT WRITE EVENTS to ES!", error)
      this.queue = data.concat(this.queue)
      this.isWriting = false
    })
  }

}

module.exports = AnalyticsWriter
