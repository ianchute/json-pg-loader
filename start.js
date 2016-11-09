const load = require('./load')
const [,,endpoint] = process.argv
const db = require('./db.json')

load(endpoint, db)
