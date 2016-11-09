const request = require('request')
const pg = require('pg')
const _ = require('underscore')
const context = {}
const TYPEMAP = {
  number: 'NUMERIC',
  string: 'TEXT'
}
let client

function load(endpoint, db) {

  context.endpoint = endpoint
  context.tablename = endpoint.split('/').slice(3).join('_')

  client = new pg.Client(db)
  client.connect(loadFromEndpoint)

}

function loadFromEndpoint() {
  console.log(`Loading data from ${context.endpoint}`)
  request(context.endpoint, loadToDb)
}

function loadToDb(err, { body }) {

  if (typeof body === 'string')
    body = JSON.parse(body)

  context.data = body

  createTable()

}

function createTable() {

  const keys = _.keys(context.data[0])

  const query = `
    DROP TABLE IF EXISTS ${context.tablename};
    CREATE TABLE ${context.tablename} (
      ${keys.map(key => `${key} ${TYPEMAP[typeof context.data[0][key]]}`).join(',\n')}
    );
  `

  console.log(`Creating table ${context.tablename} with the following structure:`)
  console.log(query)
  client.query(query, [], populateTable)

}

function populateTable(err) {

  if (err) {
    console.log(err)
    return;
  }

  const keys = _.keys(context.data[0])

  const query = `INSERT INTO ${context.tablename} VALUES ` +
    context.data.map(datum => `(${keys.map(key => typeof datum[key] === 'string' ? `'${datum[key]}'` : datum[key]).join(',')})`).join(',') + ';'

  console.log(`Populating table ${context.tablename}`)
  client.query(query, [], (err) => {
    if (err) {
      console.log(err)
      return;
    }

    client.end()
  })

}

module.exports = load
