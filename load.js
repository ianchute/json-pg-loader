const request = require('request')
const pg = require('pg')
const _ = require('underscore')

function load(endpoint, db, callback) {

  const context = {}
  const TYPEMAP = {
    number: 'NUMERIC',
    string: 'TEXT'
  }
  let client

  context.endpoint = endpoint
  context.tablename = endpoint.split('/').slice(3).join('_')
  context.callback = callback

  client = new pg.Client(db)
  client.connect(loadFromEndpoint)

  function loadFromEndpoint() {
    console.log(`Loading data from ${context.endpoint}`)
    request(context.endpoint, loadToDb)
  }

  function loadToDb(err, response) {

    if (!response) {
      end()
      return
    }

    if (typeof response.body === 'string') {
      try {
        response.body = JSON.parse(response.body)

        if (!_.isArray(response.body)) {
          end()
          return
        }
      } catch (e) {
        end()
        return
      }
    }


    context.data = response.body

    createTable()

  }

  function createTable() {

    console.log(`Creating table ${context.tablename}...`)

    const keys = _.keys(context.data[0])

    const query = `
      DROP TABLE IF EXISTS ${context.tablename};
      CREATE TABLE ${context.tablename} (
        ${keys.map(key => `${key.replace(/\s+/g, '_')} ${TYPEMAP[typeof context.data[0][key]]}`).join(',\n')}
      );
    `

    client.query(query, [], populateTable)

  }

  function populateTable(err) {

    console.log(`Populating table ${context.tablename}...`)

    if (err) {
      console.log('error')
      end()
      return;
    } else {
      console.log('success');
    }

    const keys = _.keys(context.data[0])

    const query = `INSERT INTO ${context.tablename} VALUES ` +
      context.data.map(datum => `(${keys.map(key => typeof datum[key] === 'string' ? `'${datum[key]}'` : datum[key]).join(',')})`).join(',') + ';'

    client.query(query, [], (err) => {
      if (err) {
        console.log(err)
      }

      end()
    })

  }

  function end() {
    client.end(() => {
      if (_.isFunction(context.callback))
        context.callback(context.data)
    })
  }

}

module.exports = load
