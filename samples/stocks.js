const load = require('../load')
const _ = require('underscore')
const db = require('../db.json')

load('http://api.manilainvestor.com/v1/stocks', db, (data) => {
  let p = new Promise(r => r())

  _.pluck(data, 'securitySymbol').forEach(symbol => {

    p = new Promise(r => {

      p.then(() => {
        load(
          `http://api.manilainvestor.com/v1/stocks/hdata/${symbol}`,
          db,
          r
        )
      })

    })

  })
})
