const load = require('./load')
const [,,endpoint] = process.argv

load(endpoint, { password: 'password' })
