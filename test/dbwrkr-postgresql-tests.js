/* eslint no-console: 0 */
const DBWrkrPostgreSQL = require('../dbwrkr-mongodb');
const tests = require('dbwrkr').tests;

tests({
  storage: new DBWrkrPostgreSQL({
    dbName: 'dbwrkr'
  })
});