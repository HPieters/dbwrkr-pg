'use strict';

//Modules
const knexQueryBuilder = require('knex')({client: 'pg'});
const flw = require('flw');
const massive = require('massive');
const debug = require('debug')('dbwrkr:postgresql');

/**
 * Setup tables
 * 
 * @param {Object} db 
 * @param {String} dbName 
 * @param {Function} callback 
 */
function setupTables(db, dbName, callback) {
    debug('Setup tables for database', {db: dbName});

    return flw.series([
        createTableAndIndexSubscriptions,
        createTableAndIndexEvents,
        reloadMassive
    ], callback);

    function createTableAndIndexSubscriptions (c, cb) {
        if (db.wrkr_subscriptions) return cb();

        debug('Create table', {table: 'wrkr_subscriptions'});
        db.run(subscriptionsSchema()).then(result => { return cb(null, result)}).catch(cb);
    }
    
    function createTableAndIndexEvents (c, cb) {
        if (db.wrkr_items) return cb();

        debug('Create table', {table: 'wrkr_items'});
        db.run(itemsSchema()).then(result => { return cb(null, result)}).catch(cb);
    }

    // The reload function cleans out your database’s API and performs the introspection again
    function reloadMassive (c, cb) {      
        db.reload().then(db => { c._store('db', cb)(null, db); }).catch(cb);
    }
}

/**
 * Create subscription schema
 * 
 * subscripions:
 *   eventName   String  (indexed)
 *   queues      [String]
 */
function subscriptionsSchema() {
    return knexQueryBuilder.schema.withSchema('public').createTable('wrkr_subscriptions', function (table) {
        table.string('eventName').primary().unique();
        table.specificType('queues', 'text[]').defaultTo('{}');
        table.index('eventName');
    }).toString();
}

/**
 * Create items schema
 * 
* qitems:
 *  name        String  (indexed)
 *  queue       String  (indexed)
 *  tid         Integer
 *  payload     Object
 *  parent      _ObjectId
 *  created     Date
 *  when        Date    (partial index)
 *  done        Date    (partial index)
 *  retryCount  Number
 */
function itemsSchema () {
    var query = knexQueryBuilder.schema.withSchema('public').createTable('wrkr_items', function (table) {
        table.increments('id').primary();
        table.string('name');
        table.string('queue');
        table.string('tid');
        table.jsonb('payload');
        table.integer('parent')
        table.timestamp('created');
        table.timestamp('when');
        table.timestamp('done');
        table.integer('retryCount');
        table.index('name');
        table.index('queue');
    }).raw('CREATE INDEX "when_index" ON "public"."wrkr_items" ("when") WHERE NOT "when" IS NULL')
    .raw('CREATE INDEX "done_index" ON "public"."wrkr_items" ("done") WHERE NOT "done" IS NULL')
    .toString();

    return query;
}

module.exports = setupTables;