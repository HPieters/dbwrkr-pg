'use strict';

//Modules
const debug = require('debug')('dbwrkr:postgresql');
const Client = require('pg').Client;
const async = require('async');

/**
 * 
 * @param {*} tableName 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} done 
 */
function createTable(tableName, pool, options, done) {
    const createTableByTableName = {
        'wrkr_events': createEventsTable,
        'wrkr_queues': createQueueTable,
        'wrkr_subscriptions': createSubscriptionsTable,
        'wrkr_items': createItemsTable,
    };

    return createTableByTableName[tableName](pool, options, done);
}

/**
 * Create events table
 * 
 * events:
 *   event_id   Integer  
 *   queue_id   Integer
 * 
 * @param {Object} pool Pool reference
 * @param {Object} options Options object
 * @param {Function} done callback function
 */
function createEventsTable(pool, options, done) {
    debug('Create table', {table: 'wrkr_events'});

    const query = `
        CREATE TABLE "public"."wrkr_events" (
            "id" SERIAL PRIMARY KEY,
            "name"text NOT NULL
        ); 
        CREATE INDEX "wrkr_events_name_index" on "public"."wrkr_events" ("name");`;

    pool.query(query, done);
}

/**
 * Create queues table
 * 
 * queues:
 *   id         Integer  Primary 
 *   name       Text (String)
 * 
 * @param {Object} pool Pool reference
 * @param {Object} options Options object
 * @param {Function} done callback function
 */
function createQueueTable(pool, options, done) {
    debug('Create table', {table: 'wrkr_queues'});

    const query = `
        CREATE TABLE "public"."wrkr_queues" (
            "id" SERIAL PRIMARY KEY,
            "name" text NOT NULL
        );`;

    const indexQuery = 'CREATE INDEX "wrkr_queues_name_index" on "public"."wrkr_queues" ("name");';

    async.series([
        cb => pool.query(query, cb),
        cb => pool.query(indexQuery, cb)
    ], done);
}

/**
 * Create subscriptions table
 * 
 * @TODO create index on queue_id?
 * 
 * subscripions:
 *   event_id   Integer  
 *   queue_id   Integer
 * 
 * @param {Object} pool Pool reference
 * @param {Object} options Options object
 * @param {Function} done callback function
 */
function createSubscriptionsTable(pool, options, done) {
    debug('Create table', {table: 'wrkr_subscriptions'});

    const query = `
        CREATE TABLE "public"."wrkr_subscriptions" (
            "event_id" integer NOT NULL, 
            "queue_id" integer NOT NULL,
            PRIMARY KEY ("event_id", "queue_id")
        );`;

    const alterForeignKeyEventId = `
        ALTER TABLE "public"."wrkr_subscriptions" 
        ADD FOREIGN KEY ("event_id") 
        REFERENCES "public"."wrkr_events"("id");`;

    const alterForeignKeyQueueId = `
        ALTER TABLE "public"."wrkr_subscriptions" 
        ADD FOREIGN KEY ("queue_id") 
        REFERENCES "public"."wrkr_queues"("id");
        `;

    async.series([
        cb => pool.query(query, cb),
        cb => pool.query(alterForeignKeyEventId, cb),
        cb => pool.query(alterForeignKeyQueueId, cb),
    ], done);
}

/**
 * Create items table
 * 
 * qitems:
 *  name        String  (indexed)
 *  queue       String  (indexed)
 *  tid         String
 *  payload     Object
 *  parent      _ObjectId
 *  created     Date
 *  when        Date    (partial index)
 *  done        Date    (partial index)
 *  retryCount  Number
 * 
 * @param {Object} pool Pool reference
 * @param {Object} options Options object
 * @param {Function} done callback function
 */
function createItemsTable(pool, options, done) {
    debug('Create table', {table: 'wrkr_items'});

    const query = `
        CREATE TABLE "public"."wrkr_items" (
            "id"            serial primary key, 
            "event_id"      integer, 
            "queue_id"      integer, 
            "tid"           text, 
            "payload"       jsonb, 
            "parent"        integer, 
            "created"       timestamptz, 
            "when"          timestamptz, 
            "done"          timestamptz,         
            "retryCount"    integer
        );
        CREATE INDEX "wrkr_items_name_index" on "public"."wrkr_items" ("event_id");
        CREATE INDEX "wrkr_items_queue_index" on "public"."wrkr_items" ("queue_id");
        CREATE INDEX "when_index" ON "public"."wrkr_items" ("when") WHERE NOT "when" IS NULL;
        CREATE INDEX "done_index" ON "public"."wrkr_items" ("done") WHERE NOT "done" IS NULL;`;

    const alterForeignKeyEventId = `
        ALTER TABLE "public"."wrkr_subscriptions" 
        ADD FOREIGN KEY ("event_id") 
        REFERENCES "public"."wrkr_events"("id");`;

    const alterForeignKeyQueueId = `
        ALTER TABLE "public"."wrkr_subscriptions" 
        ADD FOREIGN KEY ("queue_id") 
        REFERENCES "public"."wrkr_queues"("id");`;

    async.series([
        cb => pool.query(query, cb),
        cb => pool.query(alterForeignKeyEventId, cb),
        cb => pool.query(alterForeignKeyQueueId, cb)
    ], done);
}

/**
 * Create database based on options.database
 * 
 * @param {Object} options 
 * @param {Function} done 
 */
function createDatabase(options, done) {
    const tmpClient = new Client({
        host: options.host,
        port:  options.post,
        user: options.user,
        password: options.password,
        database: 'postgres' // Should always be there, what about cockroach?
    });

    tmpClient.connect();

    tmpClient.query(`CREATE DATABASE ${options.database}`, err => {
        if (err) return done(err);
        tmpClient.end(done);
    });
}

module.exports = {
    createDatabase: createDatabase,
    createTable: createTable
};
