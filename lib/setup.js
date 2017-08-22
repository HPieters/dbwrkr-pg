'use strict';

//Modules
const debug = require('debug')('dbwrkr:postgresql');
const {Client} = require('pg');

/**
 * Create subscriptions table
 * 
 * subscripions:
 *   eventName   String  (indexed)
 *   queues      [String]
 * 
 * @param {Object} pool Pool reference
 * @param {Object} options Options object
 * @param {Function} done callback function
 */
function createSubscriptionsTable(pool, options, done) {
    debug('Create table', {table: 'wrkr_subscriptions'});

    const query = `
        CREATE TABLE "public"."wrkr_subscriptions" ("eventName" varchar(255), "queues" text[] default '{}');
        ALTER TABLE "public"."wrkr_subscriptions" add constraint "wrkr_subscriptions_pkey" primary key ("eventName");
        ALTER TABLE "public"."wrkr_subscriptions" add constraint "wrkr_subscriptions_eventname_unique" unique ("eventName");
        CREATE index "wrkr_subscriptions_eventname_index" on "public"."wrkr_subscriptions" ("eventName")`;

    pool.query(query, done);
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
        CREATE TABLE "public"."wrkr_items" ("id" serial primary key, "name" varchar(255), "queue" varchar(255), "tid"    varchar(255), "payload" jsonb, "parent" integer, "created" timestamptz, "when" timestamptz, "done" timestamptz,         "retryCount" integer);
        CREATE INDEX "wrkr_items_name_index" on "public"."wrkr_items" ("name");
        CREATE INDEX "wrkr_items_queue_index" on "public"."wrkr_items" ("queue");
        CREATE INDEX "when_index" ON "public"."wrkr_items" ("when") WHERE NOT "when" IS NULL;
        CREATE INDEX "done_index" ON "public"."wrkr_items" ("done") WHERE NOT "done" IS NULL`;

    pool.query(query, done);
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
        database: 'postgres' // Should always be there...
    });

    tmpClient.connect();

    tmpClient.query(`CREATE DATABASE ${options.database}`, err => {
        if (err) return done(err);
        tmpClient.end(done);
    });
}

module.exports = {
    createDatabase: createDatabase,
    createSubscriptionsTable: createSubscriptionsTable,
    createItemsTable: createItemsTable
};
