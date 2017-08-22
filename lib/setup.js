'use strict';

//Modules
const debug = require('debug')('dbwrkr:postgresql');
const {Client} = require('pg');

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} done 
 */
function createSubscriptionsTable(pool, options, done) {
    debug('Create table', {table: 'wrkr_subscriptions'});
    pool.query(subscriptionsSchema(), done);
}

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} done 
 */
function createItemsTable(pool, options, done) {
    debug('Create table', {table: 'wrkr_items'});
    pool.query(itemsSchema(), done);
}

/**
 * Create subscription schema
 * 
 * subscripions:
 *   eventName   String  (indexed)
 *   queues      [String]
 */
function subscriptionsSchema() {
    // const query = knexQueryBuilder.schema.withSchema('public').createTable('wrkr_subscriptions', function (table) {
    //     table.string('eventName').primary().unique();
    //     table.specificType('queues', 'text[]').defaultTo('{}');
    //     table.index('eventName');
    // }).toString();

    const query = `create table "public"."wrkr_subscriptions" ("eventName" varchar(255), "queues" text[] default '{}');
    alter table "public"."wrkr_subscriptions" add constraint "wrkr_subscriptions_pkey" primary key ("eventName");
    alter table "public"."wrkr_subscriptions" add constraint "wrkr_subscriptions_eventname_unique" unique ("eventName");
    create index "wrkr_subscriptions_eventname_index" on "public"."wrkr_subscriptions" ("eventName")`;

    return query;
}

/**
 * Create items schema
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
 */
function itemsSchema() {
    // const query = knexQueryBuilder.schema.withSchema('public').createTable('wrkr_items', function (table) {
    //     table.increments('id').primary();
    //     table.string('name');
    //     table.string('queue');
    //     table.string('tid');
    //     table.jsonb('payload');
    //     table.integer('parent');
    //     table.timestamp('created');
    //     table.timestamp('when');
    //     table.timestamp('done');
    //     table.integer('retryCount');
    //     table.index('name');
    //     table.index('queue');
    // }).raw('CREATE INDEX "when_index" ON "public"."wrkr_items" ("when") WHERE NOT "when" IS NULL')
    //     .raw('CREATE INDEX "done_index" ON "public"."wrkr_items" ("done") WHERE NOT "done" IS NULL')
    //     .toString();

    const query = `create table "public"."wrkr_items" ("id" serial primary key, "name" varchar(255), "queue" varchar(255), "tid"    varchar(255), "payload" jsonb, "parent" integer, "created" timestamptz, "when" timestamptz, "done" timestamptz,         "retryCount" integer);
        create index "wrkr_items_name_index" on "public"."wrkr_items" ("name");
        create index "wrkr_items_queue_index" on "public"."wrkr_items" ("queue");
        CREATE INDEX "when_index" ON "public"."wrkr_items" ("when") WHERE NOT "when" IS NULL;
        CREATE INDEX "done_index" ON "public"."wrkr_items" ("done") WHERE NOT "done" IS NULL`;

    return query;
}

/**
 * Create database
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
