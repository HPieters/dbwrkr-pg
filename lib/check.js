'use strict';

// Modules
const _ = require('lodash');
const async = require('async');

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} done 
 */
function checkDatabaseAndTables(pool, options, done) {

    async.series([
        _.partial(database, pool, options),
        _.partial(events, pool, options),
        _.partial(queues, pool, options),
        _.partial(subscriptions, pool, options),
        _.partial(items, pool, options)
    ], done);
}

/**
 * 
 * @param {*} options 
 */
function database(pool, options, done) {
    pool.query('SELECT VERSION() as VERSION', err => {
        if (err && (!err.code || err.code !== '3D000')) return done(err);

        // Error code '3D000' missing db
        if (err && err.code && err.code === '3D000') {
            return require('./setup').createDatabase(options, done);
        }

        done(null);
    });
}

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} c 
 * @param {*} done 
 */
function events(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_events"', _.partial(checkHandler, 'wrkr_events', pool, options, done));
}

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} c 
 * @param {*} done 
 */
function queues(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_queues"', _.partial(checkHandler, 'wrkr_queues', pool, options, done));
}

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} c 
 * @param {*} done 
 */
function subscriptions(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_subscriptions"', _.partial(checkHandler, 'wrkr_subscriptions', pool, options, done));
}

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} c 
 * @param {*} done 
 */
function items(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_items"', _.partial(checkHandler, 'wrkr_items', pool, options, done));
}

/**
 * 
 * @param {*} tableName 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} done 
 * @param {*} err 
 */
function checkHandler(tableName, pool, options, done, err) {
    if (err && (!err.code || err.code !== '42P01')) return done(err);

    // Error code '42703' missing table
    if (err && err.code && err.code === '42P01') {
        return require('./setup').createTable(tableName, pool, options, done);
    }

    return done(null);
}

module.exports = checkDatabaseAndTables;