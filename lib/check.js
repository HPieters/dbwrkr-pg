'use strict';

// Modules
const _ = require('lodash');
const async = require('async');

/**
 * Check if database and tables exist, if not create
 * 
 * @param {Object} pool 
 * @param {Object} options 
 * @param {Function} done Callback
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
 * Check if dtabase exists, if not create
 * 
 * @param {Object} pool 
 * @param {Object} options 
 * @param {Function} done Callback
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
 * Check if 'wrkr_events' exist
 * 
 * @param {Object} pool 
 * @param {Object} options 
 * @param {Function} done Callback
 */
function events(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_events"', _.partial(checkHandler, 'wrkr_events', pool, options, done));
}

/**
 * Check if 'wrkr_queues' exists
 * 
 * @param {Object} pool 
 * @param {Object} options 
 * @param {Function} done Callback
 */
function queues(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_queues"', _.partial(checkHandler, 'wrkr_queues', pool, options, done));
}

/**
 * Check if 'wrkr_subscriptions' exists
 * 
 * @param {Object} pool 
 * @param {Object} options 
 * @param {Function} done Callback
 */
function subscriptions(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_subscriptions"', _.partial(checkHandler, 'wrkr_subscriptions', pool, options, done));
}

/**
 * Check if 'wrkr_items' exists
 * 
 * @param {Object} pool 
 * @param {Object} options 
 * @param {Function} done Callback
 */
function items(pool, options, done) {
    pool.query('SELECT * FROM "wrkr_items"', _.partial(checkHandler, 'wrkr_items', pool, options, done));
}

/**
 * Handle check callback for errors, if does not exist create
 * 
 * @param {String} tableName 
 * @param {Object} pool 
 * @param {Object} options 
 * @param {Function} done Callback
 * @param {Object} err 
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