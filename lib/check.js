'use strict';

/**
 * 
 * @param {*} options 
 */
function database(pool, options, c, done) {
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
function subscriptions(pool, options, c, done) {
    pool.query('SELECT * FROM "wrkr_subscriptions"', err => {
        if (err && (!err.code || err.code !== '42P01')) return done(err);

        // Error code '42703' missing table
        if (err && err.code && err.code === '42P01') {
            return require('./setup').createSubscriptionsTable(pool, options, done);
        }

        return done(null);
    });
}

/**
 * 
 * @param {*} pool 
 * @param {*} options 
 * @param {*} c 
 * @param {*} done 
 */
function items(pool, options, c, done) {
    pool.query('SELECT * FROM "wrkr_items"', err => {
        if (err && (!err.code || err.code !== '42P01')) return done(err);

        // Error code '42703' missing table
        if (err && err.code && err.code === '42P01') {
            return require('./setup').createItemsTable(pool, options, done);
        }

        return done(null);
    });
}

module.exports = {
    database: database,
    subscriptions: subscriptions,
    items: items
};