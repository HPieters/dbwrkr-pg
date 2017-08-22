'use strict';

// Modules
const assert = require('assert');
const debug = require('debug')('dbwrkr:postgresql');
const {Pool} = require('pg');
const _ = require('lodash');
const flw = require('flw');

// Libraries
const check = require('./lib/check');
const utils = require('./lib/utils');

/**
 * DbWrkrPostgreSQL Constructor
 *
 * @param {Object} opt Options object
 */
function DbWrkrPostgreSQL(opt) {
    if (!(this instanceof DbWrkrPostgreSQL)) {
        return new DbWrkrPostgreSQL(opt);
    }

    debug('DbWrkrPostgreSQL - opt', opt);

    this.pgOptions = {
        database: opt.dbName || '',
        host: opt.host || 'localhost',
        port: opt.dbPort || 5432,
        user: opt.username || undefined,
        password: opt.password || undefined,
        timeout: opt.timeout || undefined,
        ssl: opt.ssl || undefined
    };

    assert(this.pgOptions.database, 'has database name');
    assert(this.pgOptions.port, 'has database port');

    this.pool = null;
}

/**
 * Connect to database, set internal references and setup tables if they do not exist yet
 *
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.connect = function connect(done) {
    debug('Connecting to PostgreSQL', this.pgOptions);

    this.pool = new Pool(this.pgOptions);

    flw.series([
        _.partial(check.database, this.pool, this.pgOptions),
        _.partial(check.subscriptions, this.pool, this.pgOptions),
        _.partial(check.items, this.pool, this.pgOptions)
    ], done);
};

/**
 * Disconnect from PostgreSQL
 * @TODO Handle disconnect in massive.js
 *
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.disconnect = function disconnect(done) {
    if (!this.pool) return done();
    this.pool.end(done);
};

/**
 * Subscribe
 * 
 * @param {String} eventName
 * @param {String} queueName
 * @param {function} callback Callback option
 */
DbWrkrPostgreSQL.prototype.subscribe = function subscribe(eventName, queueName, done) {
    debug('Subscribe ', {event: eventName, queue: queueName});

    const subscribeQuery = `
        INSERT INTO  "wrkr_subscriptions" ("eventName", "queues") 
        VALUES       ($1, $2)
        ON CONFLICT  ("eventName") DO UPDATE 
        SET          "queues" = array_append("wrkr_subscriptions"."queues", $3) 
        WHERE        "wrkr_subscriptions"."eventName" = $1;`;

    this.pool.query(subscribeQuery, [eventName, `{${queueName}}`, queueName], done);
};

/**
 * Unsubscribe
 * 
 * @param {String} eventName
 * @param {String} queueName
 * @param {function} callback Callback option
 */
DbWrkrPostgreSQL.prototype.unsubscribe = function unsubscribe(eventName, queueName, done) {
    debug('Unsubscribe ', {event: eventName, queue: queueName});

    const unsubscribeQuery = `
        UPDATE  "wrkr_subscriptions"
        SET     "queues" = array_remove("wrkr_subscriptions"."queues", $1) 
        WHERE   "wrkr_subscriptions"."eventName" = $2;`;

    this.pool.query(unsubscribeQuery, [queueName, eventName], done);
};

/**
 * Subscriptions
 * 
 * @param {String} eventName
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.subscriptions = function subscriptions(eventName, done) {
    debug('Subscriptions ', {event: eventName});

    const subscriptionsQuery = `
        SELECT  * 
        FROM    wrkr_subscriptions 
        WHERE   "eventName" = $1 
        LIMIT   1`;

    this.pool.query(subscriptionsQuery, [eventName], (err, res) => {
        if (err) return done(err);

        const result = res.rows[0];
        done(null, result ? result.queues : []);
    });
};

/**
 * Publish events
 *
 * @param {String} eventName
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.publish = function publish(events, done) {
    const publishEvents = Array.isArray(events) ? events : [events];

    debug('Publish ', publishEvents);
    const queryData = utils.convertEventsToQuery(events);
    const publishQuery = `
        INSERT INTO wrkr_items ("name", "queue", "tid", "payload", "parent", "created", "when", "retryCount") 
        VALUES ${queryData.text} RETURNING *`;

    this.pool.query(publishQuery, queryData.values, (err, result) => {
        if (err) return done(err);

        if (publishEvents.length !== result.rowCount) {
            return done(new Error('insertErrorNotEnoughEvents'));
        }

        const createdIds = result.rows.map(o => { return o.id.toString(); });
        debug('Published ', publishEvents.length, createdIds);
        return done(null, createdIds);
    });
};

/**
 * Fetch the next item
 * 
 * @TODO Investigate if we should use between function (see rethinkDB implementation)
 * @param {String} queue
 * @param {function} done Callback 
 */
DbWrkrPostgreSQL.prototype.fetchNext = function fetchNext(queue, done) {
    debug('fetchNext', queue);

    const fetchNextQuery = `
        WITH itemId AS (
            SELECT id
            FROM   wrkr_items
            WHERE  "queue" = $1
            AND    "when" <= $2
            ORDER BY created DESC
            LIMIT  1
        )
        UPDATE wrkr_items
        SET    "when" = NULL, "done" = $2
        FROM   itemId
        WHERE  wrkr_items.id = itemId.id
        RETURNING *;
    `;

    this.pool.query(fetchNextQuery, [queue, new Date()], (err, res) => {
        if (err) return done(err);

        let result = res && res.rows ? res.rows[0] : false;

        debug('fetchNext result', result);
        if (!result || _.isArray(result) && result.length === 0) return done(null, undefined);
        result = _.isArray(result) ? _.first(result) : result;

        debug('fetchNext item', result);
        return done(null, utils.fieldMapper(result));
    });
};

/**
 * Find items based on the given criteria
 * 
 * @TODO Check if we can handle multiple id's better.
 * @param {Object} criteria
 * @param {function} done Callback
 */
DbWrkrPostgreSQL.prototype.find = function find(criteria, done) {
    debug('Finding ', criteria);

    // Handle multiple id's
    if (criteria.id && Array.isArray(criteria.id) && criteria.id.length > 1) {
        const ids = criteria.id.join(',');
        const findIdQuery = `
            SELECT *
            FROM   "wrkr_items"
            WHERE  id in ($1)`;

        return this.pool.query(findIdQuery, [`'${ids}'`], (err, result) => {
            if (err) return done(err);
            debug('Found ', result.rows);

            const records = result.rows.map(utils.fieldMapper);
            return done(null, records);
        });
    }

    if (criteria.id && Array.isArray(criteria.id)) {
        criteria.id = _.first(criteria.id);
    }

    const whereSQL = utils.createWhereSQL(criteria);

    if (!criteria.id && !criteria.when) {
        const newDate = new Date(0, 0, 0);
        if (Object.keys(criteria).length === 0) {
            whereSQL.text += ' WHERE ';
        } else {
            whereSQL.text += ' AND ';
        }

        whereSQL.text += `"when" > $${whereSQL.counter} `;
        whereSQL.values = whereSQL.values.concat([`'${newDate.toISOString()}'`]);
    }

    const findQuery = `
        SELECT *
        FROM   "wrkr_items" 
        ${whereSQL.text}`;

    this.pool.query(findQuery, whereSQL.values, (err, result) => {
        if (err) return done(err);

        debug('Found ', result.rows);
        const records = result.rows.map(utils.fieldMapper);
        return done(null, records);
    });
};

/**
 * Remove items based on criteria
 * 
 * @param {Object} criteria
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.remove = function remove(criteria, done) {
    debug('Removing', criteria);

    const whereSQL = utils.createWhereSQL(criteria);
    const removeQuery = `
        DELETE FROM "wrkr_items"
        ${whereSQL.text}
    `;

    this.pool.query(removeQuery, whereSQL.values, err => {
        if (err) return done(err);

        debug('Removed', criteria);
        done(null);
    });
};

module.exports = DbWrkrPostgreSQL;
