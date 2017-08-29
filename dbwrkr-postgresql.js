'use strict';

// Modules
const assert = require('assert');
const debug = require('debug')('dbwrkr:postgresql');
const Pool = require('pg').Pool;
const _ = require('lodash');
const async = require('async');
const LRU = require('lru-cache');

// Libraries
const checkDatabaseAndTables = require('./lib/check');
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

    this.getRelationalValue = _.curry(utils.getRelationalValue.bind(this));
    this.getOrInsertIdValue = _.curry(utils.getOrInsertIdValue.bind(this));
    this.mapCriteria = _.curry(utils.mapCriteria.bind(this));
    this.fieldMapper = _.curry(utils.fieldMapper.bind(this));
    this.convertEventsToQuery = _.curry(utils.convertEventsToQuery.bind(this));

    this.pool = null;
    this.memorizedEventIds = LRU();
    this.memorizedQueueIds = LRU();
    this.memorizedEventNames = LRU();
    this.memorizedQueueNames = LRU();
}

/**
 * Connect to database, set internal references and setup tables if they do not exist yet
 *
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.connect = function connect(done) {
    debug('Connecting to PostgreSQL', this.pgOptions);

    this.pool = new Pool(this.pgOptions);
    checkDatabaseAndTables(this.pool, this.pgOptions, done);
};

/**
 * Disconnect from PostgreSQL
 * @TODO Handle disconnect in massive.js
 *
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.disconnect = function disconnect(done) {
    debug('Disconnecting from PostgreSQL', this.pgOptions);
    if (!this.pool) return done();

    this.memorizedEventIds.reset();
    this.memorizedQueueIds.reset();
    this.memorizedEventNames.reset();
    this.memorizedQueueNames.reset();
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

    async.parallel([
        this.getOrInsertIdValue('event', eventName),
        this.getOrInsertIdValue('queue', queueName)
    ], (err, result) => {
        if (err) return done(err);
        // Insert subscription into database, database will refuse if duplicate
        const subscribeQuery = 'INSERT INTO "wrkr_subscriptions" ("event_id", "queue_id") VALUES ($1, $2);';
        this.pool.query(subscribeQuery, [result[0], result[1]], done);
    });
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

    async.parallel([
        this.getRelationalValue('id', 'event', eventName),
        this.getRelationalValue('id', 'queue', queueName)
    ], (err, result) => {
        if (err) return done(err);

        const unsubscribeQuery = 'DELETE FROM "wrkr_subscriptions" WHERE "event_id" = $1 AND "queue_id" = $2';
        this.pool.query(unsubscribeQuery, [result[0], result[1]], done);
    });
};

/**
 * Subscriptions
 * 
 * @param {String} eventName
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.subscriptions = function subscriptions(eventName, done) {
    debug('Subscriptions ', {event: eventName});

    this.getRelationalValue('id', 'event', eventName, (err, eventId) => {
        if (err && err !== 'noRecordsFound') return done(err);
        if (err && err === 'noRecordsFound') return done(null, []);

        const subscriptionsQuery = `
            SELECT      su.event_id, qu.name
            FROM        wrkr_subscriptions AS su
            LEFT JOIN   wrkr_queues AS qu ON su.queue_id=qu.id
            WHERE       event_id = $1`;

        this.pool.query(subscriptionsQuery, [eventId], (err, res) => {
            if (err) return done(err);

            const subscriptions = res.rows;

            if (subscriptions.length === 0) {
                return done(null, []);
            }

            const queueNames = _.reduce(subscriptions, (subscriptionQueues, subscription) => {
                subscriptionQueues.push(subscription.name);
                return subscriptionQueues;
            }, []);

            return done(null, queueNames);
        });
    });
};

/**
 * Publish events
 *
 * @TODO rewrite convert Event to Query
 * 
 * @param {String} eventName
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.publish = function publish(events, done) {
    const publishEvents = Array.isArray(events) ? events : [events];

    debug('Publish ', publishEvents);

    const queryData = utils.convertEventsToQuery(events);
    const publishQuery = `
        INSERT INTO wrkr_items (
            "event_id", 
            "queue_id", 
            "tid", 
            "payload", 
            "parent", 
            "created", 
            "when", 
            "retryCount") 
        VALUES 
            ${queryData.text} 
        RETURNING *`;

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

    this.getRelationalValue('id', 'queue', queue, (err, queueId) => {
        if (err) return done(err);

        const fetchNextQuery = `
            WITH itemId AS (
                SELECT      id
                FROM        wrkr_items
                WHERE       "queue_id" = $1
                AND         "when" <= $2
                ORDER BY    created DESC
                LIMIT  1
            )
            UPDATE wrkr_items
            SET    "when" = NULL, "done" = $2
            FROM   itemId
            WHERE  wrkr_items.id = itemId.id
            RETURNING *;
        `;

        this.pool.query(fetchNextQuery, [queueId, new Date()], (err, res) => {
            if (err) return done(err);

            let result = res && res.rows ? res.rows[0] : false;

            debug('fetchNext result', result);
            if (!result || _.isArray(result) && result.length === 0) return done(null, undefined);
            result = _.isArray(result) ? _.first(result) : result;

            debug('fetchNext item', result);
            this.fieldMapper(result, done);
        });
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

            async.map(result.rows, (row, next) => {
                this.fieldMapper(this, row, next);
            }, done);
        });
    }

    if (criteria.id && Array.isArray(criteria.id)) {
        criteria.id = _.first(criteria.id);
    }

    this.mapCriteria(criteria, (err, mappedCriteria) => {
        if (err) return done(err);

        const whereSQL = utils.createWhereSQL(mappedCriteria, 1);

        if (!mappedCriteria.id && !mappedCriteria.when) {
            const newDate = new Date(0, 0, 0);
            if (Object.keys(mappedCriteria).length === 0) {
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
            async.map(result.rows, (row, next) => {
                this.fieldMapper(row, next);
            }, done);
        });
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

    this.mapCriteria(criteria, (err, mappedCriteria) => {
        if (err) return done(err);

        const whereSQL = utils.createWhereSQL(mappedCriteria, 1);
        const removeQuery = `
            DELETE FROM "wrkr_items"
            ${whereSQL.text}
        `;

        this.pool.query(removeQuery, whereSQL.values, err => {
            if (err) return done(err);

            debug('Removed', mappedCriteria);
            done(null);
        });
    });
};

module.exports = DbWrkrPostgreSQL;
