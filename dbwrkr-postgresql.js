'use strict';

// Modules
const assert = require('assert');
const debug = require('debug')('dbwrkr:postgresql');
const Pool = require('pg').Pool;
const _ = require('lodash');
const async = require('async');

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

    this.getRelationalValue = _.curry(getRelationalValue.bind(this));
    this.getOrInsertIdValue = _.curry(getOrInsertIdValue.bind(this));
    this.mapCriteria = _.curry(mapCriteria.bind(this));

    this.pool = null;
    this.memorizedEventIds = {};
    this.memorizedQueueIds = {};
    this.memorizedEventNames = {};
    this.memorizedQueueNames = {};
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
    if (!this.pool) return done();
    this.memorizedEvents = null;
    this.memorizedQueues = null;
    this.memorizedEventNames = {};
    this.memorizedQueueNames = {};
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
            fieldMapper(this, result, done);
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
                fieldMapper(this, row, next);
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
                fieldMapper(this, row, next);
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

/**
 * 
 * @param {*} queueName 
 */
function getRelationalValue(relationalType, valueType, value, done) {
    const memorizedIdObject = valueType === 'event' ? 'memorizedEventIds' : 'memorizedQueueIds';
    const memorizedNameObject = valueType === 'event' ? 'memorizedEventNames' : 'memorizedQueueNames';
    const requestedRelationalType = relationalType === 'id' ? memorizedIdObject : memorizedNameObject;

    // Return the relationalType value from memory object if it exists
    if (this[requestedRelationalType] && this[requestedRelationalType][value]) {
        return done(null, this[requestedRelationalType][value]);
    }

    // Get the value from the database
    const getRelationalTable = valueType === 'event' ? 'wrkr_events' :  'wrkr_queues';
    const getRelationalColumn =  relationalType === 'id' ? 'name' : 'id';
    const getRelationalValueQuery = `SELECT * FROM "${getRelationalTable}" WHERE "${getRelationalColumn}" = $1 LIMIT 1`;

    this.pool.query(getRelationalValueQuery, [value], (err, result) => {
        if (err) return done(err);

        const records = result.rows;

        if (records && records.length > 0) {
            const relationalTypeValue = records[0][relationalType];

            if (relationalType === 'id') {
                this[memorizedIdObject][value] = relationalTypeValue;
                this[memorizedNameObject][relationalTypeValue] = value;
            } else {
                this[memorizedNameObject][value] = relationalTypeValue;
                this[memorizedIdObject][relationalTypeValue] = value;
            }
            // Return the value, either id or name
            return done(null, relationalTypeValue);
        }

        done('noRecordsFound');
    });
}



function getOrInsertIdValue(valueType, value, done) {
    this.getRelationalValue('id', valueType, value, (err, result) => {
        if (err && err !== 'noRecordsFound') return done(err);

        if (!err) {
            return done(null, result);
        }

        const getRelationalTable = valueType === 'event' ? 'wrkr_events' :  'wrkr_queues';
        const insertRelationalValueQuery = `INSERT INTO "${getRelationalTable}" ("name") VALUES ($1) RETURNING *`;

        this.pool.query(insertRelationalValueQuery, [value], (err, result) => {
            if (err) return done(err);

            const valueId = result.rows[0].id;
            done(null, valueId);
        });
    });
}


function mapCriteria(criteria, done) {
    const mappedCriteria = Object.assign({}, criteria);

    async.parallel([
        (cb) => {
            if (!mappedCriteria.name) {
                return cb();
            }

            this.getRelationalValue('id', 'event', criteria.name, (err, eventId) => {
                if (err) return cb(err);
                mappedCriteria.event_id = eventId;
                delete mappedCriteria.name;
                cb();
            });
        },
        (cb) => {
            if (!mappedCriteria.queue) {
                return cb();
            }

            this.getRelationalValue('id', 'queue', mappedCriteria.queue, (err, queueId) => {
                if (err) return cb(err);

                mappedCriteria.queue_id = queueId;
                delete mappedCriteria.queue;
                cb();
            });
        }
    ], err => {
        if (err) return done(err);

        done(null, mappedCriteria);
    });
}


/**
 * Map (all) fields to the correct values
 * 
 * @param {Object} item 
 * @return {Object} Mapped item
 */
function fieldMapper(self, item, done) {
    async.parallel([
        cb => {
            self.getRelationalValue('name', 'event', item.event_id, cb);
        },
        cb => {
            self.getRelationalValue('name', 'queue', item.queue_id, cb);
        }
    ], (err, result) => {
        if (err) return done(err);

        done(null, {
            id: item.id,
            name: result[0],
            tid: item.tid ? item.tid : undefined,
            parent: item.parent ? item.parent : undefined,
            payload: item.payload,
            queue: result[1],
            created: item.created,
            when: item.when || undefined,
            done: item.done || undefined,
            retryCount: item.retryCount || 0,
        });
    });
}

module.exports = DbWrkrPostgreSQL;
