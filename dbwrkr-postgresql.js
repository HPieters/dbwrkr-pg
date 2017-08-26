'use strict';

// Modules
const assert = require('assert');
const debug = require('debug')('dbwrkr:postgresql');
const Pool = require('pg').Pool;
const _ = require('lodash');
const flw = require('flw');
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

    flw.series([
        (c, cb) => {
            this.getId('event', eventName, c._store('eventId', cb));
        },
        (c, cb) => {
            this.getId('queue', queueName , c._store('queueId', cb));
        },
        (c, cb) => {
            const subscribeQuery = 'INSERT INTO "wrkr_subscriptions" ("event_id", "queue_id") VALUES ($1, $2);';
            this.pool.query(subscribeQuery, [c.eventId, c.queueId], cb);
        }
    ], done);
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

    flw.series([
        (c, cb) => {
            this.getId('event', eventName, c._store('eventId', cb));
        },
        (c, cb) => {
            this.getId('queue', queueName, c._store('queueId', cb));
        },
        (c, cb) => {
            const unsubscribeQuery = 'DELETE FROM "wrkr_subscriptions" WHERE "event_id" = $1 AND "queue_id" = $2';
            this.pool.query(unsubscribeQuery, [c.eventId, c.queueId], cb);
        }
    ], done);
};

/**
 * Subscriptions
 * 
 * @param {String} eventName
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.subscriptions = function subscriptions(eventName, done) {
    debug('Subscriptions ', {event: eventName});

    async.waterfall([
        (cb) => {
            this.getId('event', eventName, cb);
        },
        (eventId, cb) => {
            const subscriptionsQuery = `
                SELECT      su.event_id, qu.name
                FROM        wrkr_subscriptions AS su
                LEFT JOIN   wrkr_queues AS qu ON su.queue_id=qu.id
                WHERE       event_id = $1`;

            this.pool.query(subscriptionsQuery, [eventId], (err, res) => {
                if (err) return cb(err);
                cb(null, res.rows);
            });
        },
        (subscriptions, cb) => {
            if (subscriptions.length === 0) {
                return cb(null, []);
            }

            const queueNames = _.reduce(subscriptions, (subscriptionQueues, subscription) => {
                subscriptionQueues.push(subscription.name);
                return subscriptionQueues;
            }, []);

            return cb(null, queueNames);
        }
    ], done);
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


    async.waterfall([
        cb => {
            this.getId('queue', queue, cb);
        }
    ], (err, queueId) => {
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


    async.waterfall([
        (cb) => {
            if (!criteria.name) {
                return cb();
            }

            this.getId('event', criteria.name, (err, eventId) => {
                if (err) return cb(err);
                criteria.event_id = eventId;
                delete criteria.name;
                cb();
            });
        },
        (cb) => {
            if (!criteria.queue) {
                return cb();
            }

            this.getId('queue', criteria.queue, (err, queueId) => {
                if (err) return cb(err);

                criteria.queue_id = queueId;
                delete criteria.queue;
                cb();
            });
        }
    ], err => {
        if (err) return done(err);

        const whereSQL = utils.createWhereSQL(criteria, 1);

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

    async.waterfall([
        (cb) => {
            if (!criteria.name) {
                return cb();
            }

            this.getId('event', criteria.name, (err, eventId) => {
                if (err) return cb(err);
                criteria.event_id = eventId;
                delete criteria.name;
                cb();
            });
        },
        (cb) => {
            if (!criteria.queue) {
                return cb();
            }

            this.getId('queue', criteria.queue, (err, queueId) => {
                if (err) return cb(err);

                criteria.queue_id = queueId;
                delete criteria.queue;
                cb();
            });
        }
    ], err => {
        if (err) return done(err);

        const whereSQL = utils.createWhereSQL(criteria, 1);
        const removeQuery = `
            DELETE FROM "wrkr_items"
            ${whereSQL.text}
        `;

        this.pool.query(removeQuery, whereSQL.values, err => {
            if (err) return done(err);

            debug('Removed', criteria);
            done(null);
        });
    });
};

/**
 * 
 * @param {*} queueName 
 */
DbWrkrPostgreSQL.prototype.getId = function getId(type, name, done) {
    debug('GetId -', type, name);
    const memorizedIdObject = type === 'event' ? 'memorizedEventIds' : 'memorizedQueueIds';
    const memorizedNameObject = type === 'event' ? 'memorizedEventNames' : 'memorizedQueueNames';
    const self = this;

    if (this[memorizedIdObject] && this[memorizedIdObject][name]) {
        return done(null, this[memorizedIdObject][name]);
    }

    function getId(queries, name, cb) {
        const query = queries.shift();

        self.pool.query(query, [name], (err, result) => {
            if (err) return cb(err);

            result = result.rows;

            if (result && result.length > 0) {
                const eventId = result[0].id;
                self[memorizedIdObject][name] = eventId;
                self[memorizedNameObject][eventId] = name;
                return cb(null, eventId);
            }

            if (queries.length > 0) {
                return getId(queries, name, cb);
            }

            cb('noIdFound');
        });
    }

    const queueQueries = ['SELECT id FROM "wrkr_queues" WHERE name = $1 LIMIT 1', 'INSERT INTO "wrkr_queues" ("name") VALUES ($1) RETURNING *'];
    const eventQueries = ['SELECT id FROM "wrkr_events" WHERE name = $1 LIMIT 1', 'INSERT INTO "wrkr_events" ("name") VALUES ($1) RETURNING *'];

    getId(type === 'event' ? eventQueries : queueQueries , name, done);
};

/**
 * 
 */
DbWrkrPostgreSQL.prototype.getName = function getId(type, id, done) {
    debug('GetName -', type, id);

    const memorizedIdObject = type === 'event' ? 'memorizedEventIds' : 'memorizedQueueIds';
    const memorizedNameObject = type === 'event' ? 'memorizedEventNames' : 'memorizedQueueNames';

    if (this[memorizedNameObject] && this[memorizedNameObject][id]) {
        return done(null, this[memorizedNameObject][id]);
    }

    const queueQuery = 'SELECT id FROM "wrkr_queues" WHERE id = $1 LIMIT 1';
    const eventQuery = 'SELECT id FROM "wrkr_events" WHERE id = $1 LIMIT 1';

    this.pool.query(type === 'event' ? eventQuery : queueQuery, [id], (err, result) => {
        if (err) return done(err);

        const resultName  = result.rows[0].name;
        this[memorizedNameObject][id] = resultName;
        this[memorizedIdObject][resultName] = id;
        return done(null, resultName);
    });
};


/**
 * Map (all) fields to the correct values
 * 
 * @param {Object} item 
 * @return {Object} Mapped item
 */
function fieldMapper(self, item, done) {
    async.parallel([
        cb => {
            self.getName('event', item.event_id, cb);
        },
        cb => {
            self.getName('queue', item.queue_id, cb);
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
