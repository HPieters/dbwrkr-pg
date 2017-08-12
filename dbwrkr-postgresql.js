'use strict';

// Modules
const assert = require('assert');
const debug = require('debug')('dbwrkr:postgresql');
const massive = require('massive');
const flw = require('flw');
const _ = require('lodash');

/**
 * DbWrkrPostgreSQL Constructor
 * 
 * @param {Object} opt Options object
 */
function DbWrkrPostgreSQL (opt) {
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

    this.db = null;
    this.tSubscriptions = null;
    this.tQitems = null;
}

/**
 * Connect to database, set internal references and setup tables if they do not exist yet
 * 
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.connect = function connect (done) {
    debug('Connecting to PostgreSQL', this.pgOptions);

    massive(this.pgOptions).then(db => {
        debug('connected to PostgreSQL');
        
        this.db = db;
        this.dbSubscriptions = this.db.wrkr_subscriptions;
        this.dbQitems = this.db.wrkr_items;

        // Most likely scenario first
        if (db.wrkr_subscriptions && db.wrkr_items) {
            return done(null);
        }
        
        require('./lib/setup')(this.db, this.pgOptions.database, done);
    }).catch(done);
};

/**
 * Disconnect from PostgreSQL
 * @TODO Handle disconnect in massive.js
 * 
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.disconnect = function disconnect (done) {
    if (!this.db) return done();
    this.dbSubscriptions = null;
    this.dbQitems = null;
    this.db = null;
    return done();
};

/**
 * Subscribe
 * 
 * @param {String} eventName
 * @param {String} queueName
 * @param {function} callback Callback option
 */
DbWrkrPostgreSQL.prototype.subscribe = function subscribe (eventName, queueName, done) {
    debug('Subscribe ', {event: eventName, queue: queueName});

    var subscribeQuery = `INSERT INTO "wrkr_subscriptions" ("eventName", "queues") VALUES ($1, $2) 
    ON CONFLICT ("eventName") DO UPDATE SET "queues" = array_append("wrkr_subscriptions"."queues", $3) 
    WHERE "wrkr_subscriptions"."eventName" = $1;`;

    this.db.run(subscribeQuery, [eventName, `{${queueName}}`, queueName]).then(_.partial(done, null)).catch(done);
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
    
    var unsubscribeQuery = `UPDATE "wrkr_subscriptions" SET "queues" = array_remove("wrkr_subscriptions"."queues", $1) 
    WHERE "wrkr_subscriptions"."eventName" = $2;`;

    this.db.run(unsubscribeQuery, [queueName, eventName]).then(_.partial(done, null)).catch(done);
};

/**
 * Subscriptions
 * 
 * @param {String} eventName
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.subscriptions = function subscriptions (eventName, done) {
    debug('Subscriptions ', {event: eventName});

    this.dbSubscriptions.findOne({ 'eventName': eventName })
    .then(event => {
        return done(null, event ? event.queues : [])
    })
    .catch(done);
};

/**
 * Publish events
 * @TODO why are id's strings?
 * @param {String} eventName
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.publish = function publish (events, done) {
    const publishEvents = Array.isArray(events) ? events : [events];
    debug('Publish ', publishEvents);

    this.dbQitems.insert(publishEvents).then(results => {
        if (publishEvents.length !== results.length) {
            return done(new Error('insertErrorNotEnoughEvents'));
        }
       
        const createdIds = results.map(o => { return o.id.toString(); });
        debug('Published ', publishEvents.length, createdIds);
        return done(null, createdIds);
    }).catch(done);
};

/**
 * Fetch the next item
 * @TODO use between function (see rethinkDB)
 * @param {String} queue
 * @param {function} done Callback 
 */
DbWrkrPostgreSQL.prototype.fetchNext = function fetchNext (queue, done) {
    debug('FetchNext', queue);

    this.dbQitems.update({
        'queue': queue,
        'when <=': new Date()
    }, {
        'when': null,
        'done': new Date()
    }, {
        'order': 'created',
        'single': true
    })
    .then(result => {
        if (!result) return done(null, undefined);

        debug('fetchNext', result);
        return done(null, fieldMapper(result));
    })
    .catch(done);
};

/**
 * Find items based on the given criteria
 * @TODO handle multiple id's better.
 * @param {Object} criteria
 * @param {function} done Callback 
 */
DbWrkrPostgreSQL.prototype.find = function find (criteria, done) {
    debug('Finding ', criteria);

    // Handle multiple id's
    if (criteria.id && Array.isArray(criteria.id) && criteria.id.length > 1) {
        let ids = criteria.id.join(',');
        
        return this.dbQitems.where(`id in (${ids})`).then(result => {
            debug('Found ', result);
            var records = result.map(fieldMapper);
            return done(null, records);
        })
        .catch(done);
    }

    if (criteria.id && Array.isArray(criteria.id)) {
        criteria.id = _.first(criteria.id);
    }

    if (!criteria.id && !criteria.when) {
        criteria['when >'] = new Date(0, 0, 0);
    }

    this.dbQitems.find(criteria)
    .then(result => {
        debug('Found ', result);
        var records = result.map(fieldMapper);
        return done(null, records);
    })
    .catch(done);
};

/**
 * Remove items 
 * @param {Object} criteria
 * @param {function} done Callback option
 */
DbWrkrPostgreSQL.prototype.remove = function remove (criteria, done) {
    debug('Removing', criteria);

    return this.dbQitems.destroy(criteria)
    .then(_.partial(done, null))
    .catch(done);
};

/**
 * Map (all) fields to the correct values
 * @TODO only needed to convert null to undefined
 * @param {qitem} item the record to fieldmap
 */
function fieldMapper (item) {
    return {
        id: item.id.toString(),
        name: item.name,
        tid: item.tid,
        parent: item.parent || undefined,
        payload: item.payload,
        queue: item.queue,
        created: item.created,
        when: item.when || undefined,
        done: item.done || undefined,
        retryCount: item.retryCount || 0,
    };
}
  
module.exports = DbWrkrPostgreSQL;
