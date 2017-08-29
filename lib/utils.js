'use strict';

// Modules
const _ = require('lodash');
const async = require('async');

/**
 * Convert array of events into query object with SQL text and values
 * 
 * @param {Array} events 
 * @return {Object} Object containing query text and values 
 */
function convertEventsToQuery(events) {
    let text = '';
    let counter = 1;
    let values = [];

    _.each(events, function (event) {
        const eventData = convertEventToQuery(event, counter);
        if (counter !== 1) {
            text += ', ';
        }

        text += eventData.text;
        values = values.concat(eventData.values);
        counter = eventData.counter;
    });

    return {text: text, values: values};
}

/**
 * Convert an event object to a text and
 * 
 * @param {Object} event Event object
 * @param {Integer} counter Starting point for placeholder numbering 
 * @return {Object} Object containing text, values and next counter value for placholder numbering
 */
function convertEventToQuery(event, counter) {
    const payload = event.payload ? JSON.stringify(event.payload) : null;
    const created = event.created ? event.created.toISOString() : null;
    const when = event.when ? event.when.toISOString() : null;
    const parent = event.parent ? event.parent.toString() : null;
    const text = `( (SELECT id FROM "wrkr_events" WHERE name=$${counter}), (SELECT id FROM "wrkr_queues" WHERE name=$${counter + 1}), $${counter + 2}, $${counter + 3}, $${counter + 4}, $${counter + 5}, $${counter + 6}, $${counter + 7})`;
    const values = [event.name, event.queue, event.tid, payload, parent, created, when, event.retryCount];

    return {text: text, values: values, counter: counter + 8};
}

/**
 * Create a query string, and values array based on criteria object
 * 
 * @param {Object} criteria Criteria as key = column, value
 * @param {Integer} counter Starting point for placeholder numbering 
 * @return {Object} Object containing text, values and next counter value for placholder numbering
 */
function createWhereSQL(criteria, counter) {
    if (Object.keys(criteria).length === 0) {
        return {query: '', values: [], counter: counter};
    }

    let valueCounter = counter;
    const values = [];

    const text = _.reduce(criteria, (accumulator, value, key) => {
        let sql = accumulator;
        if (valueCounter !== counter) {
            sql += ' AND ';
        }

        sql += `"${key}" = $${valueCounter}`;
        values[valueCounter - 1] = value;
        valueCounter++;
        return sql;
    }, 'WHERE ');

    return {text: text, values: values, counter: valueCounter};
}

/**
 * Get string value for relational id, or id from string value
 * Since  we have to adhere the document style architecture we have to convert the string values to relational id (and back when returning)
 * 
 * @param {String} relationalType 
 * @param {String} valueType 
 * @param {*} value String or Integer depending on valueTpe 
 * @return {Function} done
 */
function getRelationalValue(relationalType, valueType, value, done) {
    const memorizedIdObject = valueType === 'event' ? 'memorizedEventIds' : 'memorizedQueueIds';
    const memorizedNameObject = valueType === 'event' ? 'memorizedEventNames' : 'memorizedQueueNames';
    const requestedRelationalType = relationalType === 'id' ? memorizedIdObject : memorizedNameObject;

    // Return the relationalType value from memory object if it exists
    if (this[requestedRelationalType] && this[requestedRelationalType].has(value)) {
        return done(null, this[requestedRelationalType].get(value));
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
                this[memorizedIdObject].set(value, relationalTypeValue);
                this[memorizedNameObject].set(relationalTypeValue, value);
            } else {
                this[memorizedNameObject].set(value, relationalTypeValue);
                this[memorizedIdObject].set(relationalTypeValue, value);
            }

            // Return the value, either id or name
            return done(null, relationalTypeValue);
        }

        done('noRecordsFound');
    });
}

/**
 * Get the id for a stirng value if it exists, create record if it down not exist
 * 
 * @param {String} valueType 'event' or 'queue', provide the valueType you require
 * @param {String} 
 * @param {Function} done Callback function
 */
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

/**
 * Map the criteria to relational schema
 * 
 * @param {Object} criteria 
 * @param {Function} done 
 */
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
 * Map (all) fields to the expected values
 * 
 * @param {Object} item 
 * @return {Object} Mapped item
 */
function fieldMapper(item, done) {
    async.parallel([
        this.getRelationalValue('name', 'event', item.event_id),
        this.getRelationalValue('name', 'queue', item.queue_id)
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

module.exports = {
    convertEventsToQuery: convertEventsToQuery,
    createWhereSQL: createWhereSQL,
    getRelationalValue: getRelationalValue,
    getOrInsertIdValue: getOrInsertIdValue,
    mapCriteria: mapCriteria,
    fieldMapper: fieldMapper
};