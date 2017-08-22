'use strict';

// Modules
const _ = require('lodash');

/**
 * Map (all) fields to the correct values
 * 
 * @param {Object} item 
 * @return {Object} Mapped item
 */
function fieldMapper(item) {
    return {
        id: item.id,
        name: item.name,
        tid: item.tid ? item.tid : undefined,
        parent: item.parent ? item.parent : undefined,
        payload: item.payload,
        queue: item.queue,
        created: item.created,
        when: item.when || undefined,
        done: item.done || undefined,
        retryCount: item.retryCount || 0,
    };
}

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
function convertEventToQuery(event, counter = 1) {
    const payload = event.payload ? JSON.stringify(event.payload) : null;
    const created = event.created ? event.created.toISOString() : null;
    const when = event.when ? event.when.toISOString() : null;
    const parent = event.parent ? event.parent.toString() : null;
    const text = `($${counter}, $${counter + 1}, $${counter + 2}, $${counter + 3}, $${counter + 4}, $${counter + 5}, $${counter + 6}, $${counter + 7})`;
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
function createWhereSQL(criteria, counter = 1) {
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

module.exports = {
    fieldMapper: fieldMapper,
    convertEventsToQuery: convertEventsToQuery,
    convertEventToQuery: convertEventToQuery,
    createWhereSQL: createWhereSQL
};