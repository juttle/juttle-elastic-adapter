var _ = require('underscore');
var util = require('util');
var Promise = require('bluebird');

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var utils = require('./utils');

function normalize_timestamp(ts) {
    var parsed = (typeof ts === 'string') ? Date.parse(ts) : ts;
    if (ts instanceof JuttleMoment) {
        return new Date(ts.valueOf());
    }

    if (typeof parsed !== 'number' || parsed !== parsed) { // speedy hack for isNaN(ts)
        throw new Error('invalid timestamp: ' + ts);
    }

    return new Date(parsed);
}

function validate_event(event) {
    if (!event.hasOwnProperty('time')) {
        throw new Error('missing time');
    }
}

function EventsInserter(client, options) {
    this.client = client;
    this.events = [];
    this.index = options.index;
    this.type = options.type;
    this.interval = options.interval;
    this.timeField = options.timeField;
    this.idField = options.idField;
}

EventsInserter.prototype.push = function(event) {
    validate_event(event);
    var ts = normalize_timestamp(event.time);
    event[this.timeField] = ts.toISOString();
    var _id;
    if (this.idField && event[this.idField]) {
        _id = event[this.idField];
        event[this.idField] = undefined;
    }

    this.events.push({
        index: {
            _id: _id,
            _index: utils.index_name(ts, this.index, this.interval),
            _type: this.type
        }
    });

    this.events.push(event);
};

EventsInserter.prototype.end = function() {
    var errors = [];
    if (this.events.length === 0) {
        return Promise.resolve(errors);
    }

    return this.client.bulkAsync({
        index: '', // aws-es demands index/type which is totally meaningless here
        type: '',
        body: this.events
    })
    .then(function(response) {
        var result;

        // aws-es returns an object while official ES client returns an array
        if (Array.isArray(response)) {
            result = response[0];
        } else {
            result = response;
        }

        if (result.errors) {
            result.items.forEach(function(item) {
                var info = item.create || item.index;
                if (info.error) {
                    errors.push(info.error.reason);
                }
            });
        }

        return errors;
    });
};

module.exports = EventsInserter;
