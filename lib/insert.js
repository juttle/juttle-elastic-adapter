var _ = require('underscore');
var util = require('util');
var Promise = require('bluebird');

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var utils = require('./utils');

var ES_MAX_FIELD_LENGTH = 32766;

function normalize_timestamp(ts) {
    var parsed = (typeof ts === 'string') ? Date.parse(ts) : ts;

    if (typeof parsed !== 'number' || parsed !== parsed) { // speedy hack for isNaN(ts)
        throw new Error('invalid timestamp: ' + ts);
    }

    return new Date(parsed);
}

function validate_event(event) {
    if (!event.hasOwnProperty('time')) {
        throw new Error('missing time');
    }

    if (event.time instanceof JuttleMoment) {
        event.time = event.time.toJSON();
    }

    for (var key in event) {
        var value = event[key];
        if (_.isObject(value)) {
            throw new Error('invalid event - value is an object or array ' + key + ' : ' + value);
        } else if (value && typeof value === 'string' && value.length > ES_MAX_FIELD_LENGTH) {
            var too_big_suffix = util.format('** FIELD TRUNCATED BY JUT (max length = %d)** ', ES_MAX_FIELD_LENGTH);
            event[key] = value.substring(0, ES_MAX_FIELD_LENGTH - too_big_suffix.length) + too_big_suffix;
        }
    }
}

function EventsInserter(client, index, type, interval, timeField) {
    this.client = client;
    this.events = [];
    this.index = index;
    this.type = type;
    this.interval = interval;
    this.timeField = timeField;
}

EventsInserter.prototype.push = function(event) {
    validate_event(event);
    var ts = normalize_timestamp(event.time);
    event[this.timeField] = ts.toISOString();

    this.events.push({
        index: {
            _index: utils.index_name(ts, this.index, this.interval),
            _type: this.type
        }
    });

    this.events.push(event);
};

EventsInserter.prototype.end = function() {
    var self = this;
    if (this.events.length === 0) {
        return Promise.resolve();
    }

    return this.client.bulkAsync({
        index: '', // aws-es demands index/type which is totally meaningless here
        type: '',
        body: this.events
    });
};

module.exports = EventsInserter;
