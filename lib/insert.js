var util = require('util');
var Promise = require('bluebird');

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var utils = require('./utils');
var elastic = require('./elastic');
var logger = require('juttle/lib/logger').getLogger('elastic-insert');

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
    this.warn = options.warn;
    this.indices_written = {};
}

function get_and_undefine(pt, key) {
    if (!key) { return; }
    var value = pt[key];
    pt[key] = undefined;

    return value;
}

EventsInserter.prototype.push = function(event) {
    validate_event(event);
    var ts = normalize_timestamp(get_and_undefine(event, 'time'));

    if (event.hasOwnProperty(this.timeField)) {
        var message = util.format('clobbering %s value of %s with %s', this.timeField, JSON.stringify(event), ts.toISOString());
        this.warn(message);
    }

    event[this.timeField] = ts.toISOString();

    var _id = get_and_undefine(event, this.idField);
    var index = utils.index_name(ts, this.index, this.interval);
    this.indices_written[index] = true;

    this.events.push({
        index: {
            _id: _id,
            _index: index,
            _type: this.type
        }
    });

    this.events.push(event);
};

EventsInserter.prototype.end = function() {
    var errors = [];
    logger.debug('inserter.end(), points:', this.events.length);
    if (this.events.length === 0) {
        return Promise.resolve(errors);
    }

    return Promise.map(Object.keys(this.indices_written), (index) => {
        return elastic.create_index_if_not_exists_with_type(this.client, index, this.type);
    })
    .then(() => {
        return this.client.bulkAsync({
            index: '', // aws-es demands index/type which is totally meaningless here
            type: '',
            body: this.events
        });
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
                    errors.push(info.error.reason || info.error);
                }
            });
        }

        return errors;
    });
};

module.exports = EventsInserter;
