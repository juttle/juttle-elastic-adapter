var _ = require('underscore');
var util = require('util');
var request = require('request');
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

function EventsInserter(es_url) {
    this.req = null;
    this.bulkstr = '';

    this.http_opts = {
        url: es_url + '/_bulk',
        method: 'POST'
    };
}

EventsInserter.prototype.start_request = function() {
    var self = this;
    this.req = request(this.http_opts, function() {});
    this.count = 0;
    this.wait_for_request_promise = new Promise(function(resolve, reject) {
        self.req.on('complete', function(res) {
            try {
                self._validateResult(res);
                resolve();
            } catch(err) {
                reject(err);
            }
        });
        self.req.on('error', function(err) {
            reject(err);
        });
    });
};

EventsInserter.prototype.push = function(event) {
    validate_event(event);
    var ts = normalize_timestamp(event.time);

    var cmd = { index: {
        _index: utils.index_name(ts),
        _type:  'event'
    } };

    event['@timestamp'] = ts.toISOString();

    if (this.req === null) {
        this.start_request();
    }

    // measurement shows a significant cpu usage decrease by calling req.end
    // with a single string instead of req.write for individual portions of
    // the elasticseach bulk insert request.
    this.bulkstr += JSON.stringify(cmd) + '\n' + JSON.stringify(event) + '\n';

    this.count++;
};

EventsInserter.prototype.end = function() {
    if (this.req === null) {
        return Promise.resolve();
    }
    this.req.end(this.bulkstr);
    return this.wait_for_request_promise;
};

EventsInserter.prototype._validateResult = function(res) {
    if (res.statusCode !== 200) {
        throw new Error('elasticsearch bulk request failed with code ' + res.statusCode + ':\n' + res.body);
    } else {
        var ctype = res.headers['content-type'].split(';')[0];
        if (ctype !== 'application/json') {
            throw new Error('elasticsearch bulk reply had unexpected mime type ' + ctype);
        }
    }
};

module.exports = EventsInserter;
