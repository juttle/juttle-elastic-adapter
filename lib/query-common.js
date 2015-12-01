var Promise = require('bluebird');
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var errors = require('./es-errors');

var SEARCH_SUFFIX = '/_search';

// Ouch, ES uses a fixed 4KB buffer for each line of HTTP input.
// The very first line contains a method, a path, and the string
// HTTP/1.1 (8 character).  The longest method name we use
// is OPTIONS (7 characters).  Then there are spaces separating
// those components.  So up to 15 bytes are use for stuff that
// isn't part of the path leaving:
var MAX_ES_URL_LENGTH = 4095 - 15;

function getTimeIndices(from, to) {
    var maxDays = JuttleMoment.duration(14, 'day');

    to = to || new JuttleMoment();

    if (from === null || JuttleMoment.add(from, maxDays).lt(to)) {
        return ['*'];
    }

    var day = JuttleMoment.duration(1, 'day');

    var strings = [];

    var current = from.quantize(JuttleMoment.duration(1, 'day'));
    var max = to.quantize(JuttleMoment.duration(1, 'day'));

    while (current.lte(max)) {
        // Push only the first part of the ISO string (e.g. 2014-01-01).
        strings.push(current.valueOf().substr(0, 10).replace(/-/g, '.'));

        current = JuttleMoment.add(current, day);
    }

    return strings;
}

function execute(url, body, method, options) {
    options = options || {};
    return request.async({
        url: url,
        method: method || 'POST',
        json: body
    })
    .cancellable()
    .spread(function(response, body) {
        var err;
        if (response.statusCode !== 200 && response.statusCode !== 201) {
            if (response.statusCode === 500) {
                err = errors.categorize_error(body.error);
                if (err && err instanceof errors.MissingField) {
                    throw err;
                }
            }

            // ugh, if we query a brand new index, we occassionally
            // get this error.  just treat it as empty results.
            if (response.statusCode === 503 && !options.fatal_503) {
                err = errors.categorize_error(body.error);
                if (err && err instanceof errors.AllFailed) {
                    return {
                        hits: {
                            total: 0,
                            hits: []
                        }
                    };
                }
            }

            err = new Error('Received status code ' + response.statusCode + ' from ElasticSearch');
            err.status = response.statusCode;
            throw err;
        }

        // oh dear this is a hack
        if (url.indexOf('_search') !== -1
            && body._shards && body._shards.failures) {
            var IGNORE_KEY = '**ignore**';
            var counts = _.countBy(body._shards.failures, function(failure) {
                var err = errors.categorize_error(failure.reason);

                if (err && err instanceof errors.MissingField) {
                    // XXX see PROD-7325 for discussion about why we skip these
                    return IGNORE_KEY;
                }
                else if (err && err instanceof errors.ElasticsearchException) {
                    return err.exception;
                } else if (err && err instanceof errors.ContextMissing) {
                    return IGNORE_KEY;
                } else {
                    return failure.reason;
                }

            });

            var total = 0;
            _.each(counts, function(n, what) {
                if (what !== IGNORE_KEY) {
                    total += n;
                }
            });
            if (total > 0) {
                throw new Error('Elasticsearch exception(s): ' + JSON.stringify(counts));
            }
        }

        return body;
    });
}

function build_logstash_url(es_url, from, to) {
    var time_indices = getTimeIndices(from, to);
    var indices = time_indices.map(function(time) {
        return 'logstash-' + time;
    }).join(',');

    var url = es_url + indices + SEARCH_SUFFIX;


    if (url.length > MAX_ES_URL_LENGTH) {
        url = es_url + '*' + SEARCH_SUFFIX;
    }

    return url;
}

module.exports = {
    build_logstash_url: build_logstash_url,
    execute: execute
};
