var Promise = require('bluebird');
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var errors = require('./es-errors');

var SEARCH_SUFFIX = '/_search';

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

function search(client, indices, body) {
    return client.searchAsync({
        index: indices,
        body: body
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

            // if we try to read from a nonexistent index
            // we get 404 so just return nothing
            if (response.statusCode === 404) {
                return {
                    hits: {
                        total: 0,
                        hits: []
                    }
                };
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

function get_logstash_indices(from, to) {
    var times = getTimeIndices(from, to);
    return times.map(function(time) {
        return 'logstash-' + time;
    }).join(',');
}

module.exports = {
    get_logstash_indices: get_logstash_indices,
    search: search
};
