var Promise = require('bluebird');
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);
var util = require('util');

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var errors = require('./es-errors');
var utils = require('./utils');

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
        type: '', // aws-es demands a type, this means all types
        body: body
    })
    .then(function(result) {
        if (Array.isArray(result)) {
            result = result[0];
        }

        if (result.error) {
            throw new Error(result.error);
        }

        return result;
    })
    .catch(function(err) {
        var categorized = errors.categorize_error(err);
        if (categorized instanceof errors.MissingIndex) {
            return {
                hits: {
                    total: 0,
                    hits: []
                }
            };
        } else if (categorized instanceof errors.WindowOverflow) {
            var error_string = util.format('Tried to read more than %d points with the same timestamp, increase the max_result_window setting on the relevant indices to read more (performance may vary)', categorized.limit);
            throw new Error(error_string);
        } else if (categorized) {
            throw categorized;
        } else {
            throw new Error(err.message);
        }
    });
}

function get_indices(from, to, prefix) {
    var times = getTimeIndices(from, to);
    return times.map(function(time) {
        return prefix + time;
    }).join(',');
}

module.exports = {
    get_indices: get_indices,
    search: search
};
