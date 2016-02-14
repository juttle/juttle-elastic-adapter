var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var current_week_number = require('current-week-number');

var errors = require('./es-errors');
var utils = require('./utils');

// ES will blow up if you send it a URL of over 4096 characters
// this determines the maximum length of a string of indices we'll
// send, leaving some space for the rest of the URL
var SAFE_INDICES_LENGTH = 3600;

var logger = require('juttle/lib/logger').getLogger('elastic-read');

function getTimeIndices(from, to, interval) {
    if (interval === 'none') { return ['']; }
    if (from === null) { return ['*']; }

    to = to || new JuttleMoment();
    var unit = JuttleMoment.duration(1, interval);
    var current = _getStart();
    var max = to.quantize(unit);

    function _getStart() {
        if (interval === 'week') { return from; }

        return from.quantize(unit);
    }

    function _shouldAddNextDate() {
        if (interval === 'week') {
            var end_week = current_week_number(to.valueOf());

            return current_week_number(current.valueOf()) <= end_week;
        }

        return current.lte(max);
    }

    var strings = [];

    while (_shouldAddNextDate()) {
        var date = new Date(current.valueOf());

        strings.push(utils.index_date(date, interval));

        current = JuttleMoment.add(current, unit);
    }

    var total_length = strings.reduce(function(memo, str) {
        return memo + str.length;
    }, 0);

    if (total_length > SAFE_INDICES_LENGTH) {
        return ['*'];
    }

    return strings;
}

function search(client, indices, type, body) {
    var search_options = {
        index: indices,
        ignoreUnavailable: true,
        type: type,
        body: body
    };

    logger.debug('search options', JSON.stringify(search_options, null, 2));

    return client.searchAsync(search_options)
    .then(function(result) {
        logger.debug('search result', JSON.stringify(result, null, 2));

        if (Array.isArray(result)) {
            result = result[0];
        }

        if (result.error) {
            throw new Error(result.error);
        }

        return result;
    })
    .catch(function(err) {
        logger.debug('search error', err);
        var categorized = errors.categorize_error(err);
        if (categorized instanceof errors.MissingIndex || categorized instanceof errors.NewIndex) {
            return {
                hits: {
                    total: 0,
                    hits: []
                }
            };
        } else if (categorized) {
            throw categorized;
        } else {
            throw new Error(err.message);
        }
    });
}

function get_indices(from, to, index, interval) {
    var times = getTimeIndices(from, to, interval);
    return times.map(function(time) {
        return index + time;
    }).join(',');
}

module.exports = {
    get_indices: get_indices,
    search: search
};
