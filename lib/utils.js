var _ = require('underscore');
var current_week_number = require('current-week-number');

var DEFAULT_PREFIXES, DEFAULT_INTERVALS;
var DEFAULT_DEFAULT_PREFIX = 'logstash-';
var DEFAULT_DEFAULT_INTERVAL = 'day';

function pointsFromESDocs(hits) {
    return hits.map(function(hit) {
        var point = hit._source;
        var time = new Date(point['@timestamp']).getTime();
        var newPoint = _.omit(point, '@timestamp');
        newPoint.time = time;

        return newPoint;
    });
}

function index_name(timestamp, prefix, interval) {
    return prefix + index_date(timestamp, interval);
}

// YYYY.MM.dd
function index_date(timestamp, interval) {
    function double_digitize(str) {
        return (str.length === 1) ? ('0' + str) : str;
    }

    var year = timestamp.getUTCFullYear();
    var month = (timestamp.getUTCMonth() + 1).toString();

    switch (interval) {
        case 'day':
            var day = timestamp.getUTCDate().toString();
            return [year, double_digitize(month), double_digitize(day)].join('.');

        case 'week':
            var week = current_week_number(timestamp);
            return [year, double_digitize(week)].join('.');

        case 'month':
            return [year, double_digitize(month)].join('.');

        case 'year':
            return year;

        case 'none':
            return '';

        default:
            throw new Error('invalid interval: ' + interval + '; accepted intervals are "day", "week", "month" "year", and "none"');
    }
}

function init(config) {
    DEFAULT_PREFIXES = {};
    DEFAULT_INTERVALS = {};
    config.forEach(function(entry) {
        if (entry.hasOwnProperty('id')) {
            if (entry.hasOwnProperty('index_prefix')) {
                DEFAULT_PREFIXES[entry.id] = entry.index_prefix;
            }

            if (entry.hasOwnProperty('index_interval')) {
                DEFAULT_INTERVALS[entry.id] = entry.index_interval;
            }
        }
    });
}

function default_prefix_for_id(id) {
    if (DEFAULT_PREFIXES.hasOwnProperty(id)) {
        return DEFAULT_PREFIXES[id];
    }

    return DEFAULT_DEFAULT_PREFIX;
}

function default_interval_for_id(id) {
    if (DEFAULT_INTERVALS.hasOwnProperty(id)) {
        return DEFAULT_INTERVALS[id];
    }

    return DEFAULT_DEFAULT_INTERVAL;
}

function ensure_valid_interval(interval) {
    var valid_intervals = ['day', 'week', 'month', 'year', 'none'];
    if (!_.contains(valid_intervals, interval)) {
        throw new Error('invalid interval: ' + interval + '; accepted intervals are "day", "week", "month" "year", and "none"');
    }
}

module.exports = {
    init: init,
    index_name: index_name,
    index_date: index_date,
    pointsFromESDocs: pointsFromESDocs,
    default_prefix_for_id: default_prefix_for_id,
    default_interval_for_id: default_interval_for_id,
    ensure_valid_interval: ensure_valid_interval,
};
