var _ = require('underscore');
var current_week_number = require('current-week-number');

var DEFAULT_INDEXES = {};
var DEFAULT_INTERVALS = {};
var DEFAULT_DEFAULT_READ_INDEX = '*';
var DEFAULT_DEFAULT_WRITE_INDEX = 'juttle';
var DEFAULT_DEFAULT_INTERVAL = 'none';

function pointsFromESDocs(hits) {
    return hits.map(function(hit) {
        var point = hit._source;
        var time = new Date(point['@timestamp']).getTime();
        var newPoint = _.omit(point, '@timestamp');
        newPoint.time = time;

        return newPoint;
    });
}

function index_name(timestamp, index, interval) {
    return index + index_date(timestamp, interval);
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
    config.forEach(function(entry) {
        if (entry.hasOwnProperty('id')) {
            if (entry.hasOwnProperty('index')) {
                DEFAULT_INDEXES[entry.id] = entry.index;
            }

            if (entry.hasOwnProperty('indexInterval')) {
                DEFAULT_INTERVALS[entry.id] = entry.indexInterval;
            }
        }
    });
}

var default_read_index_for_id = _default_for_id(DEFAULT_INDEXES, DEFAULT_DEFAULT_READ_INDEX);
var default_write_index_for_id = _default_for_id(DEFAULT_INDEXES, DEFAULT_DEFAULT_WRITE_INDEX);
var default_interval_for_id = _default_for_id(DEFAULT_INTERVALS, DEFAULT_DEFAULT_INTERVAL);

function _default_for_id(defaults_object, default_default) {
    return function(id) {
        if (defaults_object.hasOwnProperty(id)) {
            return defaults_object[id];
        }

        return default_default;
    };
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
    default_read_index_for_id: default_read_index_for_id,
    default_write_index_for_id: default_write_index_for_id,
    default_interval_for_id: default_interval_for_id,
    ensure_valid_interval: ensure_valid_interval,
};
