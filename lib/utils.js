var _ = require('underscore');
var current_week_number = require('current-week-number');

var DEFAULT_CONFIG = {
    readIndex: '*',
    writeIndex: 'juttle',
    indexInterval: 'none',
    timeField: '@timestamp',
    idField: undefined,
    writeType: 'event',
    readType: ''
};

var CONFIG = {};

function pointsFromESDocs(hits, timeField, idField) {
    return hits.map(function(hit) {
        var point = hit._source;
        var time = point[timeField];
        var newPoint = _.omit(point, timeField);
        newPoint.time = time;

        if (idField) {
            newPoint[idField] = hit._id;
        }

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
            CONFIG[entry.id] = _.defaults(entry, DEFAULT_CONFIG);
            // index and type have different defaults for read and write
            // so CONFIG has readIndex/writeIndex/readType/writeType
            // but a configured override sets both
            if (entry.hasOwnProperty('index')) {
                CONFIG[entry.id].readIndex = CONFIG[entry.id].writeIndex = entry.index;
            }
            if (entry.hasOwnProperty('type')) {
                CONFIG[entry.id].readType = CONFIG[entry.id].writeType = entry.type;
            }
        }
    });
}

function default_config_property_for_id(id, property) {
    if (id && !CONFIG.hasOwnProperty(id)) {
        throw new Error('invalid id: ' + id);
    }

    if (CONFIG[id] && CONFIG[id].hasOwnProperty(property)) {
        return CONFIG[id][property];
    }

    return DEFAULT_CONFIG[property];
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
    default_config_property_for_id: default_config_property_for_id,
    ensure_valid_interval: ensure_valid_interval,
};
