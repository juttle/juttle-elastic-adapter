var _ = require('underscore');

var DEFAULT_PREFIXES;
var DEFAULT_DEFAULT_PREFIX = 'logstash-';

function pointsFromESDocs(hits) {
    return hits.map(function(hit) {
        var point = hit._source;
        var time = new Date(point['@timestamp']).getTime();
        var newPoint = _.omit(point, '@timestamp');
        newPoint.time = time;

        return newPoint;
    });
}

function index_name(timestamp, prefix) {
    return prefix + index_date(timestamp);
}

// YYYY.MM.dd
function index_date(timestamp) {
    var year = timestamp.getUTCFullYear();
    var month = (timestamp.getUTCMonth() + 1).toString();
    var day = timestamp.getUTCDate().toString();

    function double_digitize(str) {
        return (str.length === 1) ? ('0' + str) : str;
    }

    return [year, double_digitize(month), double_digitize(day)].join('.');
}

function init(config) {
    DEFAULT_PREFIXES = {};
    config.forEach(function(entry) {
        if (entry.hasOwnProperty('id') && entry.hasOwnProperty('index_prefix')) {
            DEFAULT_PREFIXES[entry.id] = entry.index_prefix;
        }
    });
}

function default_prefix_for_id(id) {
    if (DEFAULT_PREFIXES.hasOwnProperty(id)) {
        return DEFAULT_PREFIXES[id];
    }

    return DEFAULT_DEFAULT_PREFIX;
}

module.exports = {
    init: init,
    index_name: index_name,
    pointsFromESDocs: pointsFromESDocs,
    default_prefix_for_id: default_prefix_for_id,
};
