var _ = require('underscore');

function pointsFromESDocs(hits) {
    return hits.map(function(hit) {
        var point = hit._source;
        var time = new Date(point['@timestamp']).getTime();
        var newPoint = _.omit(point, '@timestamp');
        newPoint.time = time;

        return newPoint;
    });
}

function index_name(timestamp) {
    return 'logstash-' + index_date(timestamp);
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

module.exports = {
    index_name: index_name,
    pointsFromESDocs: pointsFromESDocs,
};
