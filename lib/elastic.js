var _ = require('underscore');
var Base = require('extendable-base');
var Url = require('url');

var Inserter = require('./insert');
var query = require('./query');
var aggregation = require('./aggregation');
var common = require('./query-common');

var es_url;

function init(config) {
    es_url = Url.format({
        protocol: 'http',
        hostname: config.address,
        port: config.port
    });

    query.init(config);
    aggregation.init(config);
    common.init(es_url);
}

function get_inserter() {
    return new Inserter(es_url);
}

module.exports = {
    init: init,
    get_inserter: get_inserter,
    fetcher: query.fetcher
};
