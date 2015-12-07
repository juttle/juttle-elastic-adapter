var _ = require('underscore');
var Base = require('extendable-base');
var Url = require('url');

var Inserter = require('./insert');
var query = require('./query');
var aggregation = require('./aggregation');
var common = require('./query-common');

var es_urls;

function init(config) {
    if (!Array.isArray(config)) { config = [config]; }

    if (config.length === 0) {
        throw new Error('Elastic requires at least 1 address and port');
    }

    es_urls = config.map(function(entry) {
        if (! entry.address) {
            throw new Error('config requires address');
        }
        var url = Url.format({
            protocol: 'http',
            hostname: entry.address,
            port: entry.port || 9200
        });

        return {
            es_url: url + '/',
            id: entry.id
        };
    });

    query.init(config);
    aggregation.init(config);
}

function _url_from_id(id) {
    if (id === undefined) {
        return es_urls[0].es_url;
    } else {
        var endpoint = _.findWhere(es_urls, {id: id});
        if (!endpoint) {
            throw new Error('invalid id: ' + id);
        }

        return endpoint.es_url;
    }
}

function get_inserter(id) {
    var es_url = _url_from_id(id);
    return new Inserter(es_url);
}

function fetcher(id, filter, query_start, query_end, options) {
    var es_url = _url_from_id(id);
    return query.fetcher(es_url, filter, query_start, query_end, options);
}

module.exports = {
    init: init,
    get_inserter: get_inserter,
    fetcher: fetcher
};
