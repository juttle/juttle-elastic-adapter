var Promise = require('bluebird');
var _ = require('underscore');
var Base = require('extendable-base');

var Elasticsearch = require('elasticsearch');
var AmazonElasticsearchClient = require('aws-es');
var Inserter = require('./insert');
var query = require('./query');
var aggregation = require('./aggregation');
var common = require('./query-common');
var utils = require('./utils');

var clients;

function init(config) {
    if (!Array.isArray(config)) { config = [config]; }

    if (config.length === 0) {
        throw new Error('Elastic requires at least 1 address and port');
    }

    utils.init(config);

    clients = config.map(function(entry) {
        var client;
        if (entry.type === 'aws') {
            if (!entry.region || !entry.endpoint) {
                throw new Error('AWS expects region and endpoint');
            }

            client = new AmazonElasticsearchClient({
                accessKeyId: entry.access_key || process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: entry.secret_key || process.env.AWS_SECRET_ACCESS_KEY,
                service: 'es',
                region: entry.region,
                host: entry.endpoint
            });
        } else {
            if (!entry.address || !entry.port) {
                throw new Error('Elastic expects address and port');
            }

            var contact_point = entry.address + ':' + entry.port;

            client = new Elasticsearch.Client({
                host: contact_point
            });
        }

        Promise.promisifyAll(client);

        return {
            client: client,
            id: entry.id
        };
    });

    query.init(config);
    aggregation.init(config);
}

function _client_for_id(id) {
    if (id === undefined) {
        return clients[0].client;
    } else {
        var endpoint = _.findWhere(clients, {id: id});
        if (!endpoint) {
            throw new Error('invalid id: ' + id);
        }

        return endpoint.client;
    }
}

function get_inserter(id, prefix) {
    var client = _client_for_id(id);
    return new Inserter(client, prefix);
}

function fetcher(id, filter, query_start, query_end, options) {
    var client = _client_for_id(id);
    if (options.aggregations) {
        return query.aggregation_fetcher(client, filter, query_start, query_end, options);
    } else {
        return query.fetcher(client, filter, query_start, query_end, options);
    }
}

module.exports = {
    init: init,
    get_inserter: get_inserter,
    fetcher: fetcher
};
