var Promise = require('bluebird');
var _ = require('underscore');

var Elasticsearch = require('elasticsearch');
var AmazonElasticsearchClient = require('aws-es2');
var query = require('./query');
var aggregation = require('./aggregation');
var utils = require('./utils');

var logger = require('juttle/lib/logger').getLogger('elastic');

var instances;
var already_created_indices = {};
var index_creation_promises = {};
var DYNAMIC_MAPPING_SETTINGS = require('./dynamic-mapping-settings');

function init(config) {
    logger.debug('initializing:', JSON.stringify(config));
    if (!Array.isArray(config)) { config = [config]; }

    validate_config(config);

    utils.init(config);

    instances = config.map(function(entry) {
        var client;
        if (entry.aws) {
            client = new AmazonElasticsearchClient({
                accessKeyId: entry.access_key || process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: entry.secret_key || process.env.AWS_SECRET_ACCESS_KEY,
                service: 'es',
                region: entry.region,
                host: entry.endpoint
            });
        } else {
            var contact_point = entry.address + ':' + entry.port;

            client = new Elasticsearch.Client({
                host: contact_point
            });
        }

        Promise.promisifyAll(client);
        // XXX https://github.com/juttle/juttle-elastic-adapter/issues/96
        Promise.promisifyAll(client.indices || {});

        return {
            client: client,
            id: entry.id
        };
    });

    query.init(config);
    aggregation.init(config);
}

function validate_config(config) {
    if (config.length === 0) {
        throw new Error('Elastic requires at least 1 address and port');
    }

    config.forEach(function(entry) {
        if (entry.aws) {
            if (!entry.region || !entry.endpoint) {
                throw new Error('AWS requires region and endpoint');
            }
        } else {
            if (!entry.address || !entry.port) {
                throw new Error('Elastic requires address and port');
            }
        }
    });
}

function _instance_for_id(id) {
    if (id === undefined) { return instances[0]; }

    var instance = _.findWhere(instances, {id: id});
    if (!instance) {
        throw new Error('invalid id: ' + id);
    }

    return instance;
}

function client_for_id(id) {
    var instance = _instance_for_id(id);
    return instance.client;
}

function fetcher(id, filter, query_start, query_end, options) {
    var client = client_for_id(id);
    if (options.aggregations) {
        return query.aggregation_fetcher(client, filter, query_start, query_end, options);
    } else {
        return query.fetcher(client, filter, query_start, query_end, options);
    }
}

function mapping_for_id(id, index, type) {
    var instance = _instance_for_id(id);
    return _get_mapping(instance.client, index, type)
        .catch(function(err) {
            logger.warn('error getting ES mapping', err);
            return {};
        });
}

function _get_mapping(client, index, type) {
    var options = {index: index || '*', type: type || ''};
    return client.indices.getMappingAsync(options)
        .spread((response, statusCode) => {
            return response;
        });
}

function create_index_if_not_exists_with_type(client, index, type) {
    if (already_created_indices[index]) { return Promise.resolve(); }

    return _check_index_exists(client, index)
        .then((exists) => {
            if (!exists) {
                // limit to 1 concurrent creation request
                index_creation_promises[index] = index_creation_promises[index] ||
                    _create_index(client, index, type).finally(function() {
                        index_creation_promises[index] = null;
                    });
                return index_creation_promises[index];
            }
        })
        .then(() => {
            already_created_indices[index] = true;
        });
}

function _check_index_exists(client, index) {
    var options = {index: index};
    return client.indices.existsAsync(options)
        .spread((exists, statusCode) => {
            return exists;
        });
}

function _create_index(client, index, type) {
    var options = {
        index: index,
        body: {
            mappings: {}
        }
    };
    options.body.mappings[type] = DYNAMIC_MAPPING_SETTINGS;
    return client.indices.createAsync(options);
}

// for tests
function clear_already_created_indices() {
    already_created_indices = {};
}

module.exports = {
    init: init,
    validate_config: validate_config,
    client_for_id: client_for_id,
    mapping_for_id: mapping_for_id,
    create_index_if_not_exists_with_type: create_index_if_not_exists_with_type,
    clear_already_created_indices: clear_already_created_indices,
    fetcher: fetcher
};
