var Promise = require('bluebird');
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var _ = require('underscore');
var Elasticsearch = require('elasticsearch');
var AmazonElasticsearchClient = require('aws-es');
var uuid = require('uuid');
var util = require('util');

var Juttle = require('juttle/lib/runtime').Juttle;
var Elastic = require('../lib');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');

var check_juttle = juttle_test_utils.check_juttle;

var AWS_HOST = 'search-dave-6jy2cskdfaye4ji6gfa6x375ve.us-west-2.es.amazonaws.com';
var AWS_REGION = 'us-west-2';
var TEST_RUN_ID = uuid.v4().substring(0, 8);

var LOCAL = 'local';
var AWS = 'aws';

var mode = process.env.TESTMODE;
var modes;
if (mode === 'all') {
    modes = [LOCAL, AWS];
} else if (mode === 'aws') {
    modes = [AWS];
} else {
    modes = [LOCAL];
}

var local_client = Promise.promisifyAll(new Elasticsearch.Client({
    host: 'localhost:9200'
}));

var test_index = 'my_index';
var has_index_id = 'has_default_index';

var config = [{
    id: LOCAL,
    address: 'localhost',
    port: 9200
},
{
    id: 'b',
    address: 'localhost',
    port: 9999 // b's config is botched so we can get errors reading from it
},
{
    id: has_index_id,
    address: 'localhost',
    port: 9200,
    index: test_index
}
];

var aws_client;
if (_.contains(modes, AWS)) {
    aws_client = Promise.promisifyAll(new AmazonElasticsearchClient({
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        service: 'es',
        region: AWS_REGION,
        host: AWS_HOST
    }));

    config.push({
        id: AWS,
        type: 'aws',
        endpoint: AWS_HOST,
        region: AWS_REGION
    });
}

var adapter = Elastic(config, Juttle);

Juttle.adapters.register(adapter.name, adapter);

function write(points, id, index, interval, extra) {
    interval = interval || 'none';
    index = index || TEST_RUN_ID;
    extra = extra || '';
    var program = 'emit -points %s | write elastic -id "%s" -index "%s" -indexInterval "%s" %s';
    var write_program = util.format(program, JSON.stringify(points), id, index, interval, extra);

    return check_juttle({
        program: write_program
    });
}

function read(start, end, id, extra, index, interval) {
    interval = interval || 'none';
    index = index || TEST_RUN_ID;
    var program = 'read elastic -from :%s: -to :%s: -id "%s" -index "%s" -indexInterval "%s" %s';
    var read_program = util.format(program, start, end, id, index, interval, extra || '');

    return check_juttle({
        program: read_program
    });
}

function read_all(type, extra, index, interval) {
    return read('10 years ago', 'now', type, extra, index, interval);
}

function clear_data(type, indexes) {
    indexes = indexes || TEST_RUN_ID + '*';
    if (type === 'aws') {
        return aws_client.deleteAsync({index: indexes});
    } else {
        return local_client.indices.delete({index: indexes});
    }
}

function verify_import(points, type, indexes) {
    var client = type === 'aws' ? aws_client : local_client;
    var request_body = {
        index: indexes || TEST_RUN_ID + '*',
        type: '',
        body: {
            size: 10000
        }
    };

    return retry(function() {
        return client.searchAsync(request_body)
        .then(function(body) {
            if (Array.isArray(body)) {
                body = body[0];
            }
            var received = body.hits.hits.map(function(doc) {
                return doc._source;
            });

            points.forEach(function(point) {
                var expected = _.clone(point);
                if (expected.time) {
                    expected.time = new Date(expected.time).toISOString();
                }
                expect(_.findWhere(received, expected)).exist; // jshint ignore:line
            });
        });
    }, {max_tries: 10});
}

function expect_sorted(array) {
    for (var i = 0; i < array.length - 1; i++) {
        if (array[i].time > array[i+1].time) {
            throw new Error('points not sorted');
        }
    }
}

function check_result_vs_expected_sorting_by(received, expected, field) {
    // we have simultaneous points so time sort is not unique
    // so we make sure the result is sorted but verify the right result
    // with another sort
    expect_sorted(received);
    expect(_.sortBy(received, field)).deep.equal(_.sortBy(expected, field));
}

// only works on linear flowgraphs
function check_optimization(start, end, id, extra, options) {
    options = options || {};
    extra = extra || '';
    var unoptimized_extra = '-optimize false ' + extra;
    return Promise.all([
        read(start, end, id, extra),
        read(start, end, id, unoptimized_extra)
    ])
    .spread(function(optimized, unoptimized) {
        var opt_data = optimized.sinks.table;
        var unopt_data = unoptimized.sinks.table;

        if (options.massage) {
            opt_data = options.massage(opt_data);
            unopt_data = options.massage(unopt_data);
        }

        expect(opt_data).deep.equal(unopt_data);
    });
}

function list_indices() {
    return local_client.indices.getAliases()
        .then(function(result) {
            return Object.keys(result);
        });
}

function search() {
    return local_client.search({
        index: '*',
        size: 10000
    });
}

function expect_to_fail(promise, message) {
    return promise.throw(new Error('should have failed'))
        .catch(function(err) {
            expect(err.message).equal(message);
        });
}

module.exports = {
    read: read,
    read_all: read_all,
    write: write,
    modes: modes,
    check_result_vs_expected_sorting_by: check_result_vs_expected_sorting_by,
    verify_import: verify_import,
    check_optimization: check_optimization,
    clear_data: clear_data,
    list_indices: list_indices,
    test_index: test_index,
    has_index_id: has_index_id,
    test_id: TEST_RUN_ID,
    search: search,
    expect_to_fail: expect_to_fail,
};
