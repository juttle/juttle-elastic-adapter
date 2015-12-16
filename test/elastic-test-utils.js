var Promise = require('bluebird');
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var _ = require('underscore');
var Elasticsearch = require('elasticsearch');
var AmazonElasticsearchClient = require('aws-es');

var Juttle = require('juttle/lib/runtime').Juttle;
var Elastic = require('../lib');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;

var AWS_HOST = 'search-dave-6jy2cskdfaye4ji6gfa6x375ve.us-west-2.es.amazonaws.com';
var AWS_REGION = 'us-west-2';

var LOCAL = 'local';
var AWS = 'aws';

var local_client = Promise.promisifyAll(new Elasticsearch.Client({
    host: 'localhost:9200'
}));

var aws_client = Promise.promisifyAll(new AmazonElasticsearchClient({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    service: 'es',
    region: AWS_REGION,
    host: AWS_HOST
}));

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
    id: AWS,
    type: 'aws',
    endpoint: AWS_HOST,
    region: AWS_REGION
}];

var adapter = Elastic(config, Juttle);

Juttle.adapters.register(adapter.name, adapter);

function clear_logstash_data(type) {
    if (type === 'aws') {
        return aws_client.deleteAsync({index: 'logstash-*'});
    } else {
        return local_client.indices.delete({index: 'logstash-*'});
    }
}

function verify_import(points, type) {
    var client = type === 'aws' ? aws_client : local_client;
    return retry(function() {
        return client.searchAsync({
            index: 'logstash-*',
            type: '',
            body: {
                size: 10000
            }
        })
        .then(function(body) {
            if (Array.isArray(body)) {
                body = body[0];
            }
            var received = body.hits.hits.map(function(doc) {
                return doc._source;
            });

            points.forEach(function(point) {
                var expected = _.clone(point);
                expected.time = new Date(expected.time).toISOString();
                expect(_.findWhere(received, expected)).exist;
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
function check_optimization(juttle, options) {
    options = options || {};
    var read_elastic_length = 'read elastic'.length;
    var unoptimized_juttle = 'read elastic -optimize false ' + juttle.substring(read_elastic_length);
    return Promise.map([juttle, unoptimized_juttle], function(program) {
        return check_juttle({
            program: program
        });
    })
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

module.exports = {
    default_types: [LOCAL, AWS],
    check_result_vs_expected_sorting_by: check_result_vs_expected_sorting_by,
    verify_import: verify_import,
    check_optimization: check_optimization,
    clear_logstash_data: clear_logstash_data
};
