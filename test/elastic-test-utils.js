var Promise = require('bluebird');
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var Juttle = require('juttle/lib/runtime').Juttle;
var Elastic = require('../lib');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;

var config = [{
    id: 'a',
    address: 'localhost',
    port: 9200
},
{
    id: 'b',
    address: 'localhost',
    port: 9999 // b's config is botched so we can get errors reading from it
}];

var backend = Elastic(config, Juttle);

Juttle.backends.register(backend.name, backend);

function clear_logstash_data() {
    return request.async({
        url: 'http://localhost:9200/logstash-*',
        method: 'DELETE'
    });
}

function verify_import(points) {
    var url = 'http://localhost:9200/logstash-*/_search';
    return retry(function() {
        return request.postAsync({
            url: url,
            json: {
                size: 10000
            }
        })
        .spread(function(res, body) {
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
    check_result_vs_expected_sorting_by: check_result_vs_expected_sorting_by,
    verify_import: verify_import,
    check_optimization: check_optimization,
    clear_logstash_data: clear_logstash_data
};
