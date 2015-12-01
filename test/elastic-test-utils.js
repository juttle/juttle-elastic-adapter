var Promise = require('bluebird');
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var Juttle = require('juttle/lib/runtime').Juttle;
var Elastic = require('../lib');

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
    var url = 'http://localhost:9200/_search';
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
    expect(_.sortBy(received, 'bytes')).deep.equal(_.sortBy(expected, field));
}

module.exports = {
    check_result_vs_expected_sorting_by: check_result_vs_expected_sorting_by,
    verify_import: verify_import,
    clear_logstash_data: clear_logstash_data
};
