var _ = require('underscore');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);
var expect = require('chai').expect;
var util = require('util');

var test_utils = require('./elastic-test-utils');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var points = require('./apache-sample');

// Register the adapter
require('./elastic-test-utils');

var modes = test_utils.modes;

describe('elastic source limits', function() {
    this.timeout(30000);

    modes.forEach(function(type) {
        describe(type, function() {
            after(function() {
                return test_utils.clear_data(type);
            });

            before(function() {
                return test_utils.write(points, type)
                .then(function() {
                    return test_utils.verify_import(points, type);
                });
            });

            it('executes multiple fetches', function() {
                var start = '2014-09-17T14:13:47.000Z';
                var end = '2014-09-17T14:14:32.000Z';
                return test_utils.read(start, end, type)
                .then(function(result) {
                    var expected = points.filter(function(pt) {
                        return pt.time >= start && pt.time < end;
                    });

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                });
            });

            it('errors if you try to read too many simultaneous points', function() {
                var extra = '-fetch_size 2 -deep_paging_limit 3';
                return test_utils.read_all(type, extra)
                .then(function(result) {
                    expect(result.errors).deep.equal([ 'Cannot fetch more than 3 points with the same timestamp' ]);
                });
            });

            it('enforces head across multiple fetches', function() {
                var extra = '-fetch_size 2 | head 3';
                return test_utils.read_all(type, extra)
                .then(function(result) {
                    var expected = points.slice(0, 3);
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                    expect(result.prog.graph.es_opts.limit).equal(3);
                });
            });
        });
    });
});
