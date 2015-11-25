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

// Register the backend
require('./elastic-test-utils');

var expected_points = points.map(function(pt) {
    var new_pt = _.clone(pt);
    new_pt.time = new Date(new_pt.time).toISOString();
    return new_pt;
});

describe('elastic source limits', function() {
    this.timeout(30000);

    before(function() {
        return test_utils.clear_logstash_data()
            .then(function() {
                var points_to_write = points.map(function(point) {
                    var point_to_write = _.clone(point);
                    point_to_write.time /= 1000;
                    return point_to_write;
                });
                var program = util.format('emit -points %s | writex elastic', JSON.stringify(points_to_write));
                return check_juttle({
                    program: program
                });
            })
            .then(function() {
                return test_utils.verify_import(points);
            });
    });

    it('executes multiple fetches', function() {
        var start = '2014-09-17T14:13:47.000Z';
        var end = '2014-09-17T14:14:32.000Z';
        var program = util.format('readx elastic -from :%s: -to :%s:', start, end);
        return check_juttle({
            program: program
        })
        .then(function(result) {
            var expected = expected_points.filter(function(pt) {
                return pt.time >= start && pt.time < end;
            });

            test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
        });
    });

    it('errors if you try to read too many simultaneous points', function() {
        var program = 'readx elastic -from :10 years ago: -to :now:';
        return check_juttle({
            program: program
        })
        .then(function(result) {
            expect(result.errors).deep.equal([ 'Cannot fetch more than 3 points with the same timestamp' ]);
        });
    });
});
