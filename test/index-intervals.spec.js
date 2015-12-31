var _ = require('underscore');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var util = require('util');

var test_utils = require('./elastic-test-utils');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var points = require('./apache-sample');
var expected_points = points.map(function(pt) {
    var new_pt = _.clone(pt);
    new_pt.time = new Date(new_pt.time).toISOString();
    return new_pt;
});

// Register the adapter
require('./elastic-test-utils');

describe('index intervals', function() {
    this.timeout(300000);
    var points_to_write = points.map(function(point) {
        var point_to_write = _.clone(point);
        point_to_write.time /= 1000;
        return point_to_write;
    });

    afterEach(function() {
        return test_utils.clear_data();
    });

    function check(interval, suffix) {
        return test_utils.write(points_to_write, 'local', interval)
            .then(function() {
                return test_utils.verify_import(expected_points);
            })
            .then(function() {
                return test_utils.list_indices();
            })
            .then(function(indices) {
                var week_indices = indices.filter(function(index) {
                    return index.substring(index.length - suffix.length) === suffix;
                });

                expect(week_indices.length).at.least(1);
                var start = '2014-09-17T14:13:42.000Z';
                var end = '2014-10-17T14:13:42.000Z';
                return test_utils.read(start, end, 'local', '', interval);
            })
            .then(function(result) {
                test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected_points, 'bytes');
            });
    }

    it('writes and reads with weekly indexes', function() {
        return check('week', '2014.38');
    });

    it('writes and reads with monthly indexes', function() {
        return check('month', '2014.09');
    });

    it('writes and reads with yearly indexes', function() {
        return check('year', '2014');
    });

    it('writes and reads with one index', function() {
        return check('none', '');
    });

    it('errors on bogus interval', function() {
        var message = 'invalid interval: bananas; accepted intervals are "day", "week", "month" "year", and "none"';
        return Promise.all([
            test_utils.expect_to_fail(test_utils.read_all('local', '', 'bananas'), message),
            test_utils.expect_to_fail(test_utils.write(points_to_write, 'local', 'bananas'), message)
        ]);
    });
});
