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
        return test_utils.clear_data('local', '*');
    });

    function check(interval, suffix) {
        var index = suffix ? interval + '*' : 'none';
        return test_utils.write(points_to_write, 'local', index, interval)
            .then(function(result) {
                expect(result.errors).deep.equal([]);
                return test_utils.verify_import(expected_points, 'local', index);
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
                return test_utils.read(start, end, 'local', '', index, interval);
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

    describe('errors', function() {
        it('bogus interval', function() {
            var message = 'invalid interval: bananas; accepted intervals are "day", "week", "month" "year", and "none"';
            return Promise.all([
                test_utils.expect_to_fail(test_utils.read_all('local', '', 'some_index', 'bananas'), message),
                test_utils.expect_to_fail(test_utils.write(points_to_write, 'local', 'some_index', 'bananas'), message)
            ]);
        });

        it('star in middle of write index', function() {
            var index = 's*tar';
            var message = 'cannot write to index pattern: ' + index;
            return test_utils.expect_to_fail(test_utils.write([{}], 'local', index), message);
        });

        it('indexInterval and no star', function() {
            var message = 'with indexInterval, index must end in *';
            var index = 'no_star';
            return Promise.all([
                test_utils.expect_to_fail(test_utils.read_all('local', '', index, 'day'), message),
                test_utils.expect_to_fail(test_utils.write([{}], 'local', index, 'day'), message)
            ]);
        });

        it('star in write index for none', function() {
            var message = 'index for write with interval "none" cannot contain *';
            var index = 'star*';
            return test_utils.expect_to_fail(test_utils.write([{}], 'local', index, 'none'), message);
        });
    });
});
