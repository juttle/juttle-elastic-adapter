var Promise = require('bluebird');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');
var expect_to_fail = test_utils.expect_to_fail;
var check_no_write = test_utils.check_no_write;
var points = require('./apache-sample');

describe('index intervals', function() {
    afterEach(function() {
        return test_utils.clear_data('local', '*');
    });

    function check(interval, suffix) {
        var index = suffix ? interval + '*' : 'none';
        return test_utils.write(points, {index: index, indexInterval: interval})
            .then(function(result) {
                expect(result.errors).deep.equal([]);
                return test_utils.verify_import(points, 'local', index);
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
                return test_utils.read({from: start, to: end, index: index, indexInterval: interval});
            })
            .then(function(result) {
                test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
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
        function check_write_fail(promise, message) {
            return check_no_write(expect_to_fail(promise, message));
        }
        it('bogus interval', function() {
            var message = 'invalid interval: bananas; accepted intervals are "day", "week", "month" "year", and "none"';
            var bad_opts = {indexInterval: 'bananas'};
            return Promise.all([
                expect_to_fail(test_utils.read(bad_opts), message),
                check_write_fail(test_utils.write(points, bad_opts), message)
            ]);
        });

        it('star in middle of write index', function() {
            var index = 's*tar';
            var message = 'cannot write to index pattern: ' + index;
            return check_write_fail(test_utils.write([{}], {index: index}), message);
        });

        it('indexInterval and no star', function() {
            var message = 'with indexInterval, index must end in *';
            var index = 'no_star';
            var bad_opts = {index: index, indexInterval: 'day'};
            return Promise.all([
                expect_to_fail(test_utils.read(bad_opts), message),
                check_write_fail(test_utils.write([{}], bad_opts), message)
            ]);
        });

        it('star in write index for none', function() {
            var message = 'index for write with interval "none" cannot contain *';
            var index = 'star*';
            return check_write_fail(test_utils.write([{}], {index: index}), message);
        });

        it('invalid index name', function() {
            var time = new Date().toISOString();
            var write = test_utils.write([{time: time}], {index: 'spaces are bad'})
                .then(function(result) {
                    expect(result.errors).match(/Invalid index name/);
                });

            return check_no_write(write);
        });
    });
});
