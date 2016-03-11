var Promise = require('bluebird');
var _ = require('underscore');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');
var modes = test_utils.modes;
var expect_to_fail = test_utils.expect_to_fail;
var check_no_write = test_utils.check_no_write;

var MS_IN_DAY = 1000 * 60 * 60 * 24;
var MS_IN_YEAR = MS_IN_DAY * 365;
var points = test_utils.generate_sample_data({
    count: 65,
    start: Date.now() - MS_IN_YEAR,
    interval: MS_IN_DAY
});

function validate_indices(indices, interval) {
    function _regex_for_interval(interval) {
        switch (interval) {
            case 'day':
                return /day[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]/;
            case 'week':
                return /week[0-9][0-9][0-9][0-9]\.[0-9][0-9]?/;
            case 'month':
                return /month[0-9][0-9][0-9][0-9]\.[0-9][0-9]/;
            case 'year':
                return /year[0-9][0-9][0-9][0-9]/;
            case 'none':
                return /none/;
            default:
                throw new Error(`invalid interval ${interval}`);
        }
    }

    var regex = _regex_for_interval(interval);
    indices.forEach(function(index) {
        expect(regex.test(index)).equal(true);
    });
}

modes.forEach(function(mode) {
    describe('index intervals -- ' + mode, function() {
        afterEach(function() {
            var id = test_utils.test_id;
            return test_utils.clear_data(mode, `${id}*`);
        });

        function check(interval) {
            var from_date = new Date(points[0].time);
            var from = from_date.toISOString();

            var before_any_data_ms = from_date.getTime() - MS_IN_DAY * 15;
            var before_any_data = new Date(before_any_data_ms).toISOString();

            var to_ms = new Date(_.last(points).time).getTime();
            var to = new Date(to_ms + 1).toISOString();

            var after_all_data_ms = to_ms + MS_IN_DAY * 15;
            var after_all_data = new Date(after_all_data_ms).toISOString();

            var suffix = interval !== 'none' ? interval + '*' : interval;
            var index = test_utils.test_id + suffix;

            return Promise.map(points, function(pt, n) {
                return test_utils.write([pt], {index: index, indexInterval: interval, id: mode})
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                    });
            }, {concurrency: 10})
            .then(function(results) {
                return test_utils.verify_import(points, mode, index);
            })
            .then(function() {
                return test_utils.list_indices();
            })
            .then(function(indices) {
                validate_indices(indices, interval);

                return test_utils.read({id: mode, from: from, to: to, index: index, indexInterval: interval});
            })
            .then(function(result) {
                test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
                return test_utils.read({id: mode, from: before_any_data, to: from, index: index, indexInterval: interval});
            })
            .then(function(result) {
                expect(result.errors).deep.equal([]);
                expect(result.sinks.table).deep.equal([]);
                return test_utils.read({id: mode, from: before_any_data, to: after_all_data, index: index, indexInterval: interval});
            })
            .then(function(result) {
                test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
            });
        }

        it('writes and reads with daily indices', function() {
            this.timeout(120000);
            return check('day');
        });

        it('writes and reads with weekly indexes', function() {
            return check('week');
        });

        it('writes and reads with monthly indexes', function() {
            return check('month');
        });

        it('writes and reads with yearly indexes', function() {
            return check('year');
        });

        it('writes and reads with one index', function() {
            return check('none');
        });

        describe('errors', function() {
            function check_write_fail(promise, message) {
                return check_no_write(expect_to_fail(promise, message));
            }
            it('bogus interval', function() {
                var message = 'invalid interval: bananas; accepted intervals are "day", "week", "month", "year", and "none"';
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
});
