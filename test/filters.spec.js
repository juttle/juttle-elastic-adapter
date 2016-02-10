var _ = require('underscore');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');
var points = require('./apache-sample');

var modes = test_utils.modes;

modes.forEach(function(mode) {
    describe(`filters -- ${mode}`, function() {
        this.timeout(300000);
        before(function() {
            return test_utils.write(points, {id: mode})
            .then(function(res) {
                expect(res.errors).deep.equal([]);
                return test_utils.verify_import(points, mode);
            });
        });

        after(function() {
            return test_utils.clear_data(mode);
        });

        function filter_test(filter, expected) {
            return test_utils.read({id: mode}, filter)
            .then(function(result) {
                test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
            });
        }

        it('reads with free text search', function() {
            var expected = points.filter(function(pt) {
                return _.any(pt, function(value, key) {
                    return typeof value === 'string' && value.match(/Ubuntu/);
                });
            });

            return filter_test('"Ubuntu"', expected);
        });

        it('compiles moments in filter expressions', function() {
            return filter_test('clientip != :5 minutes ago:', points);
        });

        it('compiles durations in filter expressions', function() {
            return filter_test('clientip != :5 minutes:', points);
        });

        it('null', function() {
            return filter_test('bananas = null', points)
                .then(function() {
                    return filter_test('clientip != null', points);
                });
        });

        it('boolean', function() {
            var expected = points.filter(function(pt) {
                return pt.boolean;
            });
            return filter_test('boolean = true', expected)
                .then(function() {
                    var expected = points.filter(function(pt) {
                        return pt.boolean === false;
                    });

                    return filter_test('boolean = false', expected);
                });
        });

        it('number', function() {
            var bytes = 171717;
            var expected = points.filter(function(pt) {
                return pt.bytes === bytes;
            });

            return filter_test(`bytes = ${bytes}`, expected);
        });

        it('in', function() {
            var bytes = [26185, 8095];
            var expected = points.filter(function(pt) {
                return bytes.indexOf(pt.bytes) !== -1;
            });

            return filter_test(`bytes in [${bytes}]`, expected)
                .then(function() {
                    var ips = ['"24.236.252.67"', '"83.149.9.216"'];
                    var expected = points.filter(function(pt) {
                        return pt.clientip === '24.236.252.67' ||
                            pt.clientip === '83.149.9.216';
                    });

                    return filter_test(`clientip in [${ips}]`, expected);
                });
        });

        it('not', function() {
            var expected = points.filter(function(pt) {
                return pt.clientip !== '83.149.9.216';
            });

            return filter_test('NOT clientip = "83.149.9.216"', expected);
        });

        it('and', function() {
            var expected = points.filter(function(pt) {
                return pt.clientip === '83.149.9.216' && pt.bytes > 100000;
            });

            return filter_test('clientip = "83.149.9.216" AND bytes > 100000', expected);
        });

        it('or', function() {
            var expected = points.filter(function(pt) {
                return pt.clientip === '83.149.9.216' || pt.bytes > 100000;
            });

            return filter_test('clientip = "83.149.9.216" OR bytes > 100000', expected);
        });

        it('wildcard', function() {
            var expected = points.filter(function(pt) {
                return pt.clientip.indexOf('3.1') !== -1;
            });

            return filter_test('clientip ~ "*3.1*"', expected);
        });

        it('negative wildcard', function() {
            var expected = points.filter(function(pt) {
                return pt.clientip.indexOf('3.1') === -1;
            });

            return filter_test('clientip !~ "*3.1*"', expected);
        });

        it('>', function() {
            var expected = points.filter(function(pt) {
                return pt.bytes > 54662;
            });

            return filter_test('bytes > 54662', expected);
        });

        it('>=', function() {
            var expected = points.filter(function(pt) {
                return pt.bytes >= 54662;
            });

            return filter_test('bytes >= 54662', expected);
        });

        it('<', function() {
            var expected = points.filter(function(pt) {
                return pt.bytes < 54662;
            });

            return filter_test('bytes < 54662', expected);
        });

        it('<=', function() {
            var expected = points.filter(function(pt) {
                return pt.bytes <= 54662;
            });

            return filter_test('bytes <= 54662', expected);
        });

        it('rejects regex filters', function() {
            var failing_read = test_utils.read({id: mode}, 'clientip =~ /2/');
            var message = 'read elastic filters cannot contain regular expressions';

            return test_utils.expect_to_fail(failing_read, message);
        });

        it('rejects NaN filters', function() {
            var failing_read = test_utils.read({id: mode}, 'clientip = NaN');
            var message = 'read elastic filters cannot contain NaN';

            return test_utils.expect_to_fail(failing_read, message);
        });

        it('rejects Infinity filters', function() {
            var failing_read = test_utils.read({id: mode}, 'clientip = Infinity');
            var message = 'read elastic filters cannot contain Infinity';

            return test_utils.expect_to_fail(failing_read, message);
        });

        it('rejects -Infinity filters', function() {
            var failing_read = test_utils.read({id: mode}, 'clientip = -Infinity');
            var message = 'read elastic filters cannot contain Infinity';

            return test_utils.expect_to_fail(failing_read, message);
        });
    });
});
