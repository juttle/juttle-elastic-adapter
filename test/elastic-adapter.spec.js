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

var modes = test_utils.modes;

describe('elastic source', function() {
    this.timeout(300000);
    modes.forEach(function(type) {
        after(function() {
            return test_utils.clear_data(type);
        });

        describe('basic functionality -- ' + type, function() {
            before(function() {
                var points_to_write = points.map(function(point) {
                    var point_to_write = _.clone(point);
                    point_to_write.time /= 1000;
                    return point_to_write;
                });

                return test_utils.write(points_to_write, type)
                .then(function(res) {
                    expect(res.errors).deep.equal([]);
                    return test_utils.verify_import(points, type);
                });
            });

            it('gracefully handles a lack of data', function() {
                return test_utils.read('1 minute ago', 'now', type)
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([]);
                    expect(result.errors).deep.equal([]);
                });
            });

            it('reads points from Elastic', function() {
                return test_utils.read_all(type)
                .then(function(result) {
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected_points, 'bytes');
                });
            });

            it('reads with a nontrivial time filter', function() {
                var start = '2014-09-17T14:13:42.000Z';
                var end = '2014-09-17T14:13:43.000Z';
                return test_utils.read(start, end, type)
                .then(function(result) {
                    var expected = expected_points.filter(function(pt) {
                        return pt.time >= start && pt.time < end;
                    });

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                });
            });

            it('reads with tag filter', function() {
                return test_utils.read_all(type, 'clientip = "93.114.45.13"')
                .then(function(result) {
                    var expected = expected_points.filter(function(pt) {
                        return pt.clientip === '93.114.45.13';
                    });

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                });
            });

            it('reads with free text search', function() {
                return test_utils.read_all(type, '"Ubuntu"')
                .then(function(result) {
                    var expected = expected_points.filter(function(pt) {
                        return _.any(pt, function(value, key) {
                            return typeof value === 'string' && value.match(/Ubuntu/);
                        });
                    });

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                });
            });

            it('reads with -last', function() {
                var program = util.format('read elastic -last :10 years: -id "%s" -index_prefix "%s"', type, test_utils.test_id);
                return check_juttle({
                    program: program
                })
                .then(function(result) {
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected_points, 'bytes');
                });
            });

            it('compiles moments in filter expressions', function() {
                return test_utils.read_all(type, 'client_ip != :5 minutes ago:')
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected_points, 'bytes');
                    });
            });

            it('compiles durations in filter expressions', function() {
                return test_utils.read_all(type, 'client_ip != :5 minutes:')
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected_points, 'bytes');
                    });
            });

            it('counts points', function() {
                var start = '2014-09-17T14:13:42.000Z';
                var end = '2014-09-17T14:13:43.000Z';
                return test_utils.read(start, end, type, ' | reduce count()')
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([{count: 3}]);
                });
            });

            it('errors if you write a point without time', function() {
                var timeless = {value: 1, name: 'dave'};

                var write_program = util.format('emit -points %s | remove time | write elastic -id "%s"', JSON.stringify([timeless]), type);

                return check_juttle({
                    program: write_program
                })
                .then(function(result) {
                    var message = util.format('invalid point: %s because of missing time', JSON.stringify(timeless));
                    expect(result.errors).deep.equal([message]);
                });
            });
        });
    });

    describe('endpoints', function() {
        it('reads with -id "b", a broken endpoint', function() {
            return test_utils.read_all('b')
            .then(function(result) {
                expect(result.errors).deep.equal(['Failed to connect to Elasticsearch']);
            });
        });

        it('writes with -id "b", a broken endpoint', function() {
            return test_utils.write([{}], 'b')
            .then(function(result) {
                expect(result.errors).deep.equal(['insertion failed: Failed to connect to Elasticsearch']);
            });
        });

        it('errors if you read from nonexistent id', function() {
            return test_utils.expect_to_fail(test_utils.read_all('bananas'), 'invalid id: bananas');
        });

        it('errors if you write to nonexistent id', function() {
            return test_utils.write([{}], 'pajamas')
            .then(function(result) {
                expect(result.errors).deep.equal(['invalid id: pajamas']);
            });
        });
    });

    describe('-index argument', function() {
        var test_index = 'test';

        after(function() {
            var indexes = util.format('%s*,%s*', test_index, test_utils.test_index_prefix);
            return test_utils.clear_data(null, indexes);
        });

        it('read - no such index', function() {
            var program = 'read elastic -last :10 years: -index_prefix "no_such_index"';
            return check_juttle({
                program: program
            })
            .then(function(result) {
                expect(result.sinks.table).deep.equal([]);
                expect(result.errors).deep.equal([]);
            });
        });

        it('writes and reads a specified index', function() {
            var point = {
                time: new Date().toISOString(),
                test: '-index_prefix'
            };
            var write_program = util.format('emit -points %s | write elastic -index_prefix "%s"', JSON.stringify([point]), test_index);
            return check_juttle({
                program: write_program
            })
            .then(function() {
                var read_program = util.format('read elastic -last :10 years: -index_prefix "%s"', test_index);
                return retry(function() {
                    return check_juttle({
                        program: read_program
                    })
                    .then(function(result) {
                        expect(result.sinks.table).deep.equal([point]);
                    });
                }, {max_tries: 10});
            });
        });

        it('uses a different default if one is configured', function() {
            var index_regex = new RegExp(test_utils.test_index_prefix);
            var point = {
                time: new Date().toISOString(),
                test: 'custom_prefix'
            };
            return test_utils.list_indices()
                .then(function(indices) {
                    expect(indices).not.match(index_regex);
                })
                .then(function() {
                    var write_program = util.format('emit -points %s | write elastic -id "%s"', JSON.stringify([point]), test_utils.has_index_id);
                    return check_juttle({
                        program: write_program
                    });
                })
                .then(function() {
                    return test_utils.list_indices();
                })
                .then(function(indices) {
                    expect(indices).match(index_regex);
                });
        });
    });
});
