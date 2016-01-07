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

// Register the adapter
require('./elastic-test-utils');

var modes = test_utils.modes;

describe('elastic source', function() {
    this.timeout(300000);
    modes.forEach(function(type) {
        describe('basic functionality -- ' + type, function() {
            before(function() {
                return test_utils.write(points, {id: type})
                .then(function(res) {
                    expect(res.errors).deep.equal([]);
                    return test_utils.verify_import(points, type);
                });
            });

            after(function() {
                return test_utils.clear_data(type);
            });

            it('gracefully handles a lack of data', function() {
                return test_utils.read({from: '1 minute ago', to: 'now', id: type})
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([]);
                    expect(result.errors).deep.equal([]);
                });
            });

            it('reads points from Elastic', function() {
                return test_utils.read({id: type})
                .then(function(result) {
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
                });
            });

            it('reads with a nontrivial time filter', function() {
                var start = '2014-09-17T14:13:42.000Z';
                var end = '2014-09-17T14:13:43.000Z';
                return test_utils.read({from: start, to: end, id: type})
                .then(function(result) {
                    var expected = points.filter(function(pt) {
                        return pt.time >= start && pt.time < end;
                    });

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                });
            });

            it('reads with tag filter', function() {
                return test_utils.read({id: type}, 'clientip = "93.114.45.13"')
                .then(function(result) {
                    var expected = points.filter(function(pt) {
                        return pt.clientip === '93.114.45.13';
                    });

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                });
            });

            it('reads with free text search', function() {
                return test_utils.read({id: type}, '"Ubuntu"')
                .then(function(result) {
                    var expected = points.filter(function(pt) {
                        return _.any(pt, function(value, key) {
                            return typeof value === 'string' && value.match(/Ubuntu/);
                        });
                    });

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                });
            });

            it('reads with -last', function() {
                var program = util.format('read elastic -last :10 years: -id "%s" -index "%s*"', type, test_utils.test_id);
                return check_juttle({
                    program: program
                })
                .then(function(result) {
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
                });
            });

            it('compiles moments in filter expressions', function() {
                return test_utils.read({id: type}, 'client_ip != :5 minutes ago:')
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
                    });
            });

            it('compiles durations in filter expressions', function() {
                return test_utils.read({id: type}, 'client_ip != :5 minutes:')
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
                    });
            });

            it('counts points', function() {
                var start = '2014-09-17T14:13:42.000Z';
                var end = '2014-09-17T14:13:43.000Z';
                return test_utils.read({from: start, to: end, id: type}, ' | reduce count()')
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([{count: 3}]);
                });
            });

            it('errors if you write a point without time', function() {
                var timeless = {value: 1, name: 'dave'};

                var program_base = 'emit -points %s | remove time | write elastic -id "%s" -index "timeless"';
                var write_program = util.format(program_base, JSON.stringify([timeless]), type);

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
            return test_utils.read({id: 'b'})
            .then(function(result) {
                expect(result.errors).deep.equal(['Failed to connect to Elasticsearch']);
            });
        });

        it('writes with -id "b", a broken endpoint', function() {
            return test_utils.write([{}], {id: 'b'})
            .then(function(result) {
                expect(result.errors).deep.equal(['insertion failed: Failed to connect to Elasticsearch']);
            });
        });

        it('errors if you read from nonexistent id', function() {
            return test_utils.expect_to_fail(test_utils.read({id: 'bananas'}), 'invalid id: bananas');
        });

        it('errors if you write to nonexistent id', function() {
            return test_utils.write([{}], {id: 'pajamas'})
            .then(function(result) {
                expect(result.errors).deep.equal(['invalid id: pajamas']);
            });
        });
    });

    describe('-index argument', function() {
        var test_index = 'test';

        after(function() {
            var indexes = util.format('%s*,%s*,juttle', test_index, test_utils.test_index);
            return test_utils.clear_data(null, indexes);
        });

        it('default configuration: juttle index', function() {
            var program = util.format('emit -points %s | write elastic', JSON.stringify(points));
            return check_juttle({
                program: program
            })
            .then(function(result) {
                expect(result.errors).deep.equal([]);
                return test_utils.verify_import(points, 'local', 'juttle');
            })
            .then(function() {
                var read = 'read elastic -last :10 years:';
                return check_juttle({program: read});
            })
            .then(function(result) {
                test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
            });
        });

        it('read - no such index', function() {
            var program = 'read elastic -last :10 years: -index "no_such_index"';
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
                test: '-index'
            };
            var write_program = util.format('emit -points %s | write elastic -index "%s"', JSON.stringify([point]), test_index);
            return check_juttle({
                program: write_program
            })
            .then(function() {
                var read_program = util.format('read elastic -last :10 years: -index "%s"', test_index);
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
            var index_regex = new RegExp(test_utils.test_index);
            var point = {
                time: new Date().toISOString(),
                test: 'custom_index'
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

    describe('timeField', function() {
        after(function() {
            return test_utils.clear_data();
        });

        var time = new Date().toISOString();
        var my_timed_point = [{time: time, name: 'my_time_test'}];
        it('reads and writes', function() {
            return test_utils.write(my_timed_point, {timeField: 'my_time'})
                .then(function() {
                    return test_utils.verify_import(my_timed_point);
                })
                .then(function() {
                    return test_utils.search();
                })
                .then(function(es_result) {
                    var sources = _.pluck(es_result.hits.hits, '_source');
                    var my_point = _.findWhere(sources, {name: 'my_time_test'});
                    expect(my_point.my_time).equal(time);

                    var extra = 'name="my_time_test"';
                    return test_utils.read({timeField: 'my_time'}, extra);
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal(my_timed_point);
                    var extra = 'name="my_time_test" | reduce count()';
                    return test_utils.read({timeField: 'my_time'}, extra);
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([{count: 1}]);
                });
        });

        it('optimizes', function() {
            var end_ts = new Date(time).getTime() + 1;
            var end = new Date(end_ts).toISOString();
            var extra = 'name="my_time_test" | reduce -every :ms: count()';
            var options = {from: time, to: end, id: 'local', timeField: 'my_time'};
            return test_utils.read(options, extra)
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([{time: end, count: 1}]);
                });
        });
    });
});
