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

            it('writes a point with a giant field', function() {
                var GIANT_FIELD_LENGTH = 32766;
                var giant_string = '';
                for (var i = 0; i <= GIANT_FIELD_LENGTH; i++) {
                    giant_string += '@';
                }

                var giant_point = {
                    time: new Date().toISOString(),
                    giant_field: giant_string
                };

                return test_utils.write([giant_point], {id: type})
                    .then(function(result) {
                        return test_utils.verify_import([giant_point], type);
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
            return test_utils.expect_to_fail(test_utils.write([{}], {id: 'pajamas'}), 'invalid id: pajamas');
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

    describe('write edge cases', function() {
        after(function() {
            return test_utils.clear_data();
        });

        it('writes a nested object', function() {
            var nested_object = {
                time: new Date().toISOString(),
                nest: {nested_key: 'nest'},
                name: 'nest_haver'
            };
            return test_utils.write([nested_object])
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    return retry(function() {
                        return test_utils.search()
                            .then(function(result) {
                                var hits = _.pluck(result.hits.hits, '_source');
                                var imported = _.findWhere(hits, {name: 'nest_haver'});

                                expect(imported).exist; // jshint ignore:line
                                expect(imported.nest).deep.equal(nested_object.nest);
                            });
                    });
                });
        });

        it('fails to write a point with an _id field', function() {
            var _id_point = {time: new Date().toISOString(), _id: 'this is broken now'};
            return test_utils.write([_id_point])
                .then(function(result) {
                    expect(result.errors).match(/point rejected by Elasticsearch/);
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

    describe('-type', function() {
        var time1 = new Date().toISOString();
        var time2 = new Date(Date.now() + 1).toISOString();
        var type1 = 'type1';
        var type2 = 'type2';
        var point1 = {name: 'type1_test', time: time1};
        var point2 = {name: 'type2_test', time: time2};

        after(function() {
            return test_utils.clear_data();
        });

        it('writes', function() {
            return test_utils.write([point1], {type: type1})
                .then(function() {
                    return test_utils.write([point2], {type: type2});
                })
                .then(function() {
                    return test_utils.verify_import([point1, point2]);
                })
                .then(function() {
                    return test_utils.list_types();
                })
                .then(function(types) {
                    expect(types).contain(type1);
                    expect(types).contain(type2);
                });
        });

        it('reads', function() {
            return test_utils.read({type: type1})
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([point1]);
                    return test_utils.read({type: type2}, '| reduce count()');
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([{count: 1}]);
                });
        });

        it('default - reads all types', function() {
            return test_utils.read()
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([point1, point2]);
                });
        });

        it('reads and writes with a configured default type', function() {
            var point = {time: new Date().toISOString(), name: 'configured default'};
            return test_utils.write([point], {id: test_utils.has_default_type})
                .then(function() {
                    return test_utils.verify_import([point]);
                })
                .then(function() {
                    return test_utils.list_types();
                })
                .then(function(types) {
                    expect(types).contain('my_test_type');
                    return test_utils.read({id: test_utils.has_default_type});
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([point]);
                });
        });
    });
});
