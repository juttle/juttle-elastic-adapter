'use strict';

var _ = require('underscore');
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var util = require('util');

var test_utils = require('./elastic-test-utils');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var points = require('./apache-sample');
var DYNAMIC_MAPPING_SETTINGS = require('../lib/dynamic-mapping-settings');
var elastic = require('../lib/elastic');

var modes = test_utils.modes;

function assert_not_analyzed(settings) {
    expect(settings.dynamic_templates)
        .deep.equal(DYNAMIC_MAPPING_SETTINGS.dynamic_templates);

    var had_not_analyzed = false;
    _.each(settings.properties, function(property) {
        if (property.type === 'string') {
            had_not_analyzed = true;
            expect(property.index).equal('not_analyzed');
        }
    });

    expect(had_not_analyzed).equal(true);
}

describe('elastic source', function() {
    modes.forEach(function(type) {
        describe('basic functionality -- ' + type, function() {
            before(function() {
                elastic.clear_already_created_indices();
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
                    return test_utils.read({from: '1 minute ago', to: 'now', id: type}, '| reduce count()');
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([{count: 0}]);
                    return test_utils.read({index: 'no_such_index', id: type});
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([]);
                    expect(result.errors).deep.equal([]);
                });
            });

            it('creates indexes with appropriate mapping', function() {
                return test_utils.get_mapping(type)
                    .then(function(mapping) {
                        var settings = mapping[test_utils.test_id].mappings.event;
                        assert_not_analyzed(settings);
                    });
            });

            it('reads points from Elastic', function() {
                return test_utils.read({id: type})
                .then(function(result) {
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
                });
            });

            it('default from/to: error', function() {
                var program = util.format('read elastic -id "%s"', type);
                var failing_read = check_juttle({
                    program: program
                });
                var message = '-from, -to, or -last must be specified';

                return test_utils.expect_to_fail(failing_read, message);
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

            it('free text search', function() {
                function test_fts(string) {
                    return test_utils.read({id: type}, `"${string}"`)
                        .then(function(result) {
                            var expected = points.filter(function matches_string(pt) {
                                return _.any(pt, function(value, key) {
                                    return typeof value === 'string' && value.indexOf(string) !== -1;
                                });
                            });

                            test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                        });
                }

                return test_fts('presentations')
                    .then(function() {
                        return test_fts('/presentations/logstash-monitorama-2013/images/kibana-search.png');
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

            it('reads with a tag filter including special characters', function() {
                var request = '/presentations/logstash-monitorama-2013/images/kibana-search.png';
                return test_utils.read({id: type}, `request = "${request}"`)
                .then(function(result) {
                    var expected = points.filter(function(pt) {
                        return pt.request === request;
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

            it('counts points', function() {
                var start = '2014-09-17T14:13:42.000Z';
                var end = '2014-09-17T14:13:43.000Z';
                return test_utils.read({from: start, to: end, id: type}, ' | reduce count()')
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([{count: 3}]);
                });
            });

            it('fails to write a point with a giant field', function() {
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
                        var too_big = /Document contains at least one immense term/;
                        expect(result.errors).match(too_big);
                    });
            });

            it('errors if you write a point without time', function() {
                var timeless = {value: 1, name: 'dave'};

                var program_base = 'emit -points %s | remove time | write elastic -id "%s" -index "timeless"';
                var write_program = util.format(program_base, JSON.stringify([timeless]), type);

                var write_promise = check_juttle({
                    program: write_program
                });

                return test_utils.check_no_write(write_promise, {id: type, index: 'timeless'})
                .then(function(result) {
                    var message = util.format('invalid point: %s because of missing time', JSON.stringify(timeless));
                    expect(result.errors).deep.equal([message]);
                });
            });

            it('rejects regex filters', function() {
                var failing_read = test_utils.read({id: type}, 'clientip =~ /2/');
                var message = 'read elastic filters cannot contain regular expressions';

                return test_utils.expect_to_fail(failing_read, message);
            });

            it('warns if you filter on an unknown field', function() {
                return retry(function() {
                    return test_utils.read({id: type}, 'bananas = "pajamas"')
                        .then(function(result) {
                            var warning = `index "${test_utils.test_id}" has no ` +
                                `known property "bananas" for type "event"`;
                            expect(result.warnings).deep.equal([warning]);
                            expect(result.errors).deep.equal([]);
                            expect(result.sinks.table).deep.equal([]);
                        });
                });
            });

            it('warns if you reduce by an unknown field', function() {
                return retry(function() {
                    return test_utils.read({id: type}, '| reduce by bananas')
                        .then(function(result) {
                            var warning = `index "${test_utils.test_id}" has no ` +
                                `known property "bananas" for type "event"`;
                            expect(result.warnings).deep.equal([warning]);
                            expect(result.errors).deep.equal([]);
                            expect(result.sinks.table).deep.equal([{bananas: null}]);
                        });
                });
            });

            describe('timeField', function() {
                var time = new Date().toISOString();
                var my_timed_point = [{time: time, name: 'my_time_test'}];
                it('reads and writes', function() {
                    return test_utils.write(my_timed_point, {timeField: 'my_time', id: type})
                        .then(function() {
                            return test_utils.verify_import(my_timed_point, type, '*', {timeField: 'my_time'});
                        })
                        .then(function() {
                            return test_utils.search(type, test_utils.test_id);
                        })
                        .then(function(es_result) {
                            var sources = _.pluck(es_result.hits.hits, '_source');
                            var my_point = _.findWhere(sources, {name: 'my_time_test'});
                            expect(my_point.my_time).equal(time);

                            var extra = 'name="my_time_test"';
                            return test_utils.read({timeField: 'my_time', id: type}, extra);
                        })
                        .then(function(result) {
                            expect(result.sinks.table).deep.equal(my_timed_point);
                            var extra = 'name="my_time_test" | reduce count()';
                            return test_utils.read({timeField: 'my_time', id: type}, extra);
                        })
                        .then(function(result) {
                            expect(result.sinks.table).deep.equal([{count: 1}]);
                        });
                });

                it('optimizes', function() {
                    var end_ts = new Date(time).getTime() + 1;
                    var end = new Date(end_ts).toISOString();
                    var extra = 'name="my_time_test" | reduce -every :ms: count()';
                    var options = {from: time, to: end, id: type, timeField: 'my_time'};
                    return test_utils.read(options, extra)
                        .then(function(result) {
                            expect(result.sinks.table).deep.equal([{time: end, count: 1}]);
                        });
                });

                it('warns if you clobber a timeField field', function() {
                    return test_utils.write(my_timed_point, {timeField: 'name', id: type})
                        .then(function(result) {
                            var message = util.format('clobbering name value of {"name":"my_time_test"} with %s', time);
                            expect(result.warnings).deep.equal([message]);
                            var expected_point = {name: time};
                            return test_utils.verify_import([expected_point], type, '*', {timeField: 'my_time'});
                        });
                });

                it('warns if you read a nonexistent timefield', function() {
                    return retry(function() {
                        return test_utils.read({id: type, timeField: 'bananas'})
                            .then(function(result) {
                                var warning = `index "${test_utils.test_id}" has no ` +
                                    `known property "bananas" for type "event"`;
                                expect(result.warnings).deep.equal([warning]);
                            });
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
                var types_index = test_utils.test_id + 'type';

                it('writes', function() {
                    return test_utils.write([point1], {type: type1, id: type, index: types_index})
                        .then(function() {
                            return test_utils.write([point2], {type: type2, id: type, index: types_index});
                        })
                        .then(function() {
                            return test_utils.verify_import([point1, point2], type);
                        })
                        .then(function() {
                            return test_utils.get_mapping(type);
                        })
                        .then(function(mapping) {
                            var settings = mapping[types_index].mappings[type1];
                            assert_not_analyzed(settings);
                        });
                });

                it('reads', function() {
                    return test_utils.read({type: type1, id: type, index: types_index})
                        .then(function(result) {
                            expect(result.sinks.table).deep.equal([point1]);
                            return test_utils.read({type: type2, id: type, index: types_index}, '| reduce count()');
                        })
                        .then(function(result) {
                            expect(result.sinks.table).deep.equal([{count: 1}]);
                        });
                });

                it('default - reads all types', function() {
                    return test_utils.read({id: type, index: types_index})
                        .then(function(result) {
                            expect(result.sinks.table).deep.equal([point1, point2]);
                        });
                });

                it('reads and writes with a configured default type', function() {
                    var point = {time: new Date().toISOString(), name: 'configured default'};
                    var id = type === 'aws' ? test_utils.aws_has_default_type_id :
                        test_utils.has_default_type;
                    return test_utils.write([point], {id: id, index: types_index})
                        .then(function() {
                            return test_utils.verify_import([point], type);
                        })
                        .then(function() {
                            return test_utils.get_mapping(type);
                        })
                        .then(function(mapping) {
                            var types = Object.keys(mapping[types_index].mappings);
                            var expected_type = type === 'aws' ? 'aws_default_type' : 'my_test_type';
                            expect(types).contain(expected_type);

                            return test_utils.read({id: id, index: types_index});
                        })
                        .then(function(result) {
                            expect(result.sinks.table).deep.equal([point]);
                        });
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
            return test_utils.write([{time: new Date().toISOString()}], {id: 'b'})
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

        it('juttle index has right mapping', function() {
            return test_utils.get_mapping('local')
                .then(function(mapping) {
                    var settings = mapping.juttle.mappings.event;
                    assert_not_analyzed(settings);
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
            })
            .then(function() {
                return test_utils.get_mapping('local');
            })
            .then(function(mapping) {
                var settings = mapping[test_index].mappings.event;
                assert_not_analyzed(settings);
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

    describe('idField', function() {
        var time = new Date().toISOString();
        var id_point = {time: time, name: 'id_test', id_field: 'my_id', value: 10};
        var id_field = 'id_field';

        after(function() {
            return test_utils.clear_data();
        });

        it('reads and writes', function() {
            return test_utils.write([id_point], {idField: 'id_field'})
                .then(function(result) {
                    return test_utils.verify_import([_.omit(id_point, id_field)]);
                })
                .then(function() {
                    return test_utils.read({idField: 'read_id_field'});
                })
                .then(function(result) {
                    var expected = _.omit(id_point, id_field);
                    expected.read_id_field = 'my_id';

                    expect(result.sinks.table).deep.equal([expected]);
                });
        });

        it('aborts optimization on reduce by idField', function() {
            return test_utils.read({idField: id_field}, ' | reduce avg(value) by ' + id_field)
                .then(function(result) {
                    var expected = {avg: id_point.value};
                    expected[id_field] = id_point[id_field];
                    expect(result.sinks.table).deep.equal([expected]);
                    expect(result.prog.graph.adapter.es_opts.aggregations).equal(undefined);
                });
        });

        it('reads and writes with -idField "_id"', function() {
            var _id_point = {time: time, name: '_id_test', _id: 'my__id', value: 20};
            return test_utils.write([_id_point], {idField: '_id'})
                .then(function(result) {
                    return test_utils.verify_import([_.omit(_id_point, '_id')]);
                })
                .then(function() {
                    return test_utils.read({idField: '_id'}, 'name = "_id_test"');
                })
                .then(function(result) {
                    var expected = _.omit(_id_point, '_id');
                    expected._id = 'my__id';

                    expect(result.sinks.table).deep.equal([expected]);
                });
        });
    });
});
