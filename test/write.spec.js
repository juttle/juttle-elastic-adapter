
var _ = require('underscore');
var retry = require('bluebird-retry');
var expect = require('chai').expect;

var juttle_test_utils = require('juttle/test').utils;
var check_juttle = juttle_test_utils.check_juttle;

juttle_test_utils.withAdapterAPI(function() {
    var elastic = require('../lib/elastic');
    var test_utils = require('./elastic-test-utils');
    var modes = test_utils.modes;

    modes.forEach(function(mode) {
        describe('write ' + mode, function() {
            afterEach(function() {
                elastic.clear_already_created_indices();
                return test_utils.clear_data(mode);
            });

            it('fails to write a point with an _id field', function() {
                var _id_point = {time: new Date().toISOString(), _id: 'this is broken now'};
                return test_utils.write([_id_point], {id: mode})
                    .then(function(result) {
                        expect(result.errors).match(/point rejected by Elasticsearch/);
                    });
            });

            it('writes a nested object', function() {
                var nested_object = {
                    time: new Date().toISOString(),
                    nest: {nested_key: 'nest'},
                    name: 'nest_haver'
                };
                return test_utils.write([nested_object], {id: mode})
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        return retry(function() {
                            return test_utils.search(mode)
                                .then(function(result) {
                                    var hits = _.pluck(result.hits.hits, '_source');
                                    var imported = _.findWhere(hits, {name: 'nest_haver'});
                                    expect(imported).exist;
                                    expect(imported.nest).deep.equal(nested_object.nest);
                                });
                        });
                    });
            });

            it('writes with -chunkSize', function() {
                var chunkSize = 5;
                var points = test_utils.generate_sample_data({count: 100});
                return test_utils.write(points, {id: mode, chunkSize: chunkSize})
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        var write = result.prog.graph.out_.default[0].proc.adapter;
                        expect(write.chunks_written).equal(points.length / chunkSize);
                        return test_utils.verify_import(points, mode);
                    });
            });

            it('writes a point with a moment', function() {
                var time = new Date().toISOString();
                var write_program = `emit -limit 1 | put fake_time = :${time}:` +
                    ` | write elastic -id "${mode}" -index "${test_utils.test_id}"`;

                return check_juttle({
                    program: write_program
                })
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    return retry(function() {
                        return test_utils.read({id: mode}, `fake_time = :${time}:`)
                            .then(function(result) {
                                expect(result.sinks.table[0].fake_time).equal(time);
                            });

                    });
                });
            });

            it('writes a point with a duration', function() {
                var time = '5 minutes';
                var write_program = `emit -limit 1 | put duration = :${time}:` +
                    ` | write elastic -id "${mode}" -index "${test_utils.test_id}"`;

                return check_juttle({
                    program: write_program
                })
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    return retry(function() {
                        return test_utils.read({id: mode})
                            .then(function(result) {
                                expect(result.sinks.table.length).equal(1);
                                expect(result.sinks.table[0].duration).equal('00:05:00.000');
                            });
                    });
                })
                .then(function() {
                    return test_utils.read({id: mode}, `duration = :${time}:`);
                })
                .then(function(result) {
                    expect(result.warnings).deep.equal([]);
                    expect(result.sinks.table.length).equal(1);
                    expect(result.sinks.table[0].duration).equal('00:05:00.000');
                });
            });
        });
    });
});
