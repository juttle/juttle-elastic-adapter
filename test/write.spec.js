var _ = require('underscore');
var retry = require('bluebird-retry');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');

var modes = test_utils.modes;

describe('write', function() {
    this.timeout(300000);
    modes.forEach(function(type) {
        describe(type, function() {
            afterEach(function() {
                return test_utils.clear_data(type);
            });

            it('fails to write a point with an _id field', function() {
                var _id_point = {time: new Date().toISOString(), _id: 'this is broken now'};
                return test_utils.write([_id_point], {id: type})
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
                return test_utils.write([nested_object], {id: type})
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        return retry(function() {
                            return test_utils.search(type)
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
                return test_utils.write(points, {id: type, chunkSize: chunkSize})
                    .then(function(result) {
                        var write = result.prog.graph.out_.default[0].proc.adapter;
                        expect(write.chunks_written).equal(points.length / chunkSize);
                        return test_utils.verify_import(points, type);
                    });
            });
        });
    });
});
