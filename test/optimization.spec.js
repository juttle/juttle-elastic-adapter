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

// Register the backend
require('./elastic-test-utils');

describe('optimization', function() {
    this.timeout(300000);

    before(function() {
        return test_utils.clear_logstash_data()
            .then(function() {
                var points_to_write = points.map(function(point) {
                    var point_to_write = _.clone(point);
                    point_to_write.time /= 1000;
                    return point_to_write;
                });
                var program = util.format('emit -points %s | write elastic', JSON.stringify(points_to_write));
                return check_juttle({
                    program: program
                });
            })
            .then(function() {
                return test_utils.verify_import(points);
            });
    });

    it('optimizes head', function() {
        var program = 'read elastic -from :10 years ago: -to :now: | head 3';
        return check_juttle({
            program: program
        })
        .then(function(result) {
            var expected = expected_points.slice(0, 3);
            test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
            expect(result.prog.graph.es_opts.limit).equal(3);
        });
    });

    it('optimizes head with a nontrivial time filter', function() {
        var start = '2014-09-17T14:13:43.000Z';
        var end = '2014-09-17T14:13:46.000Z';
        var program = util.format('read elastic -from :%s: -to :%s: | head 2', start, end);
        return check_juttle({
            program: program
        })
        .then(function(result) {
            var expected = expected_points.filter(function(pt) {
                return pt.time >= start && pt.time < end;
            }).slice(0, 2);

            test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
            expect(result.prog.graph.es_opts.limit).equal(2);
        });
    });

    it('optimizes head with tag filter', function() {
        var program = 'read elastic -from :10 years ago: -to :now: clientip = "93.114.45.13" | head 2';
        return check_juttle({
            program: program
        })
        .then(function(result) {
            var expected = expected_points.filter(function(pt) {
                return pt.clientip === '93.114.45.13';
            }).slice(0, 2);

            test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
            expect(result.prog.graph.es_opts.limit).equal(2);
        });
    });

    it('optimizes head 0 (returns nothing)', function() {
        var program = 'read elastic -from :10 years ago: -to :now: | head 0';
        return check_juttle({
            program: program
        })
        .then(function(result) {
            expect(result.sinks.table).deep.equal([]);
            expect(result.prog.graph.es_opts.limit).equal(0);
        });
    });

    describe('reduce', function() {
        it('optimizes count', function() {
            var program = 'read elastic -from :10 years ago: -to :now: | reduce count()';
            return check_juttle({
                program: program
            })
            .then(function(result) {
                var first_node = result.prog.graph.head[0];
                expect(first_node.procName).equal('elastic_read');

                var second_node = first_node.out_.default[0].proc;
                expect(second_node.procName).equal('clientsink');

                expect(result.sinks.table).deep.equal([{count: 30}]);
                expect(result.prog.graph.es_opts.aggregations.count).equal('count');
            });
        });

        it('optimizes count with a named reducer', function() {
            var program = 'read elastic -from :10 years ago: -to :now: | reduce x=count()';
            return check_juttle({
                program: program
            })
            .then(function(result) {
                expect(result.sinks.table).deep.equal([{x: 30}]);
                expect(result.prog.graph.es_opts.aggregations.count).equal('x');
            });
        });

        it('optimizes count by', function() {
            var program = 'read elastic -from :10 years ago: -to :now: | reduce count() by clientip';
            return check_juttle({
                program: program
            })
            .then(function(result) {
                expect(result.sinks.table).deep.equal([
                    {clientip: '83.149.9.216', count: 23},
                    {clientip: '93.114.45.13', count: 6},
                    {clientip: '24.236.252.67', count: 1}
                ]);

                var aggregations = result.prog.graph.es_opts.aggregations;

                expect(aggregations.count).equal('count');
                expect(aggregations.grouping).deep.equal(['clientip']);
                expect(aggregations.es_aggr).deep.equal({
                    group: {
                        terms: {
                            field: 'clientip',
                            size: 1000000
                        }
                    }
                });
            });
        });
    });
});
