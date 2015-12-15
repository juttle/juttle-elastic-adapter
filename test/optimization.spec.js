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

function format_juttle_result_like_es(pts) {
    pts.forEach(function(pt) {
        _.each(pt, function nullify_infinities(value, key) {
            if (value === Infinity || value === -Infinity) {
                pt[key] = null;
            }
        });
    });

    return pts;
}

var start = new Date(points[0].time).toISOString();
var end = new Date(_.last(points).time+1).toISOString();

// Register the adapter
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

        it('optimizes reduce -every count()', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: count()', start, end);
            return test_utils.check_optimization(program);
        });

        it('optimizes reduce -every count() by', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: count() by clientip', start, end);
            return test_utils.check_optimization(program);
        });

        it('optimizes reduce -every -on count()', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: -on :0.5s: count()', start, end);
            return test_utils.check_optimization(program);
        });

        it('optimizes reduce -every -on count() by', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: -on :0.3s: count() by clientip', start, end);
            return test_utils.check_optimization(program);
        });

        it('-on a non-duration moment', function() {
            var on = new Date().toISOString();
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: -on :%s: count() by clientip', start, end, on);
            return test_utils.check_optimization(program);
        });

        it('optimizes non-count reducers', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce sum(bytes), avg(bytes), max(bytes), min(bytes), count_unique(bytes)', start, end);
            return test_utils.check_optimization(program);
        });

        it('optimizes non-count reducers -every', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: sum(bytes), avg(bytes), max(bytes), min(bytes), count_unique(bytes)', start, end);
            return test_utils.check_optimization(program, {
                massage: format_juttle_result_like_es
            });
        });

        it('optimizes non-count reducers -every -on', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: -on :0.2s: sum(bytes), avg(bytes), max(bytes), min(bytes), count_unique(bytes)', start, end);
            return test_utils.check_optimization(program, {
                massage: format_juttle_result_like_es
            });
        });

        it('optimizes no reducers', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce by clientip', start, end);
            return test_utils.check_optimization(program, {
                massage: function(array) {
                    return _.sortBy(array, 'clientip');
                }
            });
        });

        it('optimizes no reducers -every', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: by clientip', start, end);
            return test_utils.check_optimization(program);
        });

        it('optimizes no reducers -every -on', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: -on :0.4s: by clientip', start, end);
            return test_utils.check_optimization(program);
        });

        it('doesn\'t optimize reduce -acc true', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce -every :s: -acc true by clientip', start, end);
            return check_juttle({program: program})
                .then(function(result) {
                    expect(result.prog.graph.es_opts).deep.equal({ limit: undefined, aggregations: undefined });
                });
        });

        // travis doesn't have aggkey.groovy so we can't test this in the CI
        it.skip('optimizes reduce by with missing fields', function() {
            var program = util.format('read elastic -from :%s: -to :%s: | reduce count() by clientip, garbage', start, end);
            return test_utils.check_optimization(program, {
                massage: function(array) {
                    return _.sortBy(array, 'clientip');
                }
            });
        });
    });
});
