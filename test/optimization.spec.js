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
var end_ms = new Date(_.last(points).time).getTime() + 1;
var end = new Date(end_ms).toISOString();

function sortBy() {
    var fields = arguments;
    return function(array) {
        return _.sortBy(array, function(pt) {
            return _.reduce(fields, function(memo, field) {
                return memo + pt[field];
            }, '');
        });
    };
}

// Register the adapter
require('./elastic-test-utils');

var modes = test_utils.modes;

describe('optimization', function() {
    this.timeout(300000);

    modes.forEach(function(type) {
        describe(type, function() {
            after(function() {
                return test_utils.clear_data(type);
            });

            before(function() {
                return test_utils.write(points, {id: type})
                .then(function() {
                    return test_utils.verify_import(points, type);
                });
            });

            function expect_graph_has_limit(graph, limit) {
                expect(graph.adapter.es_opts.limit).equal(limit);
                expect(graph.adapter.executed_queries[0].size).equal(limit);
            }

            describe('head', function() {
                it('optimizes head', function() {
                    return test_utils.read({id: type}, '| head 3')
                    .then(function(result) {
                        var expected = points.slice(0, 3);
                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                        expect_graph_has_limit(result.prog.graph, 3);
                    });
                });

                it('optimizes head with a nontrivial time filter', function() {
                    var start = '2014-09-17T14:13:43.000Z';
                    var end = '2014-09-17T14:13:46.000Z';
                    return test_utils.read({from: start, to: end, id: type}, '| head 2')
                    .then(function(result) {
                        var expected = points.filter(function(pt) {
                            return pt.time >= start && pt.time < end;
                        }).slice(0, 2);

                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                        expect_graph_has_limit(result.prog.graph, 2);
                    });
                });

                it('optimizes head with tag filter', function() {
                    return test_utils.read({id: type}, 'clientip = "93.114.45.13" | head 2')
                    .then(function(result) {
                        var expected = points.filter(function(pt) {
                            return pt.clientip === '93.114.45.13';
                        }).slice(0, 2);

                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                        expect_graph_has_limit(result.prog.graph, 2);
                    });
                });

                it('optimizes head 0 (returns nothing)', function() {
                    return test_utils.read({id: type}, '| head 0')
                    .then(function(result) {
                        expect(result.sinks.table).deep.equal([]);
                        expect(result.prog.graph.adapter.es_opts.limit).equal(0);
                    });
                });
            });

            describe('tail', function() {
                it('optimizes tail', function() {
                    return test_utils.read({id: type}, '| tail 4')
                    .then(function(result) {
                        var expected = _.last(points, 4);
                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                        expect_graph_has_limit(result.prog.graph, 4);
                    });
                });

                it('optimizes tail with a nontrivial time filter', function() {
                    var start = '2014-09-17T14:13:43.000Z';
                    var end = '2014-09-17T14:13:46.000Z';
                    return test_utils.read({from: start, to: end, id: type}, '| tail 4')
                    .then(function(result) {
                        var points_in_range = points.filter(function(pt) {
                            return pt.time >= start && pt.time < end;
                        });

                        var expected = _.last(points_in_range, 4);

                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                        expect_graph_has_limit(result.prog.graph, 4);
                    });
                });

                it('optimizes tail with tag filter', function() {
                    return test_utils.read({id: type}, 'clientip = "93.114.45.13" | tail 4')
                    .then(function(result) {
                        var points_in_range = points.filter(function(pt) {
                            return pt.clientip === '93.114.45.13';
                        });

                        var expected = _.last(points_in_range, 4);

                        test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                        expect_graph_has_limit(result.prog.graph, 4);
                    });
                });

                it('optimizes tail 0 (returns nothing)', function() {
                    return test_utils.read({id: type}, '| tail 0')
                    .then(function(result) {
                        expect(result.sinks.table).deep.equal([]);
                        expect(result.prog.graph.adapter.executed_queries.length).equal(0);
                    });
                });

                it('optimizes tail after tail', function() {
                    return test_utils.read({id: type}, '| tail 5 | tail 4')
                        .then(function(result) {
                            var expected = _.last(points, 4);
                            test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                            expect_graph_has_limit(result.prog.graph, 4);
                        });
                });

                it('doesn\'t optimize over prior head optimization', function() {
                    return test_utils.read({id: type}, '| head 3 | tail 0', function() {
                        expect(result.sinks.table).deep.equal([]);
                        expect_graph_has_limit(result.prog.graph, 3);
                    });
                });
            });

            describe('reduce', function() {
                it('optimizes count', function() {
                    return test_utils.read({id: type}, '| reduce count()')
                    .then(function(result) {
                        var first_node = result.prog.graph.head[0];
                        expect(first_node.procName).equal('read');

                        var second_node = first_node.out_.default[0].proc;
                        expect(second_node.procName).equal('view');

                        expect(result.sinks.table).deep.equal([{count: 30}]);
                        expect(result.prog.graph.adapter.es_opts.aggregations.count).equal('count');
                    });
                });

                it('optimizes count with a named reducer', function() {
                    return test_utils.read({id: type}, '| reduce x=count()')
                    .then(function(result) {
                        expect(result.sinks.table).deep.equal([{x: 30}]);
                        expect(result.prog.graph.adapter.es_opts.aggregations.count).equal('x');
                    });
                });

                it('optimizes count by', function() {
                    return test_utils.read({id: type}, '| reduce count() by clientip')
                    .then(function(result) {
                        expect(result.sinks.table).deep.equal([
                            {clientip: '83.149.9.216', count: 23},
                            {clientip: '93.114.45.13', count: 6},
                            {clientip: '24.236.252.67', count: 1}
                        ]);

                        var aggregations = result.prog.graph.adapter.es_opts.aggregations;

                        expect(aggregations.count).equal('count');
                        expect(aggregations.grouping).deep.equal(['clientip']);
                        expect(aggregations.es_aggr).deep.equal({
                            clientip: {
                                terms: {
                                    field: 'clientip',
                                    size: 1000000
                                }
                            }
                        });
                    });
                });

                it('optimizes reduce -every count()', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: count()');
                });

                it('optimizes reduce -every count() by', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: count() by clientip');
                });

                it('optimizes reduce -every -on count()', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: -on :0.5s: count()');
                });

                it('optimizes reduce -every -on count() by', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: -on :0.3s: count() by clientip');
                });

                it('-on a non-duration moment', function() {
                    var on = new Date().toISOString();
                    var extra = util.format('| reduce -every :s: -on :%s: count() by clientip', on);
                    return test_utils.check_optimization(start, end, type, extra);
                });

                it('optimizes non-count reducers', function() {
                    var extra = '| reduce sum(bytes), avg(bytes), max(bytes), min(bytes), count_unique(bytes)';
                    return test_utils.check_optimization(start, end, type, extra);
                });

                it('optimizes non-count reducers -every', function() {
                    var extra = '| reduce -every :s: sum(bytes), avg(bytes), max(bytes), min(bytes), count_unique(bytes)';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: format_juttle_result_like_es
                    });
                });

                it('optimizes non-count reducers -every -on', function() {
                    var extra = '| reduce -every :s: -on :0.2s: sum(bytes), avg(bytes), max(bytes), min(bytes), count_unique(bytes)';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: format_juttle_result_like_es
                    });
                });

                it('optimizes no reducers', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce by clientip', {
                        massage: sortBy('clientip')
                    });
                });

                it('optimizes no reducers 2 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce by clientip, bytes', {
                        massage: sortBy('clientip', 'bytes')
                    });
                });

                it('optimizes no reducers 3 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce by clientip, bytes, httpversion', {
                        massage: sortBy('bytes', 'httpversion', 'clientip')
                    });
                });

                it('optimizes count() with 2 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce count() by clientip, bytes', {
                        massage: sortBy('bytes', 'clientip')
                    });
                });

                it('optimizes count() with 3 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce count() by clientip, bytes, httpversion', {
                        massage: sortBy('bytes', 'clientip', 'httpversion')
                    });
                });

                it('optimizes avg() with 2 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce avg(bytes) by clientip, httpversion', {
                        massage: sortBy('clientip', 'httpversion')
                    });
                });

                it('optimizes avg() with 3 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce avg(bytes) by clientip, httpversion, source_type', {
                        massage: sortBy('clientip', 'httpversion', 'source_type')
                    });
                });

                it('optimizes reduce -every no reducers 2 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: by clientip, bytes', {
                        massage: sortBy('time', 'clientip', 'bytes')
                    });
                });

                it('optimizes reduce -every no reducers 3 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: by clientip, bytes, httpversion', {
                        massage: sortBy('time', 'bytes', 'httpversion', 'clientip')
                    });
                });

                it('optimizes count() -every with 2 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: count() by clientip, bytes', {
                        massage: sortBy('time', 'bytes', 'clientip')
                    });
                });

                it('optimizes count() -every with 3 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: count() by clientip, bytes, httpversion', {
                        massage: sortBy('time', 'bytes', 'clientip', 'httpversion')
                    });
                });

                it('optimizes avg() -every with 2 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: avg(bytes) by clientip, httpversion', {
                        massage: sortBy('time', 'clientip', 'httpversion')
                    });
                });

                it('optimizes avg() -every with 3 groups', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: avg(bytes) by clientip, httpversion, source_type', {
                        massage: sortBy('time', 'clientip', 'httpversion', 'source_type')
                    });
                });

                it('optimizes no reducers -every', function() {
                    return test_utils.check_optimization(start, end, type, '| reduce -every :s: by clientip');
                });

                it('optimizes no reducers -every -on', function() {
                    var extra = '| reduce -every :s: -on :0.4s: by clientip';
                    return test_utils.check_optimization(start, end, type, extra);
                });

                it('optimizes reduce -every with a lot of buckets', function() {
                    var extra = '| reduce -every :h: count()';
                    return test_utils.check_optimization('10 years ago', 'now', type, extra);
                });

                it('optimizes reduce -every with a lot of buckets and nontrivial time filter', function() {
                    var start = '2014-09-17T00:00:00.000Z';
                    var end = '2014-09-17T14:13:46.000Z';
                    var extra = '| reduce -every :s: count()';
                    return test_utils.check_optimization(start, end, type, extra);
                });

                it('optimizes reduce -every with a lot of buckets and nontrivial time filter', function() {
                    var start = '2014-09-17T00:00:00.000Z';
                    var end = '2014-09-20T00:00:00.000Z';
                    var extra = '| reduce -every :day: count()';
                    return test_utils.check_optimization(start, end, type, extra);
                });

                it('optimizes reduce -every -on with a lot of buckets', function() {
                    var extra = '| reduce -every :h: -on :5m: count()';
                    return test_utils.check_optimization('10 years ago', 'now', type, extra);
                });

                it('doesn\'t optimize reduce -acc true', function() {
                    return test_utils.read({from: start, to: end, id: type}, '| reduce -every :s: -acc true by clientip')
                        .then(function(result) {
                            expect(result.prog.graph.adapter.es_opts.limit).equal(undefined);
                            expect(result.prog.graph.adapter.es_opts.aggregations).equal(undefined);
                        });
                });

                it('optimizes reduce by with missing field last', function() {
                    var extra = '| reduce count() by clientip, garbage';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: sortBy('clientip')
                    });
                });

                it('optimizes reduce by with missing field first', function() {
                    var extra = '| reduce count() by garbage, clientip';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: sortBy('clientip')
                    });
                });

                it('optimizes reduce -every by missing field last', function() {
                    var extra = '| reduce -every :s: count() by clientip, garbage';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: sortBy('clientip')
                    });
                });

                it('optimizes reduce -every by missing field first', function() {
                    var extra = '| reduce -every :s: count() by garbage, clientip';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: sortBy('clientip')
                    });
                });

                it('optimizes reduce -every by missing field last and avg', function() {
                    var extra = '| reduce -every :s: avg(bytes) by clientip, garbage';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: sortBy('clientip')
                    });
                });

                it('optimizes reduce -every by missing field first and avg', function() {
                    var extra = '| reduce -every :s: avg(bytes) by garbage, clientip';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: sortBy('clientip')
                    });
                });
            });

            describe('calendars', function() {
                var msInDay = 1000 * 60 * 60 * 24;
                var msInYear = msInDay * 365;
                var daily_points_for_two_years = test_utils.generate_sample_data({
                    count: 365 * 2,
                    start: new Date(Date.now() - msInYear * 2),
                    interval: msInDay,
                    tags: {
                        name: ['name1', 'name2', 'name3'],
                        tag: ['tag1', 'tag2', 'tag3', 'tag4']
                    }
                });

                var data_start = daily_points_for_two_years[0].time;
                var now = 'now';
                var index = test_utils.test_id + 'calendar';

                before(function() {
                    return Promise.map(daily_points_for_two_years, function(pt) {
                        return retry(function() {
                            return test_utils.write([pt], {
                                index: index,
                                id: type
                            })
                            .then(function(result) {
                                expect(result.errors).deep.equal([]);
                            });
                        });
                    }, {concurrency: 10})
                    .then(function() {
                        return test_utils.verify_import(daily_points_for_two_years, type, index);
                    });
                });

                after(function() {
                    return test_utils.clear_data(type, index);
                });

                it('optimizes reduce -every :month: count()', function() {
                    var extra = '| reduce -every :month: count()';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('1M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('month');
                    });
                });

                it('optimizes reduce -every :year: count()', function() {
                    var extra = '| reduce -every :year: count()';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('12M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('year');
                    });
                });

                it('does not optimize reduce -every :2 month: count()', function() {
                    var extra = '| reduce -every :2 month: count()';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs).equal(undefined);
                    });
                });

                it('does not optimizes reduce -every :2 year: count()', function() {
                    var extra = '| reduce -every :2 year: count()';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs).equal(undefined);
                    });
                });

                it('optimizes reduce -every :month: avg(value)', function() {
                    var extra = '| reduce -every :month: avg(value)';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('1M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('month');
                    });
                });

                it('optimizes reduce -every :year: avg(value)', function() {
                    var extra = '| reduce -every :year: avg(value)';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('12M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('year');
                    });
                });

                it('optimizes reduce -every :month: -on :day 2: avg(value)', function() {
                    var extra = '| reduce -every :month: -on :day 2: avg(value)';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('1M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('month');
                    });
                });

                it('optimizes reduce -every :year: -on :day 2: avg(value)', function() {
                    var extra = '| reduce -every :year: -on :day 2: avg(value)';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('12M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('year');
                    });
                });

                it('optimizes reduce -every :month: -on :2014-01-27: avg(value)', function() {
                    var extra = '| reduce -every :month: -on :2014-01-27: avg(value)';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('1M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('month');
                    });
                });

                it('optimizes reduce -every :year: -on :2014-01-27: avg(value)', function() {
                    var extra = '| reduce -every :year: -on :2014-01-27: avg(value)';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs.reduce_every).equal('12M');
                        expect(aggrs.es_aggr.time.date_histogram.interval).equal('year');
                    });
                });

                it('does not optimize reduce -every :year: -on :month: avg(value)', function() {
                    var extra = '| reduce -every :year: -on :month: avg(value)';
                    return test_utils.check_optimization(data_start, now, type, extra, {
                        index: index
                    })
                    .then(function(optimized_graph) {
                        var aggrs = optimized_graph.prog.graph.es_opts.aggregations;
                        expect(aggrs).equal(undefined);
                    });
                });
            });
        });
    });
});
