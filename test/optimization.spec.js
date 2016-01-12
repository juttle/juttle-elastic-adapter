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

            it('optimizes head', function() {
                return test_utils.read({id: type}, '| head 3')
                .then(function(result) {
                    var expected = points.slice(0, 3);
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                    expect(result.prog.graph.es_opts.limit).equal(3);
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
                    expect(result.prog.graph.es_opts.limit).equal(2);
                });
            });

            it('optimizes head with tag filter', function() {
                return test_utils.read({id: type}, 'clientip = "93.114.45.13" | head 2')
                .then(function(result) {
                    var expected = points.filter(function(pt) {
                        return pt.clientip === '93.114.45.13';
                    }).slice(0, 2);

                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'bytes');
                    expect(result.prog.graph.es_opts.limit).equal(2);
                });
            });

            it('optimizes head 0 (returns nothing)', function() {
                return test_utils.read({id: type}, '| head 0')
                .then(function(result) {
                    expect(result.sinks.table).deep.equal([]);
                    expect(result.prog.graph.es_opts.limit).equal(0);
                });
            });

            describe('reduce', function() {
                it('optimizes count', function() {
                    return test_utils.read({id: type}, '| reduce count()')
                    .then(function(result) {
                        var first_node = result.prog.graph.head[0];
                        expect(first_node.procName).equal('elastic_read');

                        var second_node = first_node.out_.default[0].proc;
                        expect(second_node.procName).equal('view');

                        expect(result.sinks.table).deep.equal([{count: 30}]);
                        expect(result.prog.graph.es_opts.aggregations.count).equal('count');
                    });
                });

                it('optimizes count with a named reducer', function() {
                    return test_utils.read({id: type}, '| reduce x=count()')
                    .then(function(result) {
                        expect(result.sinks.table).deep.equal([{x: 30}]);
                        expect(result.prog.graph.es_opts.aggregations.count).equal('x');
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
                        massage: function(array) {
                            return _.sortBy(array, 'clientip');
                        }
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

                // travis runs outdated ES so we can't test this in the CI
                it.skip('optimizes reduce -every -on with a lot of buckets', function() {
                    var extra = '| reduce -every :h: -on :5m: count()';
                    return test_utils.check_optimization('10 years ago', 'now', type, extra);
                });

                it('doesn\'t optimize reduce -acc true', function() {
                    return test_utils.read({from: start, to: end, id: type}, '| reduce -every :s: -acc true by clientip')
                        .then(function(result) {
                            expect(result.prog.graph.es_opts.limit).equal(undefined);
                            expect(result.prog.graph.es_opts.aggregations).equal(undefined);
                        });
                });

                // travis doesn't have aggkey.groovy so we can't test this in the CI
                it.skip('optimizes reduce by with missing fields', function() {
                    var extra = '| reduce count() by clientip, garbage';
                    return test_utils.check_optimization(start, end, type, extra, {
                        massage: function(array) {
                            return _.sortBy(array, 'clientip');
                        }
                    });
                });
            });
        });
    });
});
