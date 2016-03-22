var Promise = require('bluebird');
var _ = require('underscore');

/* global JuttleAdapterAPI */
var JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;
var toNative = JuttleAdapterAPI.runtime.toNative;
var common = require('./query-common');
var utils = require('./utils');
var aggregation = require('./aggregation');
var es_errors = require('./es-errors');
var logger = JuttleAdapterAPI.getLogger('elastic-query');

var DEFAULT_FETCH_SIZE = 10000;
var DEFAULT_DEEP_PAGING_LIMIT = 200000;

function make_body(filter, from, to, direction, tm_filter, size, timeField) {
    if (!tm_filter) {
        tm_filter = _time_filter(from, to, timeField);
    }
    var sort_params = {
        unmapped_type: 'date',
        order: direction
    };

    var sort = {};
    sort[timeField] = sort_params;

    return {
        from: 0,
        size: size,
        sort: sort,
        query: {
            filtered: {
                filter: filter ? { and: [ tm_filter, filter ] } : tm_filter
            }
        }
    };
}

function null_fetcher() {
    return Promise.resolve({
        points: [],
        executed_query: null,
        eof: true
    });
}

function fetcher(client, filter, query_start, query_end, options) {
    var indices = options.indices;
    var type = options.type;
    var bridge_offset = 0;
    var timeField = options.timeField;
    var idField = options.idField;
    var direction = options.direction || 'asc';

    var deep_paging_limit = options.deep_paging_limit || DEFAULT_DEEP_PAGING_LIMIT;
    var should_execute_bridge_fetch = false;
    var last_seen_timestamp;

    return query_fetcher;

    function query_fetcher(size) {
        return Promise.try(function() {
            if (should_execute_bridge_fetch) {
                return bridge_fetch(size);
            } else {
                var body = make_body(filter, query_start, query_end, direction, null, size, timeField);

                return common.search(client, indices, type, body)
                    .then(function(result) {
                        var processed = points_and_eof_from_es_result(result, body);
                        return drop_last_time_stamp_and_maybe_bridge_fetch(processed, size);
                    });
            }
        });
    }

    function points_and_eof_from_es_result(result, request_body) {
        if (!result.hits || !result.hits.hits) {
            throw new Error('unexpected ES response: ' + JSON.stringify(result));
        }

        var search_results = result.hits.hits;
        var all_points_read = (search_results.length + request_body.from === result.hits.total);
        var full_page_read = (search_results.length === request_body.size);

        if (!all_points_read && !full_page_read) {
            var message = 'fetch error - results count mismatch , expected ' +
                request_body.size + ', got ' + search_results.length;
            throw new Error(message);
        }

        var points = utils.pointsFromESDocs(search_results, timeField, idField);

        if (direction === 'desc') {
            points.reverse();
        }

        return {
            total: result.hits.total,
            executed_query: _.extend({indices: indices}, request_body), // for tests
            points: points,
            eof: all_points_read
        };
    }

    // if more than fetchSize events have the same timestamp, we do
    // a "bridge fetch" which pages through all the points with that timestamp
    function bridge_fetch(size) {
        var bridge_time = new JuttleMoment(last_seen_timestamp);

        var time_filter = {term: {}};
        time_filter.term[timeField] = bridge_time.valueOf();

        var body = make_body(filter, null, null, direction, time_filter, size, timeField);
        body.from = bridge_offset;

        if (bridge_offset > deep_paging_limit) {
            return Promise.reject(new Error('Cannot fetch more than ' + deep_paging_limit + ' points with the same timestamp'));
        }

        return common.search(client, indices, type, body)
            .then(function(result) {
                var processed = points_and_eof_from_es_result(result, body);
                bridge_offset += body.size;
                if (processed.eof) {
                    // end of the bridge fetch, not necessarily end of the query
                    // (if it really is the end of the query then the non-brige-path
                    // will fetch one more empty batch and return eof)
                    should_execute_bridge_fetch = false;
                    query_start = bridge_time.add(JuttleMoment.duration(1, 'milliseconds'));
                    processed.eof = false;
                }

                return processed;
            });
    }

    // takes the output of points_and_eof_from_es_result and removes the points
    // with the last timestamp, triggering a bridge fetch if all the points
    // are simultaneous. This makes sure the next query, which starts
    // at the last timestamp, won't return any duplicates
    function drop_last_time_stamp_and_maybe_bridge_fetch(processed, size) {
        if (processed.eof) { return processed; }
        var last = _.last(processed.points);
        last_seen_timestamp = last && last.time;

        var non_simultaneous_points = processed.points.filter(function(pt) {
            return pt.time !== last_seen_timestamp;
        });

        if (non_simultaneous_points.length === 0 && processed.points.length !== 0) {
            should_execute_bridge_fetch = true;
            bridge_offset = 0;
            return bridge_fetch(size);
        } else {
            query_start = new JuttleMoment(last_seen_timestamp);
            processed.points = non_simultaneous_points;

            return processed;
        }
    }
}

function aggregation_fetcher(client, filter, query_start, query_end, options) {
    var indices = options.indices;
    var type = options.type;
    var aggregations = options.aggregations;
    var reduce_every = aggregations.reduce_every ? JuttleMoment.duration(aggregations.reduce_every) : null;
    var timeField = options.timeField;
    var buckets = get_buckets();
    logger.debug('buckets:', JSON.stringify(buckets));

    return fetcher;

    function fetcher() {
        if (buckets.length === 0) {
            return null_fetcher();
        }

        var from = buckets.shift();
        var to = buckets[0];
        if (!from || !to) {
            throw new Error('aggregation fetcher didn\'t have from/to');
        }

        var query_body = make_body(filter, from, to, 'asc', null, 0, timeField);

        query_body.aggregations = aggregations.es_aggr;

        return common.search(client, indices, type, query_body)
        .then(function(response) {
            var aggr_points = toNative(aggregation.points_from_es_aggr_resp(response, aggregations));

            // If points are timeful we make the timestamps epsilon moments,
            // just like they would be if they came out of reduce -every
            // and add an interval since we performed a date histogram
            // aggregation which puts all the points a bucket behind
            if (reduce_every) {
                if (to === query_end) {
                    to = JuttleMoment.quantize(to, reduce_every).add(reduce_every);
                }

                aggr_points.forEach(function format_aggregated_time(pt) {
                    pt.time = pt.time.add(reduce_every);
                    pt.time.epsilon = true;
                });
            }

            return {
                eof: buckets.length === 1, // last bucket is exclusive
                executed_query: _.extend({indices: indices}, query_body), // for tests
                points: aggr_points
            };
        })
        .catch(es_errors.MissingField, function(ex) {
            // our scripted aggregation will fail if we try to group
            // by a non-existent field.  when this happens, remove
            // the non-existent field and re-run the aggregation.
            var orig = _.clone(aggregations);
            aggregations = aggregation.remove_field(aggregations, ex.name);
            buckets.unshift(from);
            return fetcher()
            .then((result) => {
                aggregations = orig;
                return result;
            });
        });
    }

    function get_batch_offset_as_duration(every_duration) {
        try {
            return JuttleMoment.duration(aggregations.reduce_on);
        } catch(err) {
            // translate a non-duration -on into the equivalent duration
            // e.g. if we're doing -every :hour: -on :2015-03-16T18:32:00.000Z:
            // then that's equivalent to -every :hour: -on :32 minutes:
            var moment = new JuttleMoment(aggregations.reduce_on);
            return JuttleMoment.subtract(moment, JuttleMoment.quantize(moment, every_duration));
        }
    }

    function get_buckets() {
        var BATCHES_IN_A_BUCKET = 1000;
        // batched aggregations are done 1000 batches at a time for flow control
        // unbatched aggregations are just executed as though they were single-batch batch queries
        var buckets = [query_start];
        if (reduce_every) {
            var zeroth_bucket = JuttleMoment.quantize(query_start, reduce_every);
            var last_bucket = JuttleMoment.quantize(query_end, reduce_every);

            if (aggregations.reduce_on) {
                var offset = get_batch_offset_as_duration(reduce_every);
                zeroth_bucket = zeroth_bucket.add(offset);
                buckets.push(zeroth_bucket);
                last_bucket = last_bucket.add(offset);
            }

            var intermediate_bucket = zeroth_bucket;
            var buckets_delta = reduce_every.multiply(BATCHES_IN_A_BUCKET);

            while (intermediate_bucket.lt(last_bucket)) {
                var next_bucket = intermediate_bucket.add(buckets_delta);
                intermediate_bucket = JuttleMoment.min(next_bucket, last_bucket);
                buckets.push(intermediate_bucket);
            }
        }

        buckets.push(query_end);

        return buckets;
    }
}

function _time_filter(from, to, timeField) {
    var time = {};

    if (!from.isBeginning()) {
        time.gte = from.valueOf();
    }

    if (to) {
        time.lt = to.valueOf();
    }

    var filter = {range: {}};
    filter.range[timeField] = time;

    return filter;
}

function init(config) {
    DEFAULT_FETCH_SIZE = config.fetchSize || DEFAULT_FETCH_SIZE;
    DEFAULT_DEEP_PAGING_LIMIT = config.deep_paging_limit || DEFAULT_DEEP_PAGING_LIMIT;
}

module.exports = {
    init: init,
    DEFAULT_FETCH_SIZE: DEFAULT_FETCH_SIZE,
    fetcher: fetcher,
    aggregation_fetcher: aggregation_fetcher
};
