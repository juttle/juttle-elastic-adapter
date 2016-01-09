var Promise = require('bluebird');
var _ = require('underscore');

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var common = require('./query-common');
var juttle_utils = require('juttle/lib/runtime').utils;
var utils = require('./utils');
var aggregation = require('./aggregation');
var es_errors = require('./es-errors');

var DEFAULT_FETCH_SIZE;
var DEFAULT_DEEP_PAGING_LIMIT;
var DEFAULT_OPTIMIZED_REQUEST_CONCURRENCY;

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

function fetcher(client, filter, query_start, query_end, options) {
    var indices = options.indices;
    var type = options.type;
    var bridge_offset = 0;
    var points_emitted_so_far = 0;
    var limit = options.limit;
    var timeField = options.timeField;
    var idField = options.idField;
    var direction = options.direction || 'asc';

    var fetch_size = options.fetch_size || DEFAULT_FETCH_SIZE;
    var deep_paging_limit = options.deep_paging_limit || DEFAULT_DEEP_PAGING_LIMIT;

    function process_query_result(result, request_body) {
        var eof = false;
        if (result.hits.hits.length + request_body.from === result.hits.total ||
            (limit && (result.hits.hits.length === limit - points_emitted_so_far))) {
            eof = true;
        }
        else if (result.hits.hits.length !== request_body.size ) {
            var message = 'fetch error - results count mismatch , expected ' +
                request_body.size + ', got ' + result.hits.hits.length;
            throw new Error(message);
        }

        var points = utils.pointsFromESDocs(result.hits.hits, timeField, idField);

        if (direction === 'desc') {
            points.reverse();
        }

        return {
            total: result.hits.total,
            points: points,
            eof: eof
        };
    }

    var should_execute_bridge_fetch = false;
    var last_seen_timestamp;

    function _get_body_size() {
        if (typeof limit === 'number' && limit === limit) {
            return Math.min(fetch_size, limit - points_emitted_so_far);
        } else {
            return fetch_size;
        }
    }

    // if more than fetch_size events have the same timestamp, we do
    // a "bridge fetch" which pages through all the points with that timestamp
    function bridge_fetch() {
        var bridge_date = new Date(last_seen_timestamp);
        var bridge_time = new JuttleMoment({rawDate: bridge_date});

        var time_filter = {term: {}};
        time_filter.term[timeField] = bridge_time.valueOf();

        var size = _get_body_size();
        var body = make_body(filter, null, null, direction, time_filter, size, timeField);
        body.from = bridge_offset;

        if (bridge_offset > deep_paging_limit) {
            return Promise.reject(new Error('Cannot fetch more than ' + deep_paging_limit + ' points with the same timestamp'));
        }

        return common.search(client, indices, type, body)
            .then(function(result) {
                var processed = process_query_result(result, body);
                bridge_offset += body.size;
                if (processed.eof) {
                    // end of the bridge fetch, not necessarily end of the query
                    // (if it really is the end of the query then the non-brige-path
                    // will fetch one more empty batch and return eof)
                    should_execute_bridge_fetch = false;
                    query_start = JuttleMoment.add(bridge_time, JuttleMoment.duration(1, 'milliseconds'));
                    processed.eof = false;
                }

                return processed;
            });
    }

    // takes the output of process_query_result and removes the points
    // with the last timestamp, triggering a bridge fetch if all the points
    // are simultaneous. This makes sure the next query, which starts
    // at the last timestamp, won't return any duplicates
    function drop_last_time_stamp_and_maybe_bridge_fetch(processed) {
        var last = _.last(processed.points);
        last_seen_timestamp = last && last.time;

        var filtered_points = processed.points.filter(function(pt) {
            return pt.time !== last_seen_timestamp;
        });

        if (filtered_points.length === 0 && processed.points.length !== 0) {
            should_execute_bridge_fetch = true;
            bridge_offset = 0;
            return bridge_fetch();
        } else {
            var new_from_date = new Date(last_seen_timestamp);
            query_start = new JuttleMoment({rawDate: new_from_date});
            if (!processed.eof) {
                processed.points = filtered_points;
            }

            return processed;
        }
    }

    function query_fetcher() {
        return Promise.try(function() {
            if (should_execute_bridge_fetch) {
                return bridge_fetch();
            } else {
                var size = _get_body_size();
                var body = make_body(filter, query_start, query_end, direction, null, size, timeField);

                return common.search(client, indices, type, body)
                    .then(function(result) {
                        var processed = process_query_result(result, body);
                        return drop_last_time_stamp_and_maybe_bridge_fetch(processed);
                    });
            }
        })
        .then(function(info) {
            points_emitted_so_far += info.points.length;
            return info;
        });
    }

    function null_fetcher() {
        return Promise.resolve({
            points: [],
            eof: true
        });
    }

    if (limit === 0) {
        return Promise.resolve(null_fetcher);
    } else {
        return Promise.resolve(query_fetcher);
    }
}

function aggregation_fetcher(client, filter, query_start, query_end, options) {
    var indices = options.indices;
    var type = options.type;
    var aggregations = options.aggregations;
    var reduce_every = aggregations.reduce_every ? new JuttleMoment.duration(aggregations.reduce_every) : null;
    var grouped = aggregations.grouping && aggregations.grouping.length > 0;
    var timeField = options.timeField;
    var concurrency = options.optimized_request_concurrency || DEFAULT_OPTIMIZED_REQUEST_CONCURRENCY;

    function get_batch_offset_as_duration(every_duration) {
        try {
            return new JuttleMoment.duration(aggregations.reduce_on);
        } catch(err) {
            // translate a non-duration -on into the equivalent duration
            // e.g. if we're doing -every :hour: -on :2015-03-16T18:32:00.000Z:
            // then that's equivalent to -every :hour: -on :32 minutes:
            var moment = new JuttleMoment(aggregations.reduce_on);
            return JuttleMoment.subtract(moment, JuttleMoment.quantize(moment, every_duration));
        }
    }

    function get_buckets() {
        // batched aggregations are done one batch at a time
        // to prevent ES from blowing up, so here we get the list of
        // batches we are going to do. Unbatched aggregations
        // are just executed as though they were single-batch batch queries
        var buckets = [query_start];
        if (reduce_every) {
            var zeroth_bucket = JuttleMoment.quantize(query_start, reduce_every);
            var last_bucket = JuttleMoment.quantize(query_end, reduce_every);

            if (aggregations.reduce_on) {
                var offset = get_batch_offset_as_duration(reduce_every);
                zeroth_bucket = JuttleMoment.add(zeroth_bucket, offset);
                buckets.push(zeroth_bucket);
                last_bucket = JuttleMoment.add(last_bucket, offset);
            }

            var intermediate_bucket = JuttleMoment.add(zeroth_bucket, reduce_every);

            while (intermediate_bucket.lte(last_bucket)) {
                buckets.push(intermediate_bucket);
                intermediate_bucket = JuttleMoment.add(intermediate_bucket, reduce_every);
            }
        }

        buckets.push(query_end);

        return buckets;
    }

    var buckets = get_buckets();
    var has_emitted, last_emitted_timestamp;

    // if there are empty batches that fall between a pair of full batches
    // we want to emit empty points. This function takes an array of all
    // our full batches and fills in the gaps between them with empty batch
    // points. It needs to be called after all `concurrency` requests are
    // completed so we have all the full batch information
    function fill_empties(points) {
        // Juttle only creates empty results for non-grouped aggregations
        if (grouped) {
            return points;
        }

        // Past the first set of `concurrency` queries, if at least
        // one of those batches was full, we may need to bridge a gap of
        // empties between the first set of queries and the next.
        // With this cheap trick we start filling empties from the end
        // of the last set of requests
        if (last_emitted_timestamp) {
            points.unshift({time: last_emitted_timestamp});
        }

        var length = points.length;
        for (var i = 0; i < length - 1; i++) {
            var t1 = points[i].time;
            var t2 = points[i+1].time;
            var next_expected_time = t1 && reduce_every && t1.add(reduce_every);
            if (next_expected_time && t2 && next_expected_time.milliseconds() < t2.milliseconds()) {
                points.splice(i+1, 0, _.extend({time: next_expected_time}, aggregations.empty_result));
                length++;
            }
        }

        // get rid of the fake point we added before the for-loop
        if (last_emitted_timestamp) {
            points.shift();
        }

        return points;
    }

    function fetcher() {
        if (buckets.length === 0) {
            return Promise.resolve({
                points: [],
                eof: true
            });
        }

        var current_buckets = buckets.splice(0, concurrency);
        return Promise.map(current_buckets, function fetchBucket(from, i) {
            var to = current_buckets[i+1] || buckets[0];
            if (!to) {
                return [];
            }

            var query_body = make_body(filter, from, to, 'asc', null, 0, timeField);

            query_body.aggregations = aggregations.es_aggr;

            return common.search(client, indices, type, query_body)
            .then(function(response) {
                var total = response.hits && response.hits.total;
                if (total === 0) {
                    return [];
                }

                var aggr_points = juttle_utils.toNative(aggregation.values_from_es_aggr_resp(response, aggregations));

                if (reduce_every) {
                    aggr_points.forEach(function(pt) {
                        if (to === query_end) {
                            to = JuttleMoment.quantize(to, reduce_every).add(reduce_every);
                        }
                        pt.time = to;
                    });
                }

                // If points are timeful, this aggregation included a date
                // histogram, so we make the timestamps epsilon moments, just
                // like they would be if they came out of reduce -every
                if (_.has(aggr_points[0], 'time')) {
                    _.each (aggr_points, function(pt) { pt.time.epsilon = true; });
                }

                has_emitted = true;
                return aggr_points;
            })
            .catch(es_errors.MissingField, function(ex) {
                // our scripted aggregation will fail if we try to group
                // by a non-existent field.  when this happens, remove
                // the non-existent field and re-run the aggregation.
                aggregations = aggregation.remove_field(aggregations, ex.name);
                return fetchBucket(from, i);
            });
        })
        .then(function(pointses) {
            var points = fill_empties(_.flatten(pointses));
            if (points.length > 0) {
                last_emitted_timestamp = _.last(points).time;
            }

            return {
                points: points,
                eof: buckets.length === 0
            };
        });
    }

    return Promise.resolve(fetcher);
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
    DEFAULT_FETCH_SIZE = config.fetch_size || 10000;
    DEFAULT_DEEP_PAGING_LIMIT = config.deep_paging_limit || 200000;
    DEFAULT_OPTIMIZED_REQUEST_CONCURRENCY = config.optimized_request_concurrency || 10;
}

module.exports = {
    init: init,
    fetcher: fetcher,
    aggregation_fetcher: aggregation_fetcher
};
