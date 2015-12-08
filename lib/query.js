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

function make_body(filter, from, to, direction, tm_filter, size) {
    if (!tm_filter) {
        tm_filter = _logstash_time_filter(from, to);
    }
    var sort_params = {
        unmapped_type: 'date',
        order: direction
    };

    var sort = {
        '@timestamp': sort_params
    };

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

function fetcher(es_url, filter, query_start, query_end, options) {
    var bridge_offset = 0;
    var points_emitted_so_far = 0;
    var limit = options.limit;
    var direction = options.direction || 'asc';

    var fetch_size = options.fetch_size || DEFAULT_FETCH_SIZE;
    var deep_paging_limit = options.deep_paging_limit || DEFAULT_DEEP_PAGING_LIMIT;

    var url = common.build_logstash_url(es_url, query_start, query_end);
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

        var points = utils.pointsFromESDocs(result.hits.hits);

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

        var logstash_time_filter = {
            term: {
                '@timestamp': bridge_time.valueOf()
            }
        };

        var size = _get_body_size();
        var body = make_body(filter, null, null, direction, logstash_time_filter, size);
        body.from = bridge_offset;

        if (bridge_offset > deep_paging_limit) {
            return Promise.reject(new Error('Cannot fetch more than ' + deep_paging_limit + ' points with the same timestamp'));
        }

        return common.execute(url, body)
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
                var body = make_body(filter, query_start, query_end, direction, null, size);

                return common.execute(url, body)
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

function aggregation_fetcher(es_url, filter, query_start, query_end, options) {
    var url = common.build_logstash_url(es_url, query_start, query_end);
    var aggregations = options.aggregations;
    var reduce_every = aggregations.reduce_every;

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
            var duration = new JuttleMoment.duration(reduce_every);
            var zeroth_bucket = JuttleMoment.quantize(query_start, duration);
            var last_bucket = JuttleMoment.quantize(query_end, duration);

            if (aggregations.reduce_on) {
                var offset = get_batch_offset_as_duration(duration);
                zeroth_bucket = JuttleMoment.add(zeroth_bucket, offset);
                buckets.push(zeroth_bucket);
                last_bucket = JuttleMoment.add(last_bucket, offset);
            }

            var intermediate_bucket = JuttleMoment.add(zeroth_bucket, duration);

            while (intermediate_bucket.lte(last_bucket)) {
                buckets.push(intermediate_bucket);
                intermediate_bucket = JuttleMoment.add(intermediate_bucket, duration);
            }
        }

        buckets.push(query_end);

        return buckets;
    }

    var buckets = get_buckets();
    var has_emitted;
    var buffered_empties = [];

    function fetcher() {
        var grouped = aggregations.grouping && aggregations.grouping.length > 0;

        if (buckets.length === 0) {
            return Promise.resolve({
                points: [],
                eof: true
            });
        }

        var from = buckets.shift();
        var to = buckets[0];
        if (!from || !to) {
            throw new Error('aggregation fetcher didn\'t have from/to');
        }

        var query_body = make_body(filter, from, to, 'asc', null, 0);

        query_body.aggregations = aggregations.es_aggr;
        return common.execute(url, query_body)
        .then(function(response) {
            var total = response.hits && response.hits.total;
            if (total === 0 && !grouped) {
                if (has_emitted) {
                    var pt = {};
                    if (reduce_every) {
                        pt.time = to;
                    }

                    buffered_empties.push(_.extend(pt, aggregations.empty_result));
                }

                return {
                    points: [],
                    eof: buckets.length === 1
                };
            }

            var aggr_points = juttle_utils.toNative(aggregation.values_from_es_aggr_resp(response, aggregations));

            if (reduce_every) {
                aggr_points.forEach(function(pt) {
                    if (to === query_end) {
                        var duration = new JuttleMoment.duration(reduce_every);
                        to = JuttleMoment.quantize(to, duration).add(duration);
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

            if (grouped) {
                var limit = aggregations.es_aggr.group.terms.size;
                if (aggr_points.length === limit || (response.aggregations &&
                    response.aggregations.group &&
                    response.aggregations.group.buckets &&
                    response.aggregations.group.buckets.length === limit)) {
                }
            }

            var all_points = buffered_empties.concat(aggr_points);
            buffered_empties = [];
            has_emitted = true;
            return {
                eof: buckets.length === 1, // last bucket is exclusive
                points: all_points
            };
        })
        .catch(es_errors.MissingField, function(ex) {
            // our scripted aggregation will fail if we try to group
            // by a non-existent field.  when this happens, remove
            // the non-existent field and re-run the aggregation.
            aggregations = aggregation.remove_field(aggregations, ex.name);
            return fetcher();
        });
    }

    return Promise.resolve(fetcher);
}

function _logstash_time_filter(from, to) {
    var time = {};

    if (!from.isBeginning()) {
        time.gte = from.valueOf();
    }

    if (to) {
        time.lt = to.valueOf();
    }

    var filter = {
        range: {
            '@timestamp' : time
        }
    };
    return filter;
}

function init(config) {
    DEFAULT_FETCH_SIZE = config.fetch_size || 10000;
    DEFAULT_DEEP_PAGING_LIMIT = config.deep_paging_limit || 200000;
}

module.exports = {
    init: init,
    fetcher: fetcher,
    aggregation_fetcher: aggregation_fetcher
};
