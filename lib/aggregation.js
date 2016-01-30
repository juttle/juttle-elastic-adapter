var _ = require('underscore');
var MissingField = require('./es-errors').MissingField;
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var logger = require('juttle/lib/logger').getLogger('elastic-aggregation');

var MAX_AGGREGATION_BUCKETS;
var MAX_EXPENSIVE_AGGREGATION_BUCKETS;

var REDUCERS_TO_ES_AGGRS = {
    avg: 'avg',
    count: 'value_count',
    count_unique: 'cardinality',
    max: 'max',
    min: 'min',
    sum: 'sum'
};

// take a juttle reducer expression (such as x=max("value") )
// and build an elasticsearch aggregation for it.
// target_field is the LHS (x in the above example)
// reducer is the name of the reducer (max in the above example)
// arg is the argument to the reducer (value in the above example)
function make_reducer_agg(target_field, reducer, arg) {
    if (! _.has(REDUCERS_TO_ES_AGGRS, reducer)) {
        return null;
    }
    var aggr_name = REDUCERS_TO_ES_AGGRS[reducer];

    var aggr = {};
    aggr[aggr_name] = { field: arg };

    return [ target_field, aggr ];
}

function _calculate_size_for_aggregation(aggr) {
    function _is_expensive(aggr) {
        return _.some(aggr, function(aggr) {
            return _.some(aggr, function(ignored, what) {
                return what === 'cardinality';
            });
        });
    }

    return _is_expensive(aggr) ? MAX_EXPENSIVE_AGGREGATION_BUCKETS : MAX_AGGREGATION_BUCKETS;
}

// build a bucketed aggregation that groups by the given list of
// field names (and optionally includes given sub-aggregations).
// groupby is a list of field names
function make_bucket_agg(groupby, subagg) {
    var bucket_agg = {};
    groupby.forEach(function(field) {
        bucket_agg[field] = {
            terms: {
                field: field,
                size: _calculate_size_for_aggregation(subagg)
            }
        };
    });

    return _.reduceRight(bucket_agg, function nest(memo, value, key) {
        var obj = {};

        if (!_.isEmpty(memo)) {
            value.aggregations = memo;
        }

        obj[key] = value;

        return obj;
    }, subagg);
}

function es_interval_from_duration(every) {
    if (every.is_calendar()) {
        switch (every.duration._months) {
            case 1:
                return 'month';
            case 12:
                return 'year';
            default:
                logger.debug('cannot optimize calendar every besides month and year, got:', every.duration._months, 'months');
                return null;
        }
    }

    return every.milliseconds() + 'ms';
}

function es_offset_from_duration(on) {
    if (on.is_calendar()) {
        logger.debug('cannot optimize calendar on, aborting');
        return null;
    }

    return on.milliseconds() + 'ms';
}

// build an elasticsearch date histogram aggregation.
// every and on are moments corresponding to the -every
// and -on arguments to batch or reduce.  subagg is a
// dictionary of sub-aggregations to be run in each time bucket.
// (the results of previous called to make_reducer_agg() )
function make_datehist_agg(every, on, subagg) {
    var interval = es_interval_from_duration(every);
    if (interval === null) { return null; }
    var histogram = {
        field: 'time',
        interval: interval,
        min_doc_count: 0
    };

    if (on) {
        var offset = es_offset_from_duration(on);
        if (offset === null) { return null; }
        histogram.offset = offset;
    }

    return {
        time: {
            date_histogram: histogram,
            aggregations: subagg
        }
    };
}

// utility to help with the case when we group by a non-existent field.
// given a grouped aggregation `aggr`, remove the field `name` from the
// grouping but store it in a list of empty_fields.  we will insert a
// point with the value null for each of these empty fields in
// points_from_es_aggr_resp() below.  see query.js for details of how
// this is used.
function remove_field(aggr, name) {
    function _extract_date_histogram() {
        var names = aggr.grouping;
        var ptr = aggr.es_aggr;
        for (var i = 0; i < names.length; i++) {
            ptr = ptr[names[i]].aggregations;
        }

        return ptr;
    }

    var empty_fields = aggr.empty_fields.slice(0);
    empty_fields.push(name);
    var grouping = _.without(aggr.grouping, name);
    var new_es_aggr = make_bucket_agg(grouping, _extract_date_histogram());

    return {
        es_aggr: new_es_aggr,
        empty_result: aggr.empty_result,
        aggr_names: aggr.aggr_names,
        grouping: grouping,
        count: aggr.count,
        empty_fields: empty_fields
    };
}

function _time_sort(points) {
    if (_.has(points[0], 'time')) {
        return _.sortBy(points, 'time');
    }

    return points;
}

function _aggregation_values(aggregation_result, count, info) {
    var pt = {};
    var aggr_names = info.aggr_names || [];
    aggr_names.forEach(function(key) {
        if (aggregation_result.hasOwnProperty(key)) {
            pt[key] = aggregation_result[key].value;
        }
    });

    if (info.count) {
        pt[info.count] = count;
    }

    info.empty_fields.forEach(function(field) {
        pt[field] = null;
    });

    return pt;
}

function _assert_nonempty_group(bucket, field) {
    // an empty time bucket just means there were no points in that time
    // but any other empty bucket means that field isn't in ES at all
    if (!bucket.buckets.length && field !== 'time') {
        throw new MissingField(field);
    }
}

// see samples/es-grouped-aggregation-response for a response from
// read | reduce -every :3s: avg(bytes) by clientip, httpversion
// basically it's a tree where each node specifies the value of a
// grouped-by key and the leaves have the results of any reducers
// this function traverses the tree and yields its Juttle points
// (date histograms are basically "group by time")
function points_from_grouped_response(response, info) {
    var points = [];
    var is_date_histogram = _is_date_histogram(response.aggregations);
    function points_from_group(aggregation, fields, point_base) {
        var field = fields.shift();
        var bucket = aggregation[field];
        _assert_nonempty_group(bucket, field);
        if (fields.length === 0) {
            var new_points = bucket.buckets.map(function base_case(b) {
                var pt = _aggregation_values(b, b.doc_count, info);
                pt[field] = b.key_as_string || b.key;

                // Following Juttle 'reduce' behavior, we don't output points
                // for an empty batch if the reducer has 'groupby'
                var should_return_point = !(is_date_histogram && info.grouping.length && !b.doc_count);

                return should_return_point && _.extend(pt, point_base);
            });

            points = points.concat(_.compact(new_points));
        } else {
            bucket.buckets.forEach(function extend_base_and_recurse(b) {
                var base = _.clone(point_base);
                base[field] = b.key;
                points_from_group(b, _.clone(fields), base);
            });
        }
    }

    var grouping = _.clone(info.grouping);
    if (is_date_histogram) {
        grouping.push('time');
    }

    points_from_group(response.aggregations, grouping, {});

    return _time_sort(points);
}

function points_from_ungrouped_response(response, info) {
    var aggregation_result = response.aggregations;
    var count = response.hits.total;
    return [_aggregation_values(aggregation_result, count, info)];
}

function _is_date_histogram(aggregation) {
    return _.has(aggregation, 'time') || _.any(aggregation, function(value, key) {
        if (value.hasOwnProperty('buckets')) {
            return _.any(value.buckets, _is_date_histogram);
        }
    });
}

// The function below takes an ES response that includes aggregations
// and builds a corresponding Juttle result (i.e., an array of points).
function points_from_es_aggr_resp(response, info) {
    if (info.grouping.length > 0 || _is_date_histogram(response.aggregations)) {
        return points_from_grouped_response(response, info);
    } else {
        return points_from_ungrouped_response(response, info);
    }
}

function init(config) {
    MAX_AGGREGATION_BUCKETS = config.max_aggregation_buckets || 1000000;
    MAX_EXPENSIVE_AGGREGATION_BUCKETS =
        config.max_expensive_aggregation_buckets || 10000;
}

module.exports = {
    init: init,
    make_reducer_agg: make_reducer_agg,
    make_bucket_agg: make_bucket_agg,
    remove_field: remove_field,
    make_datehist_agg: make_datehist_agg,
    points_from_es_aggr_resp: points_from_es_aggr_resp
};
