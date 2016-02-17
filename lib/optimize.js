var _ = require('underscore');

/* global JuttleAdapterAPI */
var aggregation = require('./aggregation');

var logger = JuttleAdapterAPI.getLogger('elastic-optimize');
var reducerDefaultValue = JuttleAdapterAPI.runtime.reducerDefaultValue;
var ALLOWED_REDUCE_OPTIONS = ['every', 'forget', 'groupby', 'on'];
var DEFAULT_FETCH_SIZE = require('./query').DEFAULT_FETCH_SIZE;
var utils = require('./utils');

function _getFieldName(node) {
    return node.name;
}

function _getReducerName(node) {
    return node.name.name;
}

function _isReducerCall(node) {
    return (node.type === 'ReducerCall');
}

function _empty_reducer_result(expr) {
    return [_getFieldName(expr.left), reducerDefaultValue(_getReducerName(expr.right))];
}

function reduce_expr_is_optimizable(expr) {
    if (expr.left.type !== 'Field') {
        logger.debug('optimization aborting -- unexpected reduce lhs:', expr.left);
        return false;
    }

    if (expr.left.name === 'time') {
        logger.debug('optimization aborting -- cannot optimize reduce on time');
        return false;
    }

    if (!_isReducerCall(expr.right)) {
        logger.debug('optimization aborting -- cannot optimize non-reducer-call node', expr.right);
        return false;
    }

    return true;
}

var optimizer = {
    optimize_head: function(read, head, graph, optimization_info) {
        if (optimization_info.type && optimization_info.type !== 'head') {
            logger.debug('optimization aborting -- cannot append head optimization to prior', optimization_info.type, 'optimization');
            return false;
        }

        var limit = graph.node_get_option(head, 'arg');

        if (optimization_info.hasOwnProperty('limit')) {
            limit = Math.min(limit, optimization_info.limit);
        }

        optimization_info.type = 'head';
        optimization_info.limit = limit;
        return true;
    },
    optimize_tail: function(read, tail, graph, optimization_info) {
        if (optimization_info.type && optimization_info.type !== 'tail') {
            logger.debug('optimization aborting -- cannot append tail optimization to prior', optimization_info.type, 'optimization');
            return false;
        }

        var limit = graph.node_get_option(tail, 'arg');

        if (optimization_info.hasOwnProperty('limit')) {
            limit = Math.min(limit, optimization_info.limit);
        }

        var read_fetch_size = graph.node_get_option(read, 'fetch_size') || DEFAULT_FETCH_SIZE;
        if (limit > read_fetch_size) {
            logger.debug('optimization aborting -- cannot optimize tail limit over fetch size');
            return false;
        }

        optimization_info.type = 'tail';
        optimization_info.limit = limit;
        return true;
    },
    optimize_reduce: function(read, reduce, graph, optimization_info) {
        if (!graph.node_contains_only_options(reduce, ALLOWED_REDUCE_OPTIONS)) {
            logger.debug('optimization aborting -- cannot optimize reduce with options', graph.node_get_option_names(reduce));
            return false;
        }
        if (optimization_info && optimization_info.type) {
            logger.debug('optimization aborting -- cannot append reduce optimization to prior', optimization_info.type, 'optimization');
            return false;
        }
        var groupby = graph.node_get_option(reduce, 'groupby');
        var grouped = groupby && groupby.length > 0;
        if (grouped && groupby.indexOf('time') !== -1) {
            logger.debug('optimization aborting -- cannot optimize group by time');
            return false;
        }

        var id_field = graph.node_get_option(read, 'idField');
        if (_.contains(groupby, id_field)) {
            logger.debug('optimization aborting -- cannot optimize reduce by -idField');
            return false;
        }

        var forget = graph.node_get_option(reduce, 'forget');
        if (forget === false) {
            logger.debug('optimization aborting -- cannot optimize -forget false');
            return false;
        }

        var aggrs = {};
        var aggr_names = [];
        var count_name, every, on;

        for (var i = 0; i < reduce.exprs.length; i++) {
            var expr = reduce.exprs[i];
            if (!reduce_expr_is_optimizable(expr)) {
                return false;
            }

            var target = _getFieldName(expr.left);
            var reducer = _getReducerName(expr.right);
            if (reducer === 'count' && expr.right.arguments.length === 0 && !count_name) {
                logger.debug('found simple count() reducer, optimizing');
                count_name = target;
                continue;
            }

            if (expr.right.arguments.length !== 1) {
                logger.debug('optimization aborting -- cannot optimize any reducer with', expr.right.arguments.length, 'arguments');
                return false;
            }

            aggr_names.push(target);
            var argument_object = expr.right.arguments[0];
            if (argument_object.type !== 'StringLiteral') {
                logger.debug('optimization aborting -- found unexpected reducer argument:', JSON.stringify(argument_object));
                return false;
            }

            var arg = argument_object.value;
            var aggr = aggregation.make_reducer_agg(target, reducer, arg);
            if (aggr === null) {
                logger.debug('optimization aborting -- unoptimizable reducer', JSON.stringify(reducer, null, 2));
                return false;
            }

            aggrs[aggr[0]] = aggr[1];
        }

        var empty_result = _.object(reduce.exprs.map(_empty_reducer_result));

        if (graph.node_has_option(reduce, 'every')) {
            every = graph.node_get_option(reduce, 'every');
            on = graph.node_get_option(reduce, 'on');
            var time_field = graph.node_get_option(read, 'timeField') || utils.DEFAULT_CONFIG.timeField;

            aggrs = aggregation.make_datehist_agg(every, on, time_field, aggrs);

            if (aggrs === null) { return false; }
        }

        if (grouped) {
            aggrs = aggregation.make_bucket_agg(groupby, aggrs);
        }

        _.extend(optimization_info, {
            type: 'reduce',
            aggregations: {
                es_aggr: aggrs,
                aggr_names: aggr_names,
                count: count_name,
                empty_result: empty_result,
                empty_fields: [],
                grouping: groupby || [],
                reduce_every: every,
                reduce_on: on
            }
        });

        logger.debug('optimization succeeded', JSON.stringify(optimization_info, null, 2));

        return true;
    }
};

module.exports = optimizer;
