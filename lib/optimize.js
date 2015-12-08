var _ = require('underscore');

var reducers = require('juttle/lib/runtime/reducers').reducers;
var aggregation = require('./aggregation');

function _getFieldName(node) {
    return node.expression.value;
}

function _getReducerName(node) {
    return node.name.name;
}

function _isSimpleFieldReference(node) {
    return node.type === 'UnaryExpression' &&
        node.operator === '*' &&
        node.expression.type === 'StringLiteral';
}

function _isReducerCall(node) {
    return (node.type === 'ReducerCall');
}

function _empty_reducer_result(expr) {
    return [_getFieldName(expr.left), reducers[_getReducerName(expr.right)].id];
}

function reduce_expr_is_optimizable(expr) {
    if (!_isSimpleFieldReference(expr.left)) {
        throw new Error("Found unexpected reduce lhs while optimizing: " + expr.left);
    }

    if (expr.left.expression.value === 'time') {
        return false;
    }

    if (!_isReducerCall(expr.right)) {
        return false;
    }

    return true;
}

var optimizer = {
    optimize_head: function(read, head, graph, optimization_info) {
        if (optimization_info.type && optimization_info.type !== 'head') {
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
    optimize_reduce: function(read, reduce, graph, optimization_info) {
        if (optimization_info && optimization_info.type) {
            return false;
        }
        var groupby = graph.node_get_option(reduce, 'groupby');
        var grouped = groupby && groupby.length > 0;
        if (grouped && groupby.indexOf('time') !== -1) {
            return false;
        }

        var forget = graph.node_get_option(reduce, 'forget');
        if (grouped && forget === false) {
            return false;
        }

        var aggrs = {};
        var count_name, every, on;

        for (var i = 0; i < reduce.exprs.length; i++) {
            var expr = reduce.exprs[i];
            if (!reduce_expr_is_optimizable(expr)) {
                return false;
            }

            var target = _getFieldName(expr.left);
            var reducer = _getReducerName(expr.right);
            if (reducer === 'count' && expr.right.arguments.length === 0 && !count_name) {
                count_name = target;
                continue;
            } else {
                return false;
            }
        }

        var empty_result = _.object(reduce.exprs.map(_empty_reducer_result));

        if (grouped) {
            aggrs = aggregation.make_bucket_agg(groupby, aggrs);
        }

        if (graph.node_has_option(reduce, 'every')) {
            every = graph.node_get_option(reduce, 'every');
            on = graph.node_get_option(reduce, 'on');
            if (every.is_calendar()) {
                return false;
            }
        }

        _.extend(optimization_info, {
            type: 'reduce',
            aggregations: {
                es_aggr: aggrs,
                count: count_name,
                empty_result: empty_result,
                grouping: groupby,
                reduce_every: every,
                reduce_on: on
            }
        });

        graph.remove_node(reduce);

        return true;
    }
};

module.exports = optimizer;
