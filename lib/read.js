'use strict';

var _ = require('underscore');

/* global JuttleAdapterAPI */
var AdapterRead = JuttleAdapterAPI.AdapterRead;
var JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;
var toNative = JuttleAdapterAPI.runtime.toNative;

var FilterESCompiler = require('./filter-es-compiler');
var elastic = require('./elastic');
var utils = require('./utils');
var common = require('./query-common');

class ReadElastic extends AdapterRead {
    constructor(options, params) {
        super(options, params);
        this._validate_options(options);
        this.id = options.id;
        this.timeField = options.timeField || this._default_config('timeField');
        this.type = options.type || this._default_config('readType');
        this.idField = options.idField || this._default_config('idField');
        this.fetchSize = options.fetchSize || this._default_config('fetchSize');

        this._setup_index(options);
        this.filtered_fields = [];
        var filter_ast = params.filter_ast;
        if (filter_ast) {
            var filter_es_compiler = new FilterESCompiler();
            var result = filter_es_compiler.compile(filter_ast);
            this.es_filter = result.filter;
            this.filtered_fields = result.filtered_fields;
        }

        this._validate_filters();

        var optimization_info = params && params.optimization_info || {};

        this.head_or_tail_limit = this._calculate_limit(optimization_info.limit);
        this.es_opts = {
            type: this.type,
            interval: this.interval,
            timeField: this.timeField,
            idField: this.idField,
            direction: optimization_info.type === 'tail' ? 'desc' : 'asc',
            aggregations: optimization_info.aggregations
        };

        if (options.deep_paging_limit) {
            this.es_opts.deep_paging_limit = options.deep_paging_limit;
        }

        this.executed_queries = [];
        this.points_emitted_so_far = 0;
    }

    static allowedOptions() {
        var elastic_options = ['id', 'timeField', 'type', 'idField', 'fetchSize',
            'es_opts', 'deep_paging_limit', 'index', 'interval', 'optimize', 'indexInterval',
            'queueSize'];
        return AdapterRead.commonOptions().concat(elastic_options);
    }

    _validate_options(options) {
        if (!options.from && !options.to) {
            throw new Error('-from, -to, or -last must be specified');
        }
    }

    _validate_filters() {
        if (_.contains(this.filtered_fields, this.idField)) {
            throw new Error('cannot filter on idField');
        }
    }

    periodicLiveRead() {
        return true;
    }

    defaultTimeRange() {
        return {
            from: new JuttleMoment(),
            to: new JuttleMoment()
        };
    }

    _default_config(property) {
        return utils.default_config_property_for_id(this.id, property);
    }

    _check_options_vs_mapping(indices) {
        if (this.checked_options_vs_mapping_already) { return Promise.resolve(); }
        this.checked_options_vs_mapping_already = true;

        return elastic.mapping_for_id(this.id, indices, this.type)
            .then((mapping) => {
                this._warn_if_no_index_or_type(mapping);
                _.each(mapping, (mapping_object, index) => {
                    var index_mapping = mapping_object.mappings || {};
                    _.each(index_mapping, (props_object, type) => {
                        if (type === '_default_') { return; }
                        var properties = props_object.properties;
                        this._warn_if_missing(properties, this.timeField, index, type);

                        this.filtered_fields.forEach((field) => {
                            this._warn_if_analyzed(properties, field, index, type);
                            this._warn_if_missing(properties, field, index, type);
                        });

                        var reduce_by_fields = (this.es_opts.aggregations &&
                            this.es_opts.aggregations.grouping) || [];
                        reduce_by_fields.forEach((field) => {
                            this._warn_if_analyzed(properties, field, index, type);
                            this._warn_if_missing(properties, field, index, type);
                            this._warn_if_object(properties, field, index, type);
                        });
                    });
                });
            });
    }

    _warn_if_no_index_or_type(mapping) {
        if (_.size(mapping) === 0) {
            this.warn(`index/type combination "${this.index}"/"${this.type}" not found`);
        }
    }

    _warn_if_analyzed(properties, field, index, type) {
        if (properties && properties.hasOwnProperty(field)) {
            if (properties[field].index !== 'not_analyzed') {
                this.warn(`field "${field}" is analyzed in type "${type}" in index ` +
                    `"${index}", results may be unexpected`);
            }
        }
    }

    _warn_if_missing(properties, field, index, type) {
        function _nested_access(object, fields) {
            while (object && fields.length) {
                var field = fields.shift();
                if (object.hasOwnProperty(field)) {
                    object = object[field].properties || object[field].fields || object[field];
                } else {
                    return null;
                }
            }

            return object;
        }

        var fields = field.split('.');

        if (properties && !_nested_access(properties, fields)) {
            this.warn(`index "${index}" has no known property ` +
                `"${field}" for type "${type}"`);
        }
    }

    _warn_if_object(properties, field, index, type) {
        if (properties && properties[field] && properties[field].properties) {
            this.warn(`field "${field}" is an object in type "${type}" in index ` +
                `"${index}", results may be unexpected`);
        }
    }

    _setup_index(options) {
        this.index = options.index || this._default_config('readIndex');
        this.interval = options.indexInterval || this._default_config('indexInterval');
        utils.ensure_valid_interval(this.interval);
        if (this.interval !== 'none') {
            if (_.last(this.index) !== '*') {
                throw new Error('with indexInterval, index must end in *');
            } else {
                this.index = this.index.substring(0, this.index.length - 1);
            }
        }
    }

    _stash_executed_query(result) {
        // for tests, store the Elasticsearch query we executed
        if (result.executed_query) {
            this.executed_queries.push(result.executed_query);
        }
    }

    _calculate_limit(limit) {
        if (typeof limit === 'number') {
            return limit;
        }

        return Infinity;
    }

    _calculate_query_size(single_fetch_limit) {
        var points_left_to_emit = this.head_or_tail_limit - this.points_emitted_so_far;

        return Math.min(points_left_to_emit, this.fetchSize, single_fetch_limit);
    }

    warn(message) {
        this.trigger('warning', new Error(message));
    }

    read(from, to, limit, state) {
        var indices = common.get_indices(from, to, this.index, this.interval);
        return this._check_options_vs_mapping(indices)
            .then(() => {
                this.es_opts.indices = indices;
                if (!this.fetcher) {
                    this.fetcher = elastic.fetcher(this.id, this.es_filter, from, to, this.es_opts);
                }

                var num_points = this._calculate_query_size(limit);

                return this.fetcher(num_points);
            })
            .then((result) => {
                this._stash_executed_query(result);
                this.fetcher = result.eof ? null : this.fetcher;
                this.points_emitted_so_far += result.points.length;
                var eof = result.eof || (this.points_emitted_so_far === this.head_or_tail_limit);
                return {
                    points: toNative(result.points),
                    readEnd: eof ? to : null
                };
            });
    }
}

module.exports = ReadElastic;
