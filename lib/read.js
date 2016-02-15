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

        this._setup_index(options);
        this.filtered_fields = [];
        var filter_ast = params.filter_ast;
        if (filter_ast) {
            var filter_es_compiler = new FilterESCompiler({timeField: this.timeField});
            var result = filter_es_compiler.compile(filter_ast);
            this.es_filter = result.filter;
            this.filtered_fields = result.filtered_fields;
        }

        this._validate_filters();

        var optimization_info = params && params.optimization_info || {};

        this.es_opts = {
            indices: this.index,
            type: this.type,
            interval: this.interval,
            timeField: this.timeField,
            idField: this.idField,
            limit: optimization_info.limit,
            direction: optimization_info.type === 'tail' ? 'desc' : 'asc',
            aggregations: optimization_info.aggregations
        };

        if (options.fetch_size) {
            this.es_opts.fetch_size = options.fetch_size;
        }

        if (options.deep_paging_limit) {
            this.es_opts.deep_paging_limit = options.deep_paging_limit;
        }

        this.executed_queries = [];
    }

    static allowedOptions() {
        var elastic_options = ['id', 'timeField', 'type', 'idField', 'fetch_size',
            'es_opts', 'deep_paging_limit', 'index', 'interval', 'optimize', 'indexInterval'];
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

    _check_options_vs_mapping() {
        if (this.checked_options_vs_mapping_already) { return Promise.resolve(); }
        this.checked_options_vs_mapping_already = true;

        return elastic.mapping_for_id(this.id, this.index, this.type)
            .then((mapping) => {
                this._warn_if_no_index_or_type(mapping);
                _.each(mapping, (mapping_object, index) => {
                    var index_mapping = mapping_object.mappings || {};
                    _.each(index_mapping, (props_object, type) => {
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
        if (properties && !properties.hasOwnProperty(field)) {
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
        if (typeof this.es_opts.limit === 'number') {
            return Math.min(limit, this.es_opts.limit);
        }

        return limit;
    }

    warn(message) {
        this.trigger('warning', new Error(message));
    }

    read(from, to, limit, state) {
        return this._check_options_vs_mapping()
            .then(() => {
                this.es_opts.indices = common.get_indices(from, to, this.index, this.interval);
                this.es_opts.limit = this._calculate_limit(limit);
                if (!this.fetcher) {
                    this.fetcher = elastic.fetcher(this.id, this.es_filter, from, to, this.es_opts);
                }

                return this.fetcher();
            })
            .then((result) => {
                this._stash_executed_query(result);
                this.fetcher = result.eof ? null : this.fetcher;
                return {
                    points: toNative(result.points),
                    readEnd: result.eof ? to : null
                };
            });
    }
}

module.exports = ReadElastic;
