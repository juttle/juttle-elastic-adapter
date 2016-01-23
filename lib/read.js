'use strict';

var _ = require('underscore');

var FilterESCompiler = require('./filter-es-compiler');
var juttle_utils = require('juttle/lib/runtime').utils;
var elastic = require('./elastic');
var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');
var common = require('./query-common');
var AdapterRead = require('juttle/lib/runtime/adapter-read');

class ReadElastic extends AdapterRead {
    static get timeRequired() { return true; }

    constructor(options, params) {
        super(options, params);
        this.id = options.id;
        this.timeField = options.timeField || this._default_config('timeField');
        this.type = options.type || this._default_config('readType');
        this.idField = options.idField || this._default_config('idField');

        this._setup_index(options);
        var filter_ast = params.filter_ast;
        if (filter_ast) {
            var filter_es_compiler = new FilterESCompiler();
            var result = filter_es_compiler.compile(filter_ast);
            this.es_filter = result.filter;
        }

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

    _default_config(property) {
        return utils.default_config_property_for_id(this.id, property);
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

    read(from, to, limit, state) {
        this.es_opts.indices = common.get_indices(from, to, this.index, this.interval);
        this.es_opts.limit = this._calculate_limit(limit);
        if (!this.fetcher) {
            this.fetcher = elastic.fetcher(this.id, this.es_filter, from, to, this.es_opts);
        }

        return this.fetcher
            .then((fetcher) => {
                return fetcher()
                    .then((result) => {
                        this._stash_executed_query(result);
                        this.fetcher = result.eof ? null : this.fetcher;
                        return {
                            points: juttle_utils.toNative(result.points),
                            readEnd: result.eof ? to : null
                        };
                    });
            });
    }
}

module.exports = ReadElastic;
