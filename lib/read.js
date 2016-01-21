var _ = require('underscore');

var FilterESCompiler = require('./filter-es-compiler');
var juttle_utils = require('juttle/lib/runtime').utils;
var elastic = require('./elastic');
var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');
var common = require('./query-common');

var Read = Juttle.proc.source.extend({
    procName: 'elastic_read',

    initialize: function(options, params) {
        this.id = options.id;
        this.timeField = options.timeField || this._default_config('timeField');
        this.type = options.type || this._default_config('readType');
        this.idField = options.idField || this._default_config('idField');

        this._setup_time_filter(options);
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
    },

    _default_config: function(property) {
        return utils.default_config_property_for_id(this.id, property);
    },

    _setup_index: function(options) {
        this.index = options.index || this._default_config('readIndex');
        this.interval = options.indexInterval || this._default_config('indexInterval');
        utils.ensure_valid_interval(this.interval);
        if (this.interval !== 'none') {
            if (_.last(this.index) !== '*') {
                throw new Error('with indexInterval, index must end in *');
            }

            var prefix = this.index.substring(0, this.index.length - 1);
            this.index = common.get_indices(this.from, this.to, prefix, this.interval);
        }
    },

    _setup_time_filter: function(options) {
        this.now = this.program.now;

        if (options.from && options.to) {
            this.from = options.from;
            this.to = options.to;
        } else if (options.last) {
            this.to = this.now;
            this.from = this.to.subtract(options.last);
        } else {
            throw new Error('-from/-to or -last time filter required');
        }
    },

    start: function() {
        var self = this;
        return elastic.fetcher(this.id, this.es_filter, this.from, this.to, this.es_opts)
            .then(function(fetcher) {
                function loop() {
                    return fetcher()
                        .then(function(result) {
                            if (result.points.length > 0) {
                                self.emit(juttle_utils.toNative(result.points));
                            }
                            if (result.eof) {
                                return self.emit_eof();
                            } else {
                                return loop();
                            }
                        });
                }

                return loop();
            })
            .catch(function(err) {
                self.trigger('error', err);
                self.emit_eof();
            });
    }
});

module.exports = Read;
