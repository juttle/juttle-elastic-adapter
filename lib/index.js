var Promise = require('bluebird');

var FilterESCompiler = require('./electra/filter-es-compiler');
var juttle_utils = require('juttle/lib/runtime').utils;
var Electra = require('./electra');
function ESBackend(config, JuttleRuntime) {
    var electra = new Electra(config);

    var Read = JuttleRuntime.proc.base.extend({
        procName: 'elastic_read',
        sourceType: 'batch',

        initialize: function(options, params) {
            this._setup_time_filter(options);
            var filter_ast = params.filter_ast;
            if (filter_ast) {
                var filter_es_compiler = new FilterESCompiler();
                var result = filter_es_compiler.compile(filter_ast);
                this.es_filter = result.filter;
            }

            this.es_opts = {};

            if (options.fetch_size) {
                this.es_opts.fetch_size = options.fetch_size;
            }

            if (options.deep_paging_limit) {
                this.es_opts.deep_paging_limit = options.deep_paging_limit;
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
            return electra.fetcher(this.es_filter, this.from, this.to, this.es_opts)
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

    var Write = JuttleRuntime.proc.sink.extend({
        procName: 'elastic_write',
        initialize: function(options) {
            var self = this;
            this.eofs = 0;
        },
        process: function(points) {
            var inserter = electra.get_inserter();
            for (var i = 0; i < points.length; i++) {
                try {
                    inserter.push(points[i]);
                } catch (err) {
                    this.trigger('error', new Error('invalid point: ' + JSON.stringify(points[i]) + ' because of ' + err.message));
                }
            }
            inserter.end()
            .catch(function(err) {
                console.error('insertion failed: ' + err.stack);
            });
        },
        eof: function(from) {
            this.eofs++;
            if (this.eofs === this.ins.length) {
                this.emit_eof();
                this.done();
            }
        }
    });

    return {
        name: 'elastic',
        read: Read,
        write: Write
    };
}

module.exports = ESBackend;
