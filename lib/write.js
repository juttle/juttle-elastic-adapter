var elastic = require('./elastic');
var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');

var Write = Juttle.proc.sink.extend({
    procName: 'elastic_write',
    initialize: function(options) {
        var self = this;
        this.eofs = 0;
        this.id = options.id;
        this.in_progress_writes = 0;
        this.prefix = options.index_prefix || utils.default_prefix_for_id(this.id);
    },
    process: function(points) {
        var self = this;
        var inserter = elastic.get_inserter(this.id, this.prefix);
        for (var i = 0; i < points.length; i++) {
            try {
                inserter.push(points[i]);
            } catch (err) {
                this.trigger('error', new Error('invalid point: ' + JSON.stringify(points[i]) + ' because of ' + err.message));
            }
        }
        self.in_progress_writes++;
        inserter.end()
        .catch(function(err) {
            var message = err.message === 'No Living connections' ? 'Failed to connect to Elasticsearch' : err.message;
            self.trigger('error', new Error('insertion failed: ' + message));
        })
        .finally(function() {
            self.in_progress_writes--;
            self._maybe_done();
        });
    },
    _maybe_done: function() {
        if (this.eofs === this.ins.length && this.in_progress_writes === 0) {
            this.emit_eof();
            this.done();
        }
    },
    eof: function(from) {
        this.eofs++;
        this._maybe_done();
    }
});

module.exports = Write;
