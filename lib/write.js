var elastic = require('./elastic');
var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');

var Write = Juttle.proc.sink.extend({
    procName: 'elastic_write',
    initialize: function(options) {
        var self = this;
        this.eofs = 0;
        this.id = options.id;
        this.type = options.type || utils.default_config_property_for_id(this.id, 'writeType');
        this.in_progress_writes = 0;
        this.timeField = options.timeField || utils.default_config_property_for_id(this.id, 'timeField');
        this._setup_index(options);
    },
    process: function(points) {
        var self = this;
        var inserter = elastic.get_inserter(this.id, this.index, this.type, this.interval, this.timeField);
        for (var i = 0; i < points.length; i++) {
            try {
                inserter.push(points[i]);
            } catch (err) {
                this.trigger('error', new Error('invalid point: ' + JSON.stringify(points[i]) + ' because of ' + err.message));
            }
        }
        self.in_progress_writes++;
        inserter.end()
        .then(function(errors) {
            errors.forEach(function(err) {
                var message = 'point rejected by Elasticsearch due to: ' + err;
                self.trigger('error', new Error(message));
            });
        })
        .catch(function(err) {
            var message = err.message === 'No Living connections' ? 'Failed to connect to Elasticsearch' : err.message;
            self.trigger('error', new Error('insertion failed: ' + message));
        })
        .finally(function() {
            self.in_progress_writes--;
            self._maybe_done();
        });
    },

    _setup_index: function(options) {
        var index = options.index || utils.default_config_property_for_id(this.id, 'writeIndex');
        this.interval = options.indexInterval || utils.default_config_property_for_id(this.id, 'indexInterval');
        utils.ensure_valid_interval(this.interval);

        var star_index = index.indexOf('*');
        if (star_index !== -1 && star_index !== index.length - 1) {
            throw new Error('cannot write to index pattern: ' + index);
        }

        if (this.interval !== 'none') {
            if (star_index !== index.length - 1) {
                throw new Error('with indexInterval, index must end in *');
            }

            // drop the star, insert.js wants only the prefix
            index = index.substring(0, index.length - 1);
        } else if (star_index !== -1) {
            throw new Error('index for write with interval "none" cannot contain *');
        }

        this.index = index;
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
