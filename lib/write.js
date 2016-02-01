'use strict';

var _ = require('underscore');

var elastic = require('./elastic');
var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');
var AdapterWrite = require('juttle/lib/runtime/adapter-write');

class WriteElastic extends AdapterWrite {
    constructor(options, params) {
        super(options, params);
        this.eofs = 0;
        this.id = options.id;
        this.type = options.type || this._default_config('writeType');
        this.in_progress_writes = 0;
        this.timeField = options.timeField || this._default_config('timeField');
        this.idField = options.idField || this._default_config('idField');
        this._setup_index(options);
    }

    write(points) {
        var inserter_options = _.pick(this, 'id', 'index', 'type', 'interval', 'timeField', 'idField');
        var inserter = elastic.get_inserter(inserter_options);
        for (var i = 0; i < points.length; i++) {
            try {
                inserter.push(points[i]);
            } catch (err) {
                this.trigger('error', new Error('invalid point: ' + JSON.stringify(points[i]) + ' because of ' + err.message));
            }
        }
        this.in_progress_writes++;
        inserter.end()
        .then((errors) => {
            errors.forEach((err) => {
                var message = 'point rejected by Elasticsearch due to: ' + err;
                this.trigger('error', new Error(message));
            });
        })
        .catch((err) => {
            var message = err.message === 'No Living connections' ? 'Failed to connect to Elasticsearch' : err.message;
            this.trigger('error', new Error('insertion failed: ' + message));
        })
        .finally(() => {
            this.in_progress_writes--;
            if (this.in_progress_writes === 0 && this.done) {
                this.done();
            }
        });
    }

    _default_config(property) {
        return utils.default_config_property_for_id(this.id, property);
    }

    _setup_index(options) {
        var index = options.index || this._default_config('writeIndex');
        this.interval = options.indexInterval || this._default_config('indexInterval');
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
    }

    eof(from) {
        if (this.in_progress_writes > 0) {
            return new Promise((resolve, reject) => {
                this.done = resolve;
            });
        }
    }
}

module.exports = WriteElastic;
