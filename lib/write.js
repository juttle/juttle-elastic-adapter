'use strict';

var _ = require('underscore');

var elastic = require('./elastic');
var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');
var AdapterWrite = require('juttle/lib/runtime/adapter-write');
var Promise = require('bluebird');

class WriteElastic extends AdapterWrite {
    constructor(options, params) {
        super(options, params);
        this.options = options;
        this._set_or_default('id', 'timeField', 'idField', 'concurrency', 'chunkSize');
        this.type = options.type || this._default_config('writeType');
        this._setup_index(options);
        this.write_promise = Promise.resolve();
    }

    _set_or_default() {
        for (var i = 0; i < arguments.length; i++) {
            var property = arguments[i];
            this[property] = this.options[property] || this._default_config(property);
        }
    }

    write(points) {
        this.write_promise = this.write_promise.then(() => {
            return Promise.map(_.range(points.length / this.chunkSize), (n) => {
                var chunk = points.splice(0, this.chunkSize);

                return this._write(chunk)
                .then((errors) => {
                    errors.forEach((err) => {
                        var message = 'point rejected by Elasticsearch due to: ' + err;
                        this.trigger('error', new Error(message));
                    });
                })
                .catch((err) => {
                    var message = err.message === 'No Living connections' ? 'Failed to connect to Elasticsearch' : err.message;
                    this.trigger('error', new Error('insertion failed: ' + message));
                });
            }, {concurrency: this.concurrency});
        });
    }

    _write(chunk) {
        var inserter_options = _.pick(this, 'id', 'index', 'type', 'interval', 'timeField', 'idField');
        inserter_options.warn = this.warn.bind(this);
        var inserter = elastic.get_inserter(inserter_options);
        for (var i = 0; i < chunk.length; i++) {
            try {
                inserter.push(chunk[i]);
            } catch (err) {
                this.trigger('error', new Error('invalid point: ' + JSON.stringify(chunk[i]) + ' because of ' + err.message));
            }
        }

        return inserter.end();
    }

    warn(message) {
        this.trigger('warning', new Error(message));
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

    eof() {
        return this.write_promise;
    }
}

module.exports = WriteElastic;
