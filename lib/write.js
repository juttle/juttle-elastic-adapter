'use strict';

var _ = require('underscore');

var elastic = require('./elastic');
var utils = require('./utils');
var util = require('util');
var AdapterWrite = require('juttle/lib/runtime/adapter-write');
var ConcurrencyMaster = require('concurrency-master');
var logger = require('juttle/lib/logger').getLogger('elastic-write');
var Inserter = require('./insert');

class WriteElastic extends AdapterWrite {
    constructor(options, params) {
        super(options, params);
        this.options = options;
        this._set_or_default('id', 'timeField', 'idField', 'concurrency', 'chunkSize');
        this.type = options.type || this._default_config('writeType');
        this._setup_index(options);
        this.points = [];
        this.concurrency_master = new ConcurrencyMaster(this.concurrency);

        this.chunks_written = 0; // for tests
    }

    allowedOptions() {
        return ['type', 'index', 'interval', 'id', 'timeField', 'idField',
            'concurrency', 'chunkSize'];
    }

    _set_or_default() {
        for (var i = 0; i < arguments.length; i++) {
            var property = arguments[i];
            this[property] = this.options[property] || this._default_config(property);
        }
    }

    write(pts) {
        this.points = this.points.concat(pts);
        if (this.points.length >= this.chunkSize) {
            while (this.points.length > 0) {
                var chunk = this.points.splice(0, this.chunkSize);

                this._write(chunk);
            }
        }
    }

    _write(chunk) {
        var self = this;
        this.chunks_written++;
        var inserter_options = _.pick(this, 'id', 'index', 'type', 'interval', 'timeField', 'idField');
        inserter_options.warn = this.warn.bind(this);
        var client = elastic.client_for_id(this.id);
        var inserter = new Inserter(client, inserter_options);
        for (var i = 0; i < chunk.length; i++) {
            try {
                inserter.push(chunk[i]);
            } catch (err) {
                var message = util.format('invalid point: %s because of %s',
                    JSON.stringify(chunk[i]), err.message);
                logger.debug(message);
                this.trigger('error', new Error(message));
            }
        }

        var execute_write = function() {
            return inserter.end().then((errors) => {
                errors.forEach((err) => {
                    var message = 'point rejected by Elasticsearch due to: ' + err;
                    logger.debug(message);
                    self.trigger('error', new Error(message));
                });
            })
            .catch((err) => {
                logger.debug('error during insert', err.stack || err);
                var message = err.message === 'No Living connections' ? 'Failed to connect to Elasticsearch' : err.message;
                self.trigger('error', new Error('insertion failed: ' + message));
            });
        };

        this.concurrency_master.add(execute_write);
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
        if (this.points.length > 0) {
            this._write(this.points);
        }

        return this.concurrency_master.wait();
    }
}

module.exports = WriteElastic;
