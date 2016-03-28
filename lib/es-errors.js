'use strict';

var util = require('util');

class MissingIndex extends Error {
    constructor(message) {
        super(message);
        this.name = 'MissingIndex';
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, this.constructor);
        }
    }
}

class NewIndex extends Error {
    constructor(message) {
        super(message);
        this.name = 'NewIndex';
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, this.constructor);
        }
    }
}

class MissingField extends Error{
    constructor(field) {
        super(field);
        this.name = 'MissingField';
        this.field = field;
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, this.constructor);
        }
    }
}

var _MISSING_FIELD_PATTERN = /No field found for \[([^\]]+)\] in mapping/;
var _GROOVY_PATTERN = /groovy_script_execution_exception/;
var _MISSING_INDEX_PATTERN = /index_not_found_exception/;
var _OLD_MISSING_INDEX_PATTERN = /IndexMissingException/; // pre-2.0
var _WINDOW_OVERFLOW_PATTERN = /Result window is too large, from \+ size must be less than or equal to: \[([^\]]+)\]/;
var _NEW_INDEX_PATTERN = /all shards failed/;

function categorize_error(error) {
    var str = error.message;

    if (str === 'No Living connections') {
        return new Error('Failed to connect to Elasticsearch');
    }

    if (str.match(_MISSING_INDEX_PATTERN) || str.match(_OLD_MISSING_INDEX_PATTERN)) {
        return new MissingIndex();
    }

    if (str.match(_NEW_INDEX_PATTERN)) {
        return new NewIndex();
    }

    var groovy = str.match(_GROOVY_PATTERN);
    if (groovy) {
        var cause = JSON.parse(error.response).error.failed_shards[0].reason.caused_by.reason;
        var match = cause.match(_MISSING_FIELD_PATTERN);
        if (match) {
            return new MissingField(match[1]);
        }
    }

    var overflow_match = str.match(_WINDOW_OVERFLOW_PATTERN);
    if (overflow_match) {
        var m = 'Tried to read more than %s points with the same timestamp, ' +
            'increase the max_result_window setting on the relevant indices to read more';
        var message = util.format(m, overflow_match[1]);
        return new Error(message);
    }

    return null;
}

module.exports = {
    categorize_error: categorize_error,
    MissingField: MissingField,
    MissingIndex: MissingIndex,
    NewIndex: NewIndex,
};
