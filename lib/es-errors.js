
var Base = require('extendable-base');

var MissingField = Base.inherits(Error, {
    initialize: function(name) {
        this.name = name;
    }
});

var WindowOverflow = Base.inherits(Error, {
    initialize: function(limit) {
        this.limit = limit;
    }
});

var MissingIndex = Base.inherits(Error, {});


var _MISSING_FIELD_PATTERN = /No field found for \[([^\]]+)\] in mapping/;
var _GROOVY_PATTERN = /groovy_script_execution_exception/;
var _MISSING_INDEX_PATTERN = /index_not_found_exception/;
var _OLD_MISSING_INDEX_PATTERN = /IndexMissingException/; // pre-2.0
var _WINDOW_OVERFLOW_PATTERN = /Result window is too large, from \+ size must be less than or equal to: \[([^\]]+)\] but was \[([^\]]+)\]./;

function categorize_error(error) {
    var str = error.message;

    if (str === 'No Living connections') {
        return new Error('Failed to connect to Elasticsearch');
    }

    var groovy = str.match(_GROOVY_PATTERN);
    var match;
    if (groovy) {
        var cause = JSON.parse(error.response).error.failed_shards[0].reason.caused_by.reason;
        match = cause.match(_MISSING_FIELD_PATTERN);
        if (match) {
            return new MissingField(match[1]);
        }
    }

    if (str.match(_MISSING_INDEX_PATTERN) || str.match(_OLD_MISSING_INDEX_PATTERN)) {
        return new MissingIndex();
    }

    match = str.match(_WINDOW_OVERFLOW_PATTERN);
    if (match) {
        return new WindowOverflow(Number(match[2]));
    }

    return null;
}

module.exports = {
    categorize_error: categorize_error,
    MissingField: MissingField,
    WindowOverflow: WindowOverflow,
    MissingIndex: MissingIndex,
};
