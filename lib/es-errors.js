
var Base = require('extendable-base');

var MissingField = Base.inherits(Error, {
    initialize: function(name) {
        this.name = name;
    }
});

var MissingIndex = Base.inherits(Error, {});

var _MISSING_FIELD_PATTERN = /No field found for \[([^\]]+)\] in mapping/;
var _GROOVY_PATTERN = /groovy_script_execution_exception/;
var _MISSING_INDEX_PATTERN = /index_not_found_exception/;
var _OLD_MISSING_INDEX_PATTERN = /IndexMissingException/; // pre-2.0

function categorize_error(error) {
    var str = error.message;

    if (str === 'No Living connections') {
        return new Error('Failed to connect to Elasticsearch');
    }

    var groovy = str.match(_GROOVY_PATTERN);
    if (groovy) {
        var cause = JSON.parse(error.response).error.failed_shards[0].reason.caused_by.reason;
        var match = cause.match(_MISSING_FIELD_PATTERN);
        if (match) {
            return new MissingField(match[1]);
        }
    }

    if (str.match(_MISSING_INDEX_PATTERN) || str.match(_OLD_MISSING_INDEX_PATTERN)) {
        return new MissingIndex();
    }

    return null;
}

module.exports = {
    categorize_error: categorize_error,
    MissingField: MissingField,
    MissingIndex: MissingIndex,
};
