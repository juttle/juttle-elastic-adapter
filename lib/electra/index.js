var Base = require('extendable-base');
var url = require('url');
var Inserter = require('./insert');

var query = require('./query');
var aggregation = require('./aggregation');
var common = require('./query-common');

var Electra = Base.extend({
    initialize: function(config) {
        this.es_url = url.format({
            protocol: 'http',
            hostname: config.address,
            port: config.port
        });

        query.init(config);
        aggregation.init(config);
        common.init(this.es_url);
    },

    get_inserter: function() {
        return new Inserter(this.es_url);
    },

    fetcher: query.fetcher,
});

module.exports = Electra;
