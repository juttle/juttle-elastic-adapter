var Promise = require('bluebird');

var Elastic = require('./elastic');
function ESAdapter(config, Juttle) {
    Elastic.init(config);

    return {
        name: 'elastic',
        read: require('./read'),
        write: require('./write'),
        optimizer: require('./optimize')
    };
}

module.exports = ESAdapter;
