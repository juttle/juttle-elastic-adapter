var Promise = require('bluebird');

var Elastic = require('./elastic');
function ESBackend(config, JuttleRuntime) {
    Elastic.init(config);

    return {
        name: 'elastic',
        read: require('./read')(JuttleRuntime),
        write: require('./write')(JuttleRuntime),
        optimizer: require('./optimize')
    };
}

module.exports = ESBackend;
