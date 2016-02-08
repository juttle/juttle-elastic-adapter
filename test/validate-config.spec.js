var expect = require('chai').expect;

var validate_config = require('../lib/elastic').validate_config;

function expect_failure(config, message) {
    try {
        validate_config(config);
        throw new Error('should have failed');
    } catch(err) {
        expect(err.message).equal(message);
    }
}

describe('configuration validation', function() {
    it('rejects a local instance without address', function() {
        var missing_address = {
            port: 100
        };

        expect_failure([missing_address], 'Elastic requires address and port');
    });

    it('rejects a local instance without port', function() {
        var missing_port = {
            address: 100
        };

        expect_failure([missing_port], 'Elastic requires address and port');
    });

    it('rejects an AWS instance without region', function() {
        var missing_region = {
            aws: true,
            endpoint: 100
        };

        expect_failure([missing_region], 'AWS requires region and endpoint');
    });

    it('rejects an AWS instance without endpoint', function() {
        var missing_endpoint = {
            aws: true,
            region: 100
        };

        expect_failure([missing_endpoint], 'AWS requires region and endpoint');
    });
});
