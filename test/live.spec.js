var _ = require('underscore');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');

// Register the adapter
require('./elastic-test-utils');

var modes = test_utils.modes;

function generate_live_data() {
    return test_utils.generate_sample_data({
        start: Date.now() + 2000
    });
}

describe('elastic source', function() {
    this.timeout(300000);
    modes.forEach(function(type) {
        describe('live reads -- ' + type, function() {
            afterEach(function() {
                return test_utils.clear_data(type);
            });

            function test_live(points_to_write, points_to_expect) {
                points_to_expect = points_to_expect || points_to_write;
                var last_time = new Date(_.last(points_to_write).time).getTime();
                var deactivateAfter = last_time - Date.now() + 5000;
                var options = {id: type, from: 0, to: 'end', lag: '2s'};

                var read = test_utils.read(options, '', deactivateAfter)
                .then(function(result) {
                    expect(result.sinks.table).deep.equal(points_to_expect);
                });

                return test_utils.write(points_to_write, {id: type})
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    return test_utils.verify_import(points_to_write, type);
                })
                .then(function() {
                    return read;
                });
            }

            it('reads live points', function() {
                var points = generate_live_data();

                return test_live(points);
            });

            it('superquery', function() {
                var historical = test_utils.generate_sample_data({
                    tags: {name: ['historical']}
                });

                return test_utils.write(historical, {id: type})
                    .then(function(result) {
                        expect(result.errors).deep.equal([]);
                        return test_utils.verify_import(historical, type);
                    })
                    .then(function() {
                        var live = generate_live_data();
                        return test_live(live, historical.concat(live));
                    });
            });
        });
    });
});
