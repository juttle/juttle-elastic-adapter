var expect = require('chai').expect;
var util = require('util');
var retry = require('bluebird-retry');

var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var clear_data = require('./elastic-test-utils').clear_logstash_data;
var check_juttle = juttle_test_utils.check_juttle;
var generate = juttle_test_utils.generate_sample_data;

var Juttle = require('juttle/lib/runtime').Juttle;
var Elastic = require('../lib');

var backend = Elastic({
    address: 'localhost',
    port: 9200
}, Juttle);

Juttle.backends.register(backend.name, backend);

describe('elastic write', function() {
    this.timeout(30000);

    before(function() {
        return clear_data();
    });

    it('writes some points', function() {
        var points = generate({
            count: 100,
            start: Date.now() - 100,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                method: ['GET', 'POST', 'HEAD', 'OPTIONS', 'DELETE']
            }
        });

        var write_program = util.format('emit -points %s | writex elastic', JSON.stringify(points));

        return check_juttle({
            program: write_program
        })
        .then(function() {
            return retry(function() {
                var read_program = 'readx elastic -last :hour:';
                return check_juttle({
                    program: read_program
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal(points);
                });
            }, {max_tries: 10});
        });    
    });

    it('errors if you write a point without time', function() {
        var timeless = {value: 1, name: 'dave'};

        var write_program = util.format('emit -points %s | remove time | writex elastic', JSON.stringify([timeless]));

        return check_juttle({
            program: write_program
        })
        .then(function(result) {
            var message = util.format('invalid point: %s because of missing time', JSON.stringify(timeless));
            expect(result.errors).deep.equal([message]);
        });
    });
});
