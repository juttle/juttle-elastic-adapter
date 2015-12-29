var _ = require('underscore');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var points = require('./apache-sample');
var expected_points = points.map(function(pt) {
    var new_pt = _.clone(pt);
    new_pt.time = new Date(new_pt.time).toISOString();
    return new_pt;
});

// Register the adapter
require('./elastic-test-utils');

var logstash_template = require('./logstash-template');

describe('logstash schema', function() {
    this.timeout(300000);
    before(function() {
        return test_utils.put_template('logstash', logstash_template)
            .then(function() {
                var points_to_write = points.map(function(point) {
                    var point_to_write = _.clone(point);
                    point_to_write.time /= 1000;
                    return point_to_write;
                });

                return test_utils.write(points_to_write, 'local', 'logstash-');
            })
            .then(function(res) {
                expect(res.errors).deep.equal([]);
                return test_utils.verify_import(points, 'local', 'logstash-*');
            });
    });

    after(function() {
        return test_utils.clear_data('local', 'logstash-*');
    });

    it('reduce by', function() {
        return test_utils.read_all('local', '| reduce sum(bytes) by clientip', 'logstash-')
            .then(function(result) {
                var expected = [
                    { clientip: '24.236.252.67', sum: 3638 },
                    { clientip: '83.149.9.216', sum: 4379454 },
                    { clientip: '93.114.45.13', sum: 86839 }
                ];
                var received = _.sortBy(result.sinks.table, 'clientip');
                expect(result.errors).deep.equal([]);
                expect(received).deep.equal(expected);
            });
    });

    it('cardinality', function() {
        return test_utils.read_all('local', '| reduce count_unique(clientip)', 'logstash-')
            .then(function(result) {
                expect(result.errors).deep.equal([]);
                expect(result.sinks.table).deep.equal([{count_unique: 3}]);
            });
    });

    it('value_count', function() {
        return test_utils.read_all('local', '| reduce count(clientip)', 'logstash-')
            .then(function(result) {
                expect(result.errors).deep.equal([]);
                expect(result.sinks.table).deep.equal([{count: 30}]);
            });
    });
});
