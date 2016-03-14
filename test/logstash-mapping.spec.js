'use strict';

var _ = require('underscore');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');
var points = require('./apache-sample');
var LOGSTASH_MAPPING = require('./logstash-mapping');
var index_name = 'logstash_mapping_test' + test_utils.test_id;

var modes = test_utils.modes;
modes.forEach(function(mode) {
    describe(`logstash mapping -- ${mode}`, function() {
        before(function() {
            return test_utils.create_index(mode, index_name, LOGSTASH_MAPPING)
                .then(function() {
                    return test_utils.write(points, {id: mode, index: index_name});
                })
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    return test_utils.verify_import(points, mode, index_name);
                });
        });

        after(function() {
            return test_utils.clear_data(mode, index_name);
        });

        it('reads data ingested by logstash', function() {
            return test_utils.read({id: mode, index: index_name})
                .then(function(result) {
                    test_utils.check_result_vs_expected_sorting_by(result.sinks.table, points, 'bytes');
                });
        });

        it('reduce by without warnings', function() {
            return test_utils.read({id: mode, index: index_name}, '| reduce count() by "clientip.raw"')
            .then(function(result) {
                expect(result.warnings).deep.equal([]);
                var expected = _.chain(points).countBy('clientip').map(function(count, clientip) {
                    return {
                        'clientip.raw': clientip,
                        count: count
                    };
                }).value();

                test_utils.check_result_vs_expected_sorting_by(result.sinks.table, expected, 'clientip.raw');
            });
        });
    });
});
