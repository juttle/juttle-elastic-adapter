'use strict';

var retry = require('bluebird-retry');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');
var points = [require('./apache-sample')[0]];
var index_name = 'analysis_warning_test' + test_utils.test_id;

var modes = test_utils.modes;
modes.forEach(function(mode) {
    describe(`analyzed field warnings -- ${mode}`, function() {
        before(function() {
            return test_utils.create_index(mode, index_name)
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

        it('warns if you reduce by an analyzed field', function() {
            return retry(function() {
                return test_utils.read({id: mode, index: index_name}, '| reduce by request')
                    .then(function(result) {
                        var warning = `field "request" is analyzed in type ` +
                            `"event" in index "${index_name}", results may be unexpected`;
                        expect(result.warnings).deep.equal([warning]);
                        expect(result.errors).deep.equal([]);
                    });
            });
        });

        it('warns if you filter on an analyzed field', function() {
            return retry(function() {
                return test_utils.read({id: mode, index: index_name}, 'request = "/presentations/logstash-monitorama-2013/images/sad-medic.png"')
                    .then(function(result) {
                        var warning = `field "request" is analyzed in type ` +
                            `"event" in index "${index_name}", results may be unexpected`;
                        expect(result.warnings).deep.equal([warning]);
                        expect(result.errors).deep.equal([]);
                        expect(result.sinks.table).deep.equal([]);
                    });
            });
        });
    });
});
