var _ = require('underscore');
var expect = require('chai').expect;

var test_utils = require('./elastic-test-utils');
var points = test_utils.generate_sample_data({
    tags: {
        nest: [{tag: 'a', arr: ['a']}, {tag: 'b', arr: ['b']}],
        array: [['a', 'b', 'c'], ['d', 'e', 'f'], ['a', 'e', 'g']]
    }
});

var modes = test_utils.modes;

modes.forEach(function(mode) {
    describe(`nested objects -- ${mode}`, function() {
        this.timeout(30000);
        before(function() {
            return test_utils.write(points, {id: mode})
                .then(function() {
                    return test_utils.verify_import(points, mode);
                });
        });

        after(function() {
            return test_utils.clear_data(mode);
        });

        it('read', function() {
            return test_utils.read({id: mode})
                .then(function(result) {
                    expect(result.sinks.table).deep.equal(points);
                });
        });

        // depends on https://github.com/juttle/juttle/issues/320
        it.skip('object property access filter', function() {
            return test_utils.read({id: mode}, 'nest["tag"] = "a"')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    var expected = points.filter(function(pt) {
                        return pt.nest.tag === 'a';
                    });

                    expect(result.sinks.table).deep.equal(expected);
                });
        });

        // depends on https://github.com/juttle/juttle/issues/320
        it.skip('object filter', function() {
            return test_utils.read({id: mode}, 'nest = {tag: "a"}')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    var expected = points.filter(function(pt) {
                        return pt.nest.tag === 'a';
                    });

                    expect(result.sinks.table).deep.equal(expected);
                });
        });

        it('optimized reduce by object: warns', function() {
            return test_utils.read({id: mode}, '| reduce count() by nest')
                .then(function(result) {
                    var expected = [{nest: null, count: points.length}];
                    expect(result.sinks.table).deep.equal(expected);
                    expect(result.warnings).match(/"nest" is an object/);
                });
        });

        // inconsistent with Juttle which can't reduce by object field, but useful
        it('optimized reduce by object field', function() {
            return test_utils.read({id: mode}, '| reduce count() by "nest.tag"')
                .then(function(result) {
                    var counts = _.countBy(points, function(pt) {
                        return pt.nest.tag;
                    });

                    var expected = _.chain(counts).map(function(count, tag) {
                        return {count: count, 'nest.tag': tag};
                    }).sortBy('nest.tag').value();

                    var received = _.sortBy(result.sinks.table, 'nest.tag');

                    expect(expected).deep.equal(received);
                });
        });

        // depends on https://github.com/juttle/juttle/issues/320
        it.skip('array literal filter', function() {
            return test_utils.read({id: mode}, 'array = ["a", "b", "c"]')
                .then(function(result) {
                    var expected = points.filter(function(pt) {
                        return pt.array[0] === 'a' && pt.array[1] === 'b' &&
                            pt.array[2] === 'c';
                    });

                    expect(result.sinks.table).deep.equal(expected);
                });
        });
    });
});
