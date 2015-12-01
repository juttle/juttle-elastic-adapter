var elastic = require('./elastic');

module.exports = function Write(JuttleRuntime) {
    return JuttleRuntime.proc.sink.extend({
        procName: 'elastic_write',
        initialize: function(options) {
            var self = this;
            this.eofs = 0;
        },
        process: function(points) {
            var inserter = elastic.get_inserter();
            for (var i = 0; i < points.length; i++) {
                try {
                    inserter.push(points[i]);
                } catch (err) {
                    this.trigger('error', new Error('invalid point: ' + JSON.stringify(points[i]) + ' because of ' + err.message));
                }
            }
            inserter.end()
            .catch(function(err) {
                console.error('insertion failed: ' + err.stack);
            });
        },
        eof: function(from) {
            this.eofs++;
            if (this.eofs === this.ins.length) {
                this.emit_eof();
                this.done();
            }
        }
    });
};
