// Compiler that transforms filter expression AST into an Elasticsearch filter.
//
// The filter is returned in the "filter" property of the toplevel AST node.

var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;

var OPS_TO_INVERTED_OPS = {
    '==': '==',
    '!=': '!=',
    '<':  '>',
    '>':  '<',
    '<=': '>=',
    '>=': '<='
};

var OPS_TO_ES_OPS = {
    '<':  'lt',
    '>':  'gt',
    '<=': 'lte',
    '>=': 'gte'
};

var match_all_filter = { match_all : {} };

var FilterESCompiler = ASTVisitor.extend({
    initialize: function(options) {
        options = options || {};
        this.skipField = options.skipField;
    },

    compile: function(node) {
        return this.visit(node);
    },

    visitNullLiteral: function(node) {
        return null;
    },

    visitBooleanLiteral: function(node) {
        return node.value;
    },

    visitNumericLiteral: function(node) {
        return node.value;
    },

    visitInfinityLiteral: function(node) {
        return node.negative ? -Infinity : Infinity;
    },

    visitNaNLiteral: function() {
        return NaN;
    },

    visitStringLiteral: function(node) {
        return node.value;
    },

    visitMomentLiteral: function(node) {
        return node.value;
    },

    visitDurationLiteral: function(node) {
        return JuttleMoment.duration(node.value).seconds();
    },

    visitFilterLiteral: function(node) {
        return this.visit(node.ast);
    },

    visitArrayLiteral: function(node) {
        var self = this;

        return node.elements.map(function(e) { return self.visit(e); });
    },

    visitUnaryExpression: function(node) {
        switch (node.operator) {
            case 'NOT':
                return {
                    filter: {
                        bool: { must_not: [this.visit(node.expression).filter] }
                    },
                };

            case '*':
                return this.visit(node.expression);

            default:
                throw new Error('Invalid operator: ' + node.operator + '.');
        }
    },

    visitBinaryExpression: function(node) {
        var left, right, filter, elements;

        switch (node.operator) {
            case 'AND':
                left = this.visit(node.left);
                right = this.visit(node.right);

                filter = { bool: { must: [left.filter, right.filter] } };
                break;

            case 'OR':
                left = this.visit(node.left);
                right = this.visit(node.right);

                filter = { bool: { should: [left.filter, right.filter] } };
                break;

            case '==':
                elements = this._getQueryElements(node);

                if (elements.field === this.skipField) {
                    filter = match_all_filter;
                } else if (elements.value === null) {
                    filter = { missing: { field: elements.field } };
                } else {
                    filter = { term: {} };
                    filter.term[elements.field] = elements.value;
                }
                break;

            case '!=':
                elements = this._getQueryElements(node);

                if (elements.field === this.skipField) {
                    filter = match_all_filter;
                } else if (elements.value === null) {
                    filter = { not: { missing: { field: elements.field } } };
                } else {
                    filter = { not: { term: {} } };
                    filter.not.term[elements.field] = elements.value;
                }
                break;

            case '=~':
                elements = {
                    field: this.visit(node.left),
                    value: this.visit(node.right),
                };

                filter = { query: { wildcard: {} } };
                filter.query.wildcard[elements.field] = elements.value;
                break;

            case '!~':
                elements = {
                    field: this.visit(node.left),
                    value: this.visit(node.right),
                };

                filter = { not: { query: { wildcard: {} } } };
                filter.not.query.wildcard[elements.field] = elements.value;
                break;

            case '<':
            case '>':
            case '<=':
            case '>=':
                elements = this._getQueryElements(node);

                if (elements.field === this.skipField) {
                    filter = match_all_filter;
                    break;
                }

                filter = { range: {} };
                filter.range[elements.field] = {};
                filter.range[elements.field][OPS_TO_ES_OPS[elements.operator]] = elements.value;
                break;

            case 'in':
                elements = {
                    field: this.visit(node.left),
                    value: this.visit(node.right),
                };

                if (elements.field === this.skipField) {
                    filter = match_all_filter;
                    break;
                }

                filter = { terms: {} };
                filter.terms[elements.field] = elements.value;
                break;

            default:
                throw new Error('Invalid operator: ' + node.operator + '.');
        }

        return { filter: filter };
    },

    visitExpressionFilterTerm: function(node) {
        return this.visit(node.expression);
    },

    visitSimpleFilterTerm: function(node) {
        switch (node.expression.type) {
            case 'StringLiteral':
                return {
                    filter: { query: { match_phrase: { '_all': this.visit(node.expression) } } },
                };

            case 'FilterLiteral':
                return this.visit(node.expression);

            default:
                throw new Error('Invalid node type: ' + node.expression.type + '.');
        }
    },

    _getQueryElements: function(node) {
        if (this._isSimpleFieldReference(node.left)) {
            return {
                field: this.visit(node.left),
                value: this.visit(node.right),
                operator: node.operator
            };
        } else if (this._isSimpleFieldReference(node.right)) {
            return {
                field: this.visit(node.right),
                value: this.visit(node.left),
                operator: OPS_TO_INVERTED_OPS[node.operator]
            };
        } else {
            throw new Error('One operand of the "' + node.operator + '" must be a field reference.');
        }
    },

    _isSimpleFieldReference: function(node) {
        return node.type === 'UnaryExpression' &&
            node.operator === '*' &&
            node.expression.type === 'StringLiteral';
    }
});

module.exports = FilterESCompiler;
