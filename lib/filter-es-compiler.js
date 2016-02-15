'use strict';
// Compiler that transforms filter expression AST into an Elasticsearch filter.
//
// The filter is returned in the "filter" property of the toplevel AST node.

/* global JuttleAdapterAPI */
var ASTVisitor = JuttleAdapterAPI.compiler.ASTVisitor;
var JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;

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

class FilterESCompiler extends ASTVisitor {
    constructor(options) {
        super(options);
        options = options || {};
        this.skipField = options.skipField;
        this.filtered_fields = [];
        this.timeField = options.timeField;
    }

    compile(node) {
        var result = this.visit(node);
        result.filtered_fields = this.filtered_fields;
        return result;
    }

    _getNameForField(node) {
        var name = node.name;
        if (name === 'time' && this.timeField) {
            return this.timeField;
        }

        return name;
    }

    visitNullLiteral(node) {
        return null;
    }

    visitBooleanLiteral(node) {
        return node.value;
    }

    visitNumberLiteral(node) {
        return node.value;
    }

    visitInfinityLiteral(node) {
        throw new Error('read elastic filters cannot contain Infinity');
    }

    visitNaNLiteral() {
        throw new Error('read elastic filters cannot contain NaN');
    }

    visitStringLiteral(node) {
        return node.value;
    }

    visitMomentLiteral(node) {
        return node.value;
    }

    visitDurationLiteral(node) {
        return JuttleMoment.duration(node.value).toJSON();
    }

    visitFilterLiteral(node) {
        return this.visit(node.ast);
    }

    visitArrayLiteral(node) {
        return node.elements.map((e) => { return this.visit(e); });
    }

    visitRegExpLiteral(node) {
        throw new Error('read elastic filters cannot contain regular expressions');
    }

    visitUnaryExpression(node) {
        switch (node.operator) {
            case 'NOT':
                return {
                    filter: {
                        bool: { must_not: [this.visit(node.argument).filter] }
                    }
                };

            default:
                throw new Error('Invalid operator: ' + node.operator + '.');
        }
    }

    visitField(node) {
        var name = this._getNameForField(node);
        this.filtered_fields.push(name);
        return name;
    }

    visitBinaryExpression(node) {
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
    }

    visitExpressionFilterTerm(node) {
        return this.visit(node.expression);
    }

    visitFulltextFilterTerm(node) {
        return {
            filter: { query: { match_phrase: { '_all': node.text } } }
        };
    }

    _getQueryElements(node) {
        if (node.left.type === 'Field') {
            return {
                field: this.visit(node.left),
                value: this.visit(node.right),
                operator: node.operator
            };
        } else if (node.right.type === 'Field') {
            return {
                field: this.visit(node.right),
                value: this.visit(node.left),
                operator: OPS_TO_INVERTED_OPS[node.operator]
            };
        } else {
            throw new Error('One operand of the "' + node.operator + '" must be a field reference.');
        }
    }
}

module.exports = FilterESCompiler;
