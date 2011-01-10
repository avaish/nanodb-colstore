package edu.caltech.nanodb.expressions;


/**
 * This class represents an expression that results are ordered by, as well as
 * whether the order is ascending or descending.  Lists of this object specify
 * ordering of results from a query, as well as the ordering of tuples in a
 * sequential file, or attributes in an ordered index.
 */
public class OrderByExpression {
    
    private Expression expression;


    private boolean ascending;


    public OrderByExpression(Expression expression) {
        this(expression, true);
    }


    public OrderByExpression(Expression expression, boolean ascending) {
        this.expression = expression;
        this.ascending = ascending;
    }


    public Expression getExpression() {
        return expression;
    }


    public boolean isAscending() {
        return ascending;
    }
}
