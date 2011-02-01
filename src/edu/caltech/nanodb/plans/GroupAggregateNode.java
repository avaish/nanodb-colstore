package edu.caltech.nanodb.plans;


import java.util.List;

import edu.caltech.nanodb.commands.SelectValue;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;


/**
 * This class provides the common functionality necessary for grouping and
 * aggregation.  Concrete subclasses implement grouping and aggregation using
 * different strategies.
 */
public abstract class GroupAggregateNode extends PlanNode {

    protected List<Expression> groupByExprs;


    protected List<SelectValue> selectValues;


    protected GroupAggregateNode(List<Expression> groupByExprs,
                              List<SelectValue> selectValues) {
        super(PlanNode.OperationType.GROUP_AGGREGATE);


        if (groupByExprs == null)
            throw new IllegalArgumentException("groupByExprs cannot be null");

        if (selectValues == null)
            throw new IllegalArgumentException("selectValues cannot be null");

        this.groupByExprs = groupByExprs;
        this.selectValues = selectValues;
    }


    /**
     * This helper function computes the schema of the grouping/aggregate
     * plan-node, based on the schema of its child-plan, and also the
     * expressions specified in the grouping/aggregate operation.
     */
    protected void prepareSchema() {
        Schema childSchema = leftChild.getSchema();
        schema = new Schema();

        // Only the SELECT values are used in the output result.  The
        // expressions specified in the GROUP BY clause do not appear in the
        // result.

        for (SelectValue selVal : selectValues) {
            if (selVal.isWildcard()) {
                throw new IllegalArgumentException(
                    "GROUP BY doesn't support wildcards in SELECT clause");
            }
            else if (selVal.isExpression()) {
                Expression expr = selVal.getExpression();
                ColumnInfo colInfo = expr.getColumnInfo(childSchema);
                schema.addColumnInfo(colInfo);
            }
            else if (selVal.isScalarSubquery()) {
                throw new UnsupportedOperationException(
                    "Scalar subquery support is currently incomplete.");
            }
        }
    }
}
