package edu.caltech.nanodb.plans;


import java.io.IOException;

import java.util.List;

import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.qeval.Cost;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This plan-node implements the Rename relational algebra operation, for
 * either renaming a table in a query, or assigning a table-name to a derived
 * relation.  <b>Note that at present, it is not possible to rename individual
 * columns.</b>  That should be done with the {@link ProjectNode} instead.
 */
public class RenameNode extends PlanNode {

    /** The result table-name to use in the output schema of this plan-node. */
    private String resultTableName;

    public RenameNode(PlanNode subplan, String resultTableName) {
        super(OperationType.RENAME, subplan);

        if (resultTableName == null)
            throw new IllegalArgumentException("resultTableName cannot be null");

        this.resultTableName = resultTableName;
    }

    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return leftChild.resultsOrderedBy();
    }

    @Override
    public boolean supportsMarking() {
        return leftChild.supportsMarking();
    }

    @Override
    public boolean requiresLeftMarking() {
        return false;
    }

    @Override
    public boolean requiresRightMarking() {
        return false;
    }

    @Override
    public Cost estimateCost() {
        return leftChild.estimateCost();
    }

    @Override
    protected void prepareSchema() {
        // Copy the left child's schema, then change the schema's table-name.
        schema = new Schema(leftChild.getSchema());
        schema.setTableName(resultTableName);
    }

    @Override
    public void initialize() {
        super.initialize();
        leftChild.initialize();
    }

    @Override
    public Tuple getNextTuple() throws IOException {
        return leftChild.getNextTuple();
    }

    @Override
    public void markCurrentPosition() throws UnsupportedOperationException {
        leftChild.markCurrentPosition();
    }

    @Override
    public void resetToLastMark() {
        leftChild.resetToLastMark();
    }

    @Override
    public void cleanUp() {
        leftChild.cleanUp();
    }

    @Override
    public String toString() {
        return "Rename[resultTableName=" + resultTableName + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RenameNode) {
            RenameNode other = (RenameNode) obj;
            return resultTableName.equals(other.resultTableName) &&
                   leftChild.equals(other.leftChild);
        }

        return false;
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = 37 * hash + resultTableName.hashCode();
        hash = 37 * hash + leftChild.hashCode();
        return hash;
    }
}
