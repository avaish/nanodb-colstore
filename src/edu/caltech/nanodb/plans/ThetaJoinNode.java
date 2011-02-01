package edu.caltech.nanodb.plans;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.relations.ColumnInfo;


/**
 * PlanNode representing the <tt>FROM</tt> clause in a <tt>SELECT</tt>
 * operation. This is the relational algebra ThetaJoin operator.
 *
 * @todo <ul>
 *         <li>FIX PREDICATE CHECK</li>
 *         <li>ADD MERGE, HASH JOINS</li>
 *
 *         <li>Add cost estimation.</li>
 *         <li>Add marking/reseting of position.</li>
 *         <li>Add any cleanup functionality.</li>
 *       </ul>
 **/
public abstract class ThetaJoinNode extends PlanNode {

    /** The type of the join operation to perform. */
    public JoinType joinType;


    /** Join condition. */
    public Expression predicate;


    /**
     * The cached schema of the left subplan, used for join-predicate
     * evaluation.
     */
    protected Schema leftSchema;


    /**
     * The cached schema of the right subplan, used for join-predicate
     * evaluation.
     */
    protected Schema rightSchema;


    /** True if the schema of this node needs to be swapped. */
    protected boolean schemaSwapped = false;


    /**
     * Constructs a ThetaJoinNode that joins the tuples from the left and right
     * subplans, using the specified join type and join predicate.
     *
     * @param leftChild the left relation
     *
     * @param rightChild the right relation
     *
     * @param joinType the type of join operation to perform
     *
     * @param predicate the join condition
     */
    public ThetaJoinNode(PlanNode leftChild, PlanNode rightChild,
        JoinType joinType, Expression predicate) {

        super(OperationType.THETA_JOIN, leftChild, rightChild);

        if (joinType == null)
            throw new IllegalArgumentException("joinType cannot be null");

        this.joinType = joinType;
        this.predicate = predicate;
    }


    /**
     * Combine the left tuple and the right tuple. If schemaSwapped is set to
     * true, the tuples are copied in the opposite order.  This can only happen if
     * swap() was called an odd number of times, switching the left and right
     * subtrees.
     *
     * @param left the left tuple
     * @param right the right tuple
     * @return the combined tuple
     */
    protected Tuple joinTuples(Tuple left, Tuple right) {

        TupleLiteral joinedTuple = new TupleLiteral();

        // appendTuple() also copies schema information from the source tuples.
        if (!schemaSwapped) {
            joinedTuple.appendTuple(left);
            joinedTuple.appendTuple(right);
        }
        else {
            joinedTuple.appendTuple(right);
            joinedTuple.appendTuple(left);
        }

        return joinedTuple;
    }


    /**
     * Do initialization for the join operation. Resets state variables.
     * Initialize both children.
     */
    public void initialize() {
        super.initialize();

        if (joinType != JoinType.CROSS && joinType != JoinType.INNER) {
            throw new UnsupportedOperationException(
                "We don't support joins of type " + joinType + " yet!");
        }

        leftChild.initialize();
        rightChild.initialize();
    }


    /**
     * Return the list of ColumnInfo objects that will make up the resulting
     * schema of this node. For joins, we must combine the two schemas.
     */
    protected void prepareSchema() {
        leftSchema = leftChild.getSchema();
        rightSchema = rightChild.getSchema();

        schema = new Schema();
        if (!schemaSwapped) {
            schema.append(leftSchema);
            schema.append(rightSchema);
        }
        else {
            schema.append(rightSchema);
            schema.append(leftSchema);
        }
    }


    /**
     * Swaps the left child and right child subtrees. Ensures that the schema of
     * the node does not change in the swap, so that this is still a valid query
     * plan.
     */
    public void swap() {
        PlanNode left = leftChild;
        leftChild = rightChild;
        rightChild = left;
    
        schemaSwapped = !schemaSwapped;
    }


    /**
     * Returns true if the schema is swapped in this theta join node.
     */
    public boolean isSwapped() {
        return schemaSwapped;
    }
}
