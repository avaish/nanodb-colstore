package edu.caltech.nanodb.plans;


import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;

import java.io.IOException;
import java.util.List;


/**
 * This plan node implements a nested-loops join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopsJoinNode extends ThetaJoinNode {

    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;

    /** Most recently joined tuple. */
    protected Tuple joinedTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;


    public NestedLoopsJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necesarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopsJoinNode) {
            NestedLoopsJoinNode other = (NestedLoopsJoinNode) obj;

            return predicate.equals(other.predicate) &&
                   leftChild.equals(other.leftChild) &&
                   rightChild.equals(other.rightChild);
        }

        return false;
    }


    /**
     * Computes the hash-code of the nested-loops plan node.
     */
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoops[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopsJoinNode node = (NestedLoopsJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    /** Computes the cost in terms of disk accesses of the selected algorithm. */
    public Cost estimateCost() {
        Cost leftCost = leftChild.estimateCost();
        Cost rightCost = rightChild.estimateCost();

        float selectivity = 1.0f;
        if (predicate != null)
            selectivity = estimateSelectivity(predicate);

        // Number of tuples in the plain cartesian product is left*right.
        // Multiplying this by the selectivity of the join condition we get
        double numTuplesD = (double)((leftCost.numTuples * rightCost.numTuples) *
            selectivity);

        numTuplesD = Math.ceil(numTuplesD);

        // Since tuple schemas are concatenated, we add the tuple sizes.
        float tupleSize = leftCost.tupleSize + rightCost.tupleSize;

        // In a nested loops join, the right table must be fully read once for
        // each row in the left table.
        long numPages = leftCost.numPageReads +
            (leftCost.numTuples * rightCost.numPageReads);

        return new Cost((long) numTuplesD, tupleSize, numPages, 0);
    }


    public void initialize() {
        super.initialize();

        done = false;
        leftTuple = null;
        rightTuple = null;
        joinedTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;

        while (getTuplesToJoin()) {
            if (canJoinTuples()) {
                joinedTuple = joinTuples(leftTuple, rightTuple);
                return joinedTuple;
            }
        }

        return null;
    }


    private boolean getTuplesToJoin() throws IOException {
        if (leftTuple == null && !done) {
            leftTuple = leftChild.getNextTuple();
            if (leftTuple == null) {
                done = true;
                return !done;
            }
        }

        rightTuple = rightChild.getNextTuple();
        if (rightTuple == null) {
            // Reached end of right relation.  Need to do two things:
            //   * Go to next tuple in left relation.
            //   * Restart iteration over right relation.

            leftTuple = leftChild.getNextTuple();
            if (leftTuple == null) {
                // Reached end of left relation.  All done.
                done = true;
                return !done;
            }

            rightChild.initialize();
            rightTuple = rightChild.getNextTuple();
            if (rightTuple == null) {
                // Right relation is empty!  All done.
                done = true;
                // Redundant:  return !done;
            }
        }

        return !done;
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftTuple);
        environment.addTuple(rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition()
        throws IllegalStateException {

        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark()
        throws IllegalStateException {

        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
