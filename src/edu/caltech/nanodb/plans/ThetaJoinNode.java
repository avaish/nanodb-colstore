package edu.caltech.nanodb.plans;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.relations.JoinType;
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

        super(OperationType.THETA_JOIN);

        if (leftChild == null)
            throw new IllegalArgumentException("leftChild cannot be null");

        if (rightChild == null)
            throw new IllegalArgumentException("rightChild cannot be null");

        if (joinType == null)
            throw new IllegalArgumentException("joinType cannot be null");

        this.leftChild = leftChild;
        leftChild.parent = this;

        this.rightChild = rightChild;
        rightChild.parent = this;

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
     * Helper function that computes the selectivity of an expression that could
     * possibly be a BooleanOperator.
     * @design  Selectivity of regular expressions is assumed to be .10
     *
     * @param expr the expression whose selectivity we are computing
     * @return the selectivity as a float
     */
    protected float estimateSelectivity(Expression expr) {
        float selectivity = 1.0f;

        if (expr instanceof BooleanOperator) {
            BooleanOperator bool = (BooleanOperator)expr;
      
            int numTerms = bool.getNumTerms();

            switch (bool.getType()) {
            case AND_EXPR:
                // This is an AND
                // Multiply conjunct selectivities together.
                for (int i = 0; i < numTerms; i++)
                    selectivity *= estimateSelectivity(bool.getTerm(i));

                break;

            case OR_EXPR:
                // This is an OR
                // Multiply the negation of the disjunct selectivities and negate it.
                for (int i = 0; i < numTerms; i++)
                    selectivity *= (1.0f - estimateSelectivity(bool.getTerm(i)));

                selectivity = 1.0f - selectivity;
                break;

            case NOT_EXPR:
                // This is a NOT
                // Return the negation of the selectivity.
                selectivity = 1.0f - estimateSelectivity(bool.getTerm(0));
                break;

            default:
                throw new IllegalArgumentException(
                    "Illegal Boolean operator type:  " + bool.getType());
            }
        }
        else {
            // This is some other kind of predicate, assume 10% selectivity.
            selectivity = 0.1f;
        }

        return selectivity;
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
    public List<ColumnInfo> getColumnInfos() {
        List<ColumnInfo> leftInfos = leftChild.getColumnInfos();
        List<ColumnInfo> rightInfos = rightChild.getColumnInfos();
    
        ArrayList<ColumnInfo> joinedInfos = new ArrayList<ColumnInfo>();

        for (ColumnInfo left : leftInfos)
            joinedInfos.add(left);

        for (ColumnInfo right : rightInfos)
            joinedInfos.add(right);

        return joinedInfos;
    }


    /**
     * Swaps the left child and right child subtrees. Ensures that the schema of
     * the node does not change in the swap, so that this is still a valid query
     * plan.
     * Note: This function is no longer needed.
     */
    @Deprecated
    public void swap() {
        PlanNode left = leftChild;
        leftChild = rightChild;
        rightChild = left;
    
        schemaSwapped = !schemaSwapped;
    }


    /**
     * Returns true if the schema is swapped in this theta join node.
     * Note: This function is no longer needed.
     */
    @Deprecated
    public boolean isSwapped() {
        return schemaSwapped;
    }
}
