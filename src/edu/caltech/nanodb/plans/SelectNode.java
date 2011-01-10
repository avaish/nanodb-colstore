package edu.caltech.nanodb.plans;


import java.io.IOException;

import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.BooleanOperator;


/**
 * PlanNode representing the <tt>WHERE</tt> clause in a <tt>SELECT</tt>
 * operation. This is the relational algebra Select operator.
 *
 * @todo Add index scanning.
 * @todo Add cost estimation.
 * @todo Add marking/reseting of position.
 * @todo Add any cleanup functionality.
 **/
public abstract class SelectNode extends PlanNode {

    /** Predicate used for selection. */
    public Expression predicate;


    /** The current tuple that the node is selecting. */
    protected Tuple currentTuple;


    /** True if we have finished scanning or pulling tuples from children. */
    private boolean done;


    /**
     * Constructs a SelectNode that scans a file for tuples.
     *
     * @param predicate the selection criterion
     */
    public SelectNode(Expression predicate) {

        // This node is a Select node.
        super(OperationType.SELECT);

        // This is a scanning node, load file information.
        this.predicate = predicate;
    }


    /**
     * Creates a copy of this select node and its subtree.  This method is used
     * by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        SelectNode node = (SelectNode) super.clone();

        // Copy the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Gets the next tuple selected by the predicate.
     *
     * @return the tuple to be passed up to the next node.
     *
     * @throws java.lang.IllegalStateException if this is a scanning node
     *         with no algorithm or a filtering node with no child, or if
     *         the leftChild threw an IllegalStateException.
     *
     * @throws java.io.IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IllegalStateException, IOException {

        // If this node is finished finding tuples, return null until it is
        // re-initialized.
        if (done)
            return null;

        // Continue to advance the current tuple until it is selected by the
        // predicate.
        do {
            advanceCurrentTuple();

            // If the last tuple in the file (or chain of nodes) did not satisfy the
            // predicate, then the selection process is over, so set the done flag and
            // return null.
            if (currentTuple == null) {
                done = true;
                return null;
            }
        }
        while (!isTupleSelected(currentTuple));

        // The current tuple now satisfies the predicate, so return it.
        return currentTuple;
    }


    /** Helper function that advances the current tuple reference in the node.
     *
     * @throws java.lang.IllegalStateException if this is a node with no
     * algorithm or a filtering node with no child.
     * @throws java.io.IOException if a db file failed to open at some point
     */
    protected abstract void advanceCurrentTuple()
        throws IllegalStateException, IOException;


    private boolean isTupleSelected(Tuple tuple) {
        // If the predicate was not set, return true.
        if (predicate == null)
            return true;

        // Set up the environment and then evaluate the predicate!

        environment.clear();
        environment.addTuple(tuple);
        return predicate.evaluatePredicate(environment);
    }


    /**
     * Helper function that computes the selectivity of an expression that could
     * possibly be a BooleanOperator.
     *
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
            case AND_EXPR :
                // This is an AND
                // Multiply conjunct selectivities together.
                for (int i = 0; i < numTerms; i++) {
                    selectivity *= estimateSelectivity(bool.getTerm(i));
                }
                break;

            case OR_EXPR :
                // This is an OR
                // Multiply the negation of the disjunct selectivities and negate it.
                for (int i = 0; i < numTerms; i++) {
                    selectivity *= (1f - estimateSelectivity(bool.getTerm(i)));
                }
                selectivity = 1f - selectivity;
                break;

            default :
                // This is a NOT
                // Return the negation of the selectivity.
                selectivity = 1f - estimateSelectivity(bool.getTerm(0));
            }
        }
        else {
            // This is some other kind of predicate, assume 10% selectivity.
            selectivity = .1f;
        }

        return selectivity;
    }


    /** Do initialization for the select operation. Resets state variables. */
    @Override
    public void initialize() {
        super.initialize();

        done = false;
        currentTuple = null;
    }
}
