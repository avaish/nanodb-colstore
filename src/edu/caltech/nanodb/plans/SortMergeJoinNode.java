package edu.caltech.nanodb.plans;


import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.CompareOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.LiteralValue;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the basic sort-merge join algorithm for use in join
 * evaluation.
 */
public class SortMergeJoinNode extends ThetaJoinNode {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(SortMergeJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;


    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;


    public SortMergeJoinNode(PlanNode leftChild, PlanNode rightChild,
                             JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    /** This plan-node does not support marking. */
    @Override
    public boolean supportsMarking() {
        return false;
    }


    /** This plan-node does not require marking on the left child-plan. */
    @Override
    public boolean requiresLeftMarking() {
        return false;
    }


    /** This plan-node requires marking on the right child-plan. */
    @Override
    public boolean requiresRightMarking() {
        return true;
    }


    private void prepareJoinExpression() {
        if (predicate instanceof BooleanOperator) {
            BooleanOperator boolOp = (BooleanOperator) predicate;
            if (boolOp.getType() != BooleanOperator.Type.AND_EXPR) {
                throw new IllegalStateException(
                    "Sort-merge join can only handle one or more comparisons " +
                    "between two columns, ANDed together.  Got \"" +
                    predicate + "\"");
            }
            
            for (int i = 0; i < boolOp.getNumTerms(); i++) {
                Expression term = boolOp.getTerm(i);
                if (!(term instanceof CompareOperator)) {
                    throw new IllegalStateException(
                        "Sort-merge join can only handle one or more comparisons " +
                            "between two columns, ANDed together.  Got \"" +
                            predicate + "\"");
                }


            }
        }
        else if (predicate instanceof CompareOperator) {
            
        }
        
    }
    
/***
    private void estimateCompareSelectivity(CompareOperator comp,
        Schema exprSchema, ArrayList<ColumnStats> stats) {

        // Move the comparison into a normalized order so that it's easier to
        // write the logic for analysis.
        comp.normalize();

        Expression left = comp.getLeftExpression();
        Expression right = comp.getRightExpression();

        if (left instanceof ColumnValue && right instanceof ColumnValue) {
            logger.debug("Estimated selectivity of cmp-col-col operator \"" +
                comp + "\" as " + selectivity);
        }

        return selectivity;
    }
***/
    
    


    @Override
    public void prepare() {
        leftChild.prepare();
        rightChild.prepare();

        if (!rightChild.supportsMarking()) {
            throw new IllegalStateException("Sort-merge join requires the " +
                "right child-plan to support marking.");
        }

        // Get the schemas and the result-orderings so that we can analyze the
        // join-expressions.
        
        Schema leftSchema = leftChild.getSchema();
        List<OrderByExpression> leftOrder = leftChild.resultsOrderedBy();

        Schema rightSchema = rightChild.getSchema();
        List<OrderByExpression> rightOrder = rightChild.resultsOrderedBy();
        
        
        
    }


    @Override
    public void initialize() {
        super.initialize();

        done = false;
        leftTuple = null;
        rightTuple = null;
    }


    @Override
    public Tuple getNextTuple() throws IllegalStateException, IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public void markCurrentPosition() {
        throw new UnsupportedOperationException(
            "Sort-merge join plan-node doesn't support marking.");
    }


    @Override
    public void resetToLastMark() {
        throw new UnsupportedOperationException(
            "Sort-merge join plan-node doesn't support marking.");
    }


    @Override
    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }


    /**
     * Returns a string representing this sort-merge join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("SortMergeJoin[");

        // The predicate is expected to be non-null.
        buf.append("pred:  ").append(predicate);

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Checks if the argument is a plan node tree with the same structure,
     * but not necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof SortMergeJoinNode) {
            SortMergeJoinNode other = (SortMergeJoinNode) obj;

            return predicate.equals(other.predicate) &&
                   leftChild.equals(other.leftChild) &&
                   rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the sort-merge join plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + predicate.hashCode();
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /** Creates a copy of this plan node and its subtrees. */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        SortMergeJoinNode node = (SortMergeJoinNode) super.clone();

        // Clone the predicate.
        node.predicate = predicate.duplicate();

        return node;
    }

}
