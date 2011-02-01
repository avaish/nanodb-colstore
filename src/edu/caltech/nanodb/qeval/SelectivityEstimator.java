package edu.caltech.nanodb.qeval;


import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.CompareOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.LiteralValue;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.storage.TableStats;

/**
 */
public class SelectivityEstimator {

    /**
     * This constant specifies the default selectivity assumed when a select
     * predicate is too complicated to compute more accurate estimates.  We are
     * assuming that generally people are going to do things that limit the
     * results produced.
     */
    public static final float DEFAULT_SELECTIVITY = 0.25f;


    /**
     * This function computes the selectivity of a selection predicate, using
     * table statistics and other estimates to make an educated guess.  The
     * result is between 0.0 and 1.0, with 1.0 meaning that all rows will be
     * selected by the predicate.
     *
     * @param expr the expression whose selectivity we are estimating
     *
     * @param exprSchema a schema describing the environment that the expression
     *        will be evaluated within
     *
     * @param stats statistics that may be helpful in estimating the selectivity
     *
     * @return the estimated selectivity as a float
     */
    public static float estimateSelectivity(Expression expr, Schema exprSchema,
                                            TableStats stats) {
        float selectivity = 1.0f;

        if (expr instanceof BooleanOperator) {
            // A Boolean AND, OR, or NOT operation.
            BooleanOperator bool = (BooleanOperator) expr;
            selectivity = estimateBoolOpSelectivity(bool, exprSchema, stats);
        }
        else if (expr instanceof CompareOperator) {
            // This is a simple comparison between expressions.
            CompareOperator comp = (CompareOperator) expr;
            selectivity = estimateCompOpSelectivity(comp, exprSchema, stats);
        }
        else {
            // This is some other kind of predicate; assume default selectivity.
            selectivity = DEFAULT_SELECTIVITY;
        }

        return selectivity;
    }


    public static float estimateBoolOpSelectivity(BooleanOperator bool,
        Schema exprSchema, TableStats stats) {

        float selectivity = 1.0f;
        float term;

        int numTerms = bool.getNumTerms();

        switch (bool.getType()) {
        case AND_EXPR:
            // This is an AND:  Multiply conjunct selectivities together.
            for (int i = 0; i < numTerms; i++) {
                term = estimateSelectivity(bool.getTerm(i), exprSchema, stats);
                selectivity *= term;
            }

            break;

        case OR_EXPR:
            // This is an OR:  Multiply the negation of the disjunct
            // selectivities and negate it.
            for (int i = 0; i < numTerms; i++) {
                term = estimateSelectivity(bool.getTerm(i), exprSchema, stats);
                selectivity *= (1.0f - term);
            }
            selectivity = 1.0f - selectivity;
            break;

        case NOT_EXPR:
            // This is a NOT:  Return the negation of the selectivity.
            term = estimateSelectivity(bool.getTerm(0), exprSchema, stats);
            selectivity = 1.0f - term;
            break;

        default:
            // Shouldn't have any other Boolean expression types.
            assert false : "Unexpected Boolean operator type:  " + bool.getType();
        }

        return selectivity;
    }


    public static float estimateCompOpSelectivity(CompareOperator comp,
        Schema exprSchema, TableStats stats) {

        float selectivity = 1.0f;

        // Move the comparison into a normalized order so that it's easier to
        // write the logic for analysis.  Specifically, this will ensure that
        // if we are comparing a column and a value, the column will always be
        // on the left and the value will always be on the right.
        comp.normalize();

        Expression left = comp.getLeftExpression();
        Expression right = comp.getRightExpression();

        if (left instanceof ColumnValue && right instanceof LiteralValue) {
            // Comparison:  column op value

            // Pull out the critical values for making the estimates.
            CompareOperator.Type compType = comp.getType();
            ColumnInfo colInfo = left.getColumnInfo(exprSchema);
            Object value = right.evaluate();

            switch (compType) {
            case EQUALS:
            case NOT_EQUALS:
                // Compute the equality value.  Then, if inequality, invert the
                // result.

                // We can make this selectivity estimate regardless of the
                // column's type, as long as we have a count of the distinct
                // values that appear in the column.

                // TODO

                break;

            case GREATER_OR_EQUAL:
            case LESS_THAN:
                // Compute the greater-or-equal value.  Then, if less-than,
                // invert the result.

                // Only estimate selectivity for this kind of expression if the
                // column's type is numeric.

                // TODO

                break;

            case LESS_OR_EQUAL:
            case GREATER_THAN:
                // Compute the less-or-equal value.  Then, if greater-than,
                // invert the result.

                // Only estimate selectivity for this kind of expression if the
                // column's type is numeric.

                // TODO

                break;

            default:
                // Shouldn't be any other comparison types...
                assert false : "Unexpected compare-operator type:  " + compType;
            }
        }
        else {
            // This comparison is too complicated for us, so just assume the
            // default selectivity.
            selectivity = DEFAULT_SELECTIVITY;
        }

        return selectivity;
    }
}
