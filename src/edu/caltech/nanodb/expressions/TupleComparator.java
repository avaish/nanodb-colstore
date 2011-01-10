package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import edu.caltech.nanodb.relations.Tuple;


/**
 * This class allows us to sort and compare tuples based on an order-by
 * specification.  The specification is simply a list of
 * {@link OrderByExpression} objects, and the order of the expressions
 * themselves matters.  Tuples will be ordered by the first expression; if the
 * tuples' values are the same then the tuples will be ordered by the second
 * expression; etc.
 */
public class TupleComparator implements Comparator<Tuple> {

    /** The specification of how to order the tuples being compared. */
    private ArrayList<OrderByExpression> orderSpec;


    /**
     * The environment to use for evaluating order-by expressions against the
     * first tuple.
     */
    private Environment envTupleA = new Environment();


    /**
     * The environment to use for evaluating order-by expressions against the
     * second tuple.
     */
    private Environment envTupleB = new Environment();


    /**
     * Construct a new tuple-comparator with the given ordering specification.
     *
     * @param orderSpec a series of order-by expressions used to order the
     *        tuples being compared
     */
    public TupleComparator(List<OrderByExpression> orderSpec) {
        if (orderSpec == null)
            throw new NullPointerException();

        this.orderSpec = new ArrayList<OrderByExpression>(orderSpec);
    }


    @Override
    public int compare(Tuple a, Tuple b) {

        // Set up the environments for evaluating the order-by specifications.

        envTupleA.clear();
        envTupleA.addTuple(a);

        envTupleB.clear();
        envTupleB.addTuple(b);

        int compareResult = 0;

        // For each order-by spec, evaluate the expression against both tuples,
        // and compare the results.
        for (OrderByExpression entry : orderSpec) {
            Expression expr = entry.getExpression();

            Comparable valueA = (Comparable) expr.evaluate(envTupleA);
            Comparable valueB = (Comparable) expr.evaluate(envTupleB);

            if (valueA == null) {
                if (valueB != null)
                    compareResult = -1;
                else
                    compareResult = 0;
            }
            else if (valueB == null) {
                compareResult = 1;
            }
            else {
                compareResult = valueA.compareTo(valueB);
            }

            if (compareResult != 0) {
                if (!entry.isAscending())
                    compareResult = -compareResult;

                break;
            }
        }

        return compareResult;
    }
}
