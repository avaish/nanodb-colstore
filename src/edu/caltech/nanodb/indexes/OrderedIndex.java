package edu.caltech.nanodb.indexes;


import edu.caltech.nanodb.expressions.OrderByExpression;

import java.util.List;


/**
 * This interface specifies the operations that ordered indexes generally
 * provide.  If an index doesn't provide a particular
 */
public interface OrderedIndex {

    /**
     * Returns the column(s) that are used to order the records in this ordered
     * index.
     *
     * @return the column(s) that are used to order the records in this ordered
     *         index.
     */
    List<OrderByExpression> getOrderSpec();


    IndexPointer findFirstTupleAtLeast(Object[] values);


    IndexPointer rfindFirstTupleAtMost(Object[] values);


    IndexPointer findNextTuple(IndexPointer entry);


    IndexPointer findPrevTuple(IndexPointer entry);
}
