package edu.caltech.nanodb.storage;


import java.util.HashSet;


/**
 * This class facilitates the collection of statistics for a single column of a
 * table being analyzed by the {@link TableManager#analyzeTable(TableFileInfo)}
 * method.  Instances of the class compute the number of distinct values, the
 * number of non-<tt>NULL</tt> values, and for appropriate data types, the
 * minimum and maximum values for the column.
 * <p>
 * The class also makes it very easy to construct a {@link ColumnStats} object
 * from the result of the analysis.
 *
 * @design (Donnie) This class is limited in its ability to efficiently compute
 *         the number of unique values for very large tables.  An
 *         external-memory approach would have to be used to support extremely
 *         large tables.
 */
public class ColumnStatsCollector {

    /**
     * The set of all values seen in this column.  This set could obviously
     * occupy a large amount of memory for large tables.
     */
    private HashSet<Object> uniqueValues;

    /**
     * A count of the number of <tt>NULL</tt> values seen in the column-values.
     */
    private int numNullValues;


    /**
     * The minimum value seen in the column's values, or <tt>null</tt> if the
     * minimum is unknown or won't be computed.
     */
    Comparable minValue;


    /**
     * The maximum value seen in the column's values, or <tt>null</tt> if the
     * maximum is unknown or won't be computed.
     */
    Comparable maxValue;


    public ColumnStatsCollector() {
        uniqueValues = new HashSet<Object>();
        numNullValues = 0;
        minValue = null;
        maxValue = null;
    }


    public void addValue(Object value) {
        if (value == null) {
            numNullValues++;
        }
        else {
            // If the value implements the Comparable interface, use it to
            // update the minimum and maximum values.
            if (value instanceof Comparable) {
                Comparable comp = (Comparable) value;

                if (minValue == null || comp.compareTo(minValue) < 0)
                    minValue = comp;

                if (maxValue == null || comp.compareTo(maxValue) > 0)
                    maxValue = comp;
            }

            // Update the set of unique values.
            uniqueValues.add(value);
        }
    }


    public int getNumNullValues() {
        return numNullValues;
    }


    public int getNumUniqueValues() {
        return uniqueValues.size();
    }


    public Object getMinValue() {
        return minValue;
    }


    public Object getMaxValue() {
        return maxValue;
    }


    public ColumnStats getColumnStats() {
        return new ColumnStats(getNumUniqueValues(), numNullValues,
            minValue, maxValue);
    }
}
