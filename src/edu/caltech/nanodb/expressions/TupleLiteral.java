package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * A simple implementation of the {@link edu.caltech.nanodb.relations.Tuple}
 * interface for storing literal tuple values.
 */
public class TupleLiteral implements Tuple {

    /**
     * This collection stores the tuple's schema; in other words, the types of
     * each of the values in the tuple, if known.  Note that his collection may
     * contain <tt>null</tt> values for certain indexes, if the type of that
     * column is not known.
     */
    private ArrayList<ColumnInfo> types;


    /** The actual values of the columns in the tuple. **/
    private ArrayList<Object> values;


    /**
     * Construct a new tuple-literal that initially has zero columns.  Column
     * values can be added with the {@link #addValue} method, or entire tuples
     * can be appended using the {@link #appendTuple} method.
     */
    public TupleLiteral() {
        types = new ArrayList<ColumnInfo>();
        values = new ArrayList<Object>();
    }


    /**
     * Construct a new tuple-literal with the specified number of columns, each
     * of which is initialized to the SQL <tt>NULL</tt> (Java <tt>null</tt>)
     * value.  Each column's type-information is also set to <tt>null</tt>.
     *
     * @param numCols the number of columns that the tuple-literal will
     *        initially contain.
     */
    public TupleLiteral(int numCols) {
        if (numCols < 0) {
            throw new IllegalArgumentException(
                "numCols cannot be negative; got " + numCols);
        }

        types = new ArrayList<ColumnInfo>(numCols);
        values = new ArrayList<Object>(numCols);
        for (int i = 0; i < numCols; i++) {
            types.add(null);
            values.add(null);
        }
    }


    public TupleLiteral(Schema schema) {
        int numCols = schema.numColumns();

        types = new ArrayList<ColumnInfo>(numCols);
        types.addAll(schema.getColumnInfos());

        values = new ArrayList<Object>(numCols);
        for (int i = 0; i < numCols; i++)
            values.add(null);
    }


    /**
     * Constructs a new tuple-literal that is a copy of the specified tuple.
     * After construction, the new tuple-literal object can be manipulated in
     * various ways, just like all tuple-literals.
     */
    public TupleLiteral(Tuple tuple) {
        // Initialize to an empty tuple, then copy over the other tuple's
        // contents.
        this();
        appendTuple(tuple);
    }


    /**
     * Appends the specified value to the end of the tuple-literal.  The type
     * information for the value is set to <code>null</code>.
     */
    public void addValue(Object value) {
        addValue(null, value);
    }


    /**
     * Appends the specified value to the end of the tuple-literal, and also
     * stores the specified type information.
     */
    public void addValue(ColumnInfo type, Object value) {
        types.add(type);
        values.add(value);
    }


    /**
     * Appends the specified tuple's contents to this tuple-literal object.
     * Both the values and the type information are copied from the input tuple.
     */
    public void appendTuple(Tuple tuple) {
        for (int i = 0; i < tuple.getColumnCount(); i++)
            addValue(tuple.getColumnInfo(i), tuple.getColumnValue(i));
    }


    public int getColumnCount() {
        assert types.size() == values.size();
        return values.size();
    }


    public ArrayList<ColumnInfo> getColumnInfos() {
        return types;
    }


    public ColumnInfo getColumnInfo(int colIndex) {
        return types.get(colIndex);
    }


    /**
     * This method allows column information to be specified (or overriden) for
     * a particular column in the tuple-literal.
     */
    public void setColumnInfo(int colIndex, ColumnInfo colInfo) {
        types.set(colIndex, colInfo);
    }


    /**
     * This method iterates through all type information in this tuple-literal
     * and sets all the table names to be the specified table name.
     *
     * @design (donnie) At present, this method does this by replacing each
     *         {@link edu.caltech.nanodb.relations.ColumnInfo} object with a new
     *         object with updated information.  This is because
     *         <code>ColumnInfo</code> is currently immutable.
     *
     */
    public void setTableName(String tableName) {
        for (int i = 0; i < types.size(); i++) {
            ColumnInfo type = types.get(i);
            if (type != null) {
                type = new ColumnInfo(type.getName(), tableName, type.getType());
                types.set(i, type);
            }
        }
    }


    // Let javadoc copy the docs from the interface spec.
    public int getColumnIndex(String colName) {
        SortedMap<Integer, ColumnInfo> found = findColumns(new ColumnName(colName));

        int index = -1;

        if (found.size() == 1)
            index = found.firstKey();
        else if (found.size() > 1) {
            throw new RuntimeException("Ambiguous column name \"" + colName +
                "\" matches multiple columns in this tuple.");
        }

        return index;
    }


    // Let javadoc copy the docs from the interface spec.
    public SortedMap<Integer, ColumnInfo> findColumns(ColumnName colName) {

        TreeMap<Integer, ColumnInfo> found = new TreeMap<Integer, ColumnInfo>();

        for (int i = 0; i < types.size(); i++) {
            ColumnInfo colInfo = types.get(i);
            if (matches(colInfo, colName))
                found.put(new Integer(i), colInfo);
        }

        return found;
    }


    /**
     * <ul>
     *   <li><tt>*</tt> matches all inputs</li>
     *   <li>
     *     <tt>col</tt> matches any input where <tt>column</tt> == <tt>col</tt>
     *   </li>
     *   <li>
     *     <tt>tbl.*</tt> matches any input where <tt>table</tt> == <tt>tbl</tt>
     *   </li>
     *   <li>
     *     <tt>tbl.col</tt> matches any input where <tt>table</tt> == <tt>tbl</tt>
     *     and <tt>column</tt> == <tt>col</tt>
     *   </li>
     * </ul>
     */
    private boolean matches(ColumnInfo colInfo, ColumnName colName) {
        // These cases deal with wildcard column-names; i.e. the ColumnName object
        // has null for the column-name value.
        if (colName.isColumnWildcard()) {
            if (!colName.isTableSpecified()) {
                // The ColumnName represents a wildcard with no table name (*),
                // so it always matches.
                return true;
            }
            else {
                // The ColumnName represents a wildcard with a table name
                // (tbl.*), so we check the ColumnInfo's table name and
                // ignore the column name.
                return colName.getTableName().equals(colInfo.getTableName());
            }
        }

        // If we get here then we know that the ColumnName specifies a value
        // for the column-name (but possibly not for the table-name).
        // ColumnInfo objects *always* have a value for the column-name, but
        // the table-name is optional.
        if (colName.isTableSpecified()) {
            // The ColumnName object specifies a table-name.  Only return true
            // if the ColumnInfo has the exact same values for both table-name
            // and column-name.  Note that this is a stricter matching criterion
            // than the next case.  If a caller specifies a ColumnName object
            // with a table-name value, it is clear that they are looking for
            // something very specific.  If the ColumnInfo doesn't specify any
            // table information, we can't assume that it's a match.
            return colName.getTableName().equals(colInfo.getTableName()) &&
                   colName.getColumnName().equals(colInfo.getName());
        }
        else {
            // The ColumnName object doesn't specify a table-name.  If the
            // column names are the same then it's a match, regardless of
            // whether the ColumnInfo has a table-name.  (For example, I might
            // have "SELECT a FROM tbl" where internally I have ColumnInfo
            // objects for "tbl" with values like "tbl.a", "tbl.b".  It is good
            // to match "a" with "tbl.a" in such cases.)  Note that this is a
            // looser matching criterion than if the ColumnName specifies a
            // table-name.
            return colName.getColumnName().equals(colInfo.getName());
        }
    }


    public boolean isNullValue(int colIndex) {
        return (values.get(colIndex) == null);
    }


    public Object getColumnValue(int colIndex) {
        return values.get(colIndex);
    }


    /** This method allows the value of a specific column to be changed. */
    public void setColumnValue(int colIndex, Object value) {
        values.set(colIndex, value);
    }
}
