package edu.caltech.nanodb.relations;


import java.util.List;
import java.util.SortedMap;

import edu.caltech.nanodb.expressions.ColumnName;


/**
 * This interface provides the operations that can be performed with a tuple.
 * The data stored in a tuple can vary, so there are methods for querying the
 * number, type, and status of the attributes in the tuple.  Values can be
 * retrieved and set on the tuple using the get/set methods on the interface.
 * <p>
 * There is a natural question that arises, where is the tuple data stored?
 * Some tuples may be straight out of a table file, and thus their data will be
 * backed by a buffer page that can be written back to the filesystem.  Other
 * tuples may exist entirely in memory, with no corresponding back-end storage.
 * <p>
 * SQL data types are mapped to/from Java data types as follows:
 * <ul>
 *   <li><tt>TINYINT</tt> - <code>byte</code> (8 bit signed integer)</li>
 *   <li><tt>SMALLINT</tt> - <code>short</code> (16 bit signed integer)</li>
 *   <li>
 *     <tt>INTEGER</tt> (<tt>INT</tt>) - <code>int</code> (32 bit signed
 *     integer)
 *   </li>
 *   <li><tt>BIGINT</tt> - <code>long</code> (64 bit signed integer)</li>
 *   <li><tt>CHAR</tt> and <tt>VARCHAR</tt> - <code>java.lang.String</code></li>
 *   <li><tt>NUMERIC</tt> - <code>java.math.BigDecimal</code></li>
 * </ul>
 */
public interface Tuple {

    /** Returns a count of the number of columns in the tuple. */
    int getColumnCount();

    /** Returns an array of type information for all columns in the database. */
    List<ColumnInfo> getColumnInfos();

    /** Returns the type information for a specific column. */
    ColumnInfo getColumnInfo(int colIndex);

    /**
     * Returns the integer index of the column with the specified name.  If no
     * column has the specified name, this method returns -1.
     */
    int getColumnIndex(String colName);


    /**
     * Given a (possibly wildcard) column-name, this method returns the
     * collection of all columns that match the specified column name.  The
     * collection is a mapping from integer indexes (the keys) to
     * <tt>ColumnInfo</tt> objects from the tuple.
     */
    SortedMap<Integer, ColumnInfo> findColumns(ColumnName colName);


    /**
     * Returns <code>true</code> if the specified column's value is
     * <tt>NULL</tt>.
     */
    boolean isNullValue(int colIndex);


    /**
     * Returns the value of a column, or <code>null</code> if the column's SQL
     * value is <tt>NULL</tt>.
     */
    Object getColumnValue(int colIndex);

    /**
     * Sets the value of a column.  If <code>null</code> is passed, the column
     * is set to the SQL <tt>NULL</tt> value.
     */
    void setColumnValue(int colIndex, Object value);
}
