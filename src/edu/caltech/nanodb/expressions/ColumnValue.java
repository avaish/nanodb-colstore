package edu.caltech.nanodb.expressions;


import java.util.Collection;
import java.util.SortedMap;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;
import org.apache.commons.lang.ObjectUtils;


/**
 * This expression class represents the value of a tuple column.  The
 * column name is stored in the expression object, and the actual value
 * of the column is looked up during evaluation time.
 */
public class ColumnValue extends Expression {

    /** The name of the column. */
    private ColumnName columnName;


    public ColumnValue(ColumnName columnName) {
        if (columnName == null)
          throw new NullPointerException();

        if (columnName.isColumnWildcard()) {
          throw new IllegalArgumentException(
            "Cannot specify wildcard for a column value; got " + columnName + ".");
        }

        this.columnName = columnName;
    }


    /** Returns the column name object */
    public ColumnName getColumnName() {
        return columnName;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        SortedMap<Integer, ColumnInfo> found = schema.findColumns(columnName);

        ColumnInfo colInfo;

        if (found.size() == 1) {
            colInfo = found.get(found.firstKey());
        }
        else if (found.size() == 0) {
            throw new SchemaNameException("Unknown column " + columnName + ".");
        }
        else {
            assert found.size() > 1;
            throw new SchemaNameException("Ambiguous column " + columnName +
                "; found " + found.values() + ".");
        }

        return colInfo;
    }


    public Object evaluate(Environment env) throws ExpressionException {
        if (columnName.isColumnWildcard())
            throw new IllegalStateException("Wildcard columns cannot be evaluated.");

        return env.getColumnValue(columnName);
    }


    /** Since a column-value is a symbol, this method always returns true. */
    public boolean hasSymbols() {
        return true;
    }


    /** Stores this column-value's symbol into the specified set. **/
    public void getAllSymbols(Collection<ColumnName> symbols) {
        symbols.add(columnName);
    }


    @Override
    public String toString() {
        return columnName.toString();
    }


    /**
     * Column values cannot be simplified any further, so this method just
     * returns the expression it's called on.
     */
    public Expression simplify() {
        return this;
    }
  
  
    /**
     * Checks if the argument is an expression with the same structure, but not
     * necesarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ColumnValue) {
            ColumnValue other = (ColumnValue) obj;
            return columnName.equals(other.columnName);
        }
        return false;
    }
  
  
    /**
     * Computes the hashcode of an Expression.  This method is used to see if
     * two expressions might be equal.
     */
    @Override
    public int hashCode() {
        // Since the only thing in a column-value is a column-name, just return
        // that object's hash-code.
        return columnName.hashCode();
    }


    /** Creates a copy of expression. */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ColumnValue expr = (ColumnValue) super.clone();

        // Copy the ColumnName object, since it can be mutated in place.
        expr.columnName = (ColumnName) columnName.clone();

        return expr;
    }
}
