package edu.caltech.nanodb.commands;


import java.util.*;

import edu.caltech.nanodb.relations.TableConstraintType;


/**
 * Constraints may be specified at the table level, or they may be specified on
 * individual columns.  Obviously, the kinds of constraint allowed depends on
 * what the constraint is associated with.
 **/
public class ConstraintDecl {

  /**
   * The optional name of the constraint, or <code>null</code> if no name was
   * specified.
   **/
  private String name = null;


  /** The type of the constraint. **/
  private TableConstraintType type;


  /**
   * Flag indicating whether the constraint is specified at the table-level or
   * at the column-level.  A value of <code>true</code> indicates that it is a
   * column-level constraint; a value of <code>false</code> indicates that it is
   * a table-level constraint.
   **/
  private boolean columnConstraint;


  /**
   * For {@link edu.caltech.nanodb.relations.TableConstraintType#UNIQUE} and
   * {@link edu.caltech.nanodb.relations.TableConstraintType#PRIMARY_KEY}
   * constraints, this is a list of one or more column names that are
   * constrained.  Note that for a column constraint, this list will contain
   * <i>exactly</i> one column name.  For a table-level constraint, this list
   * will contain one or more column names.
   * <p>
   * For any other constraint type, this will be set to <code>null</code>.
   **/
  private List<String> columnNames = new ArrayList<String>();


  /**
   * For {@link TableConstraintType#FOREIGN_KEY} constraints, this is the table
   * that is referenced by the column.
   * <p>
   * For any other constraint type, this will be set to <code>null</code>.
   **/
  private String refTableName = null;


  /**
   * For {@link TableConstraintType#FOREIGN_KEY} constraints, this is a list of
   * one or more column names in the reference-table that are referenced by the
   * foreign-key constraint.  Note that for a column-constraint, this list will
   * contain <i>exactly</i> one column-name.  For a table-constraint, this list
   * will contain one or more column-names.
   * <p>
   * For any other constraint type, this will be set to <code>null</code>.
   **/
  private List<String> refColumnNames = null;


  /** Create a new unnamed constraint for a table or a table-column. **/
  public ConstraintDecl(TableConstraintType type, boolean columnConstraint) {
    this(type, null, columnConstraint);
  }


  /** Create a new named constraint for a table or a table-column. **/
  public ConstraintDecl(TableConstraintType type, String name,
                        boolean columnConstraint) {
    this.type = type;
    this.name = name;
    this.columnConstraint = columnConstraint;
  }


  /** Returns the type of this constraint. **/
  public TableConstraintType getType() {
    return type;
  }


  /**
   * Returns <code>true</code> if this constraint is associated with a
   * table-column, or <code>false</code> if it is a table-level constraint.
   **/
  public boolean isColumnConstraint() {
    return columnConstraint;
  }


  /**
   * Add a column to the constraint.  This specifies that the constraint governs
   * values in the column.  For column-level constraints, only a single column
   * may be specified.  For table-level constraints, one or more columns may be
   * specified.
   *
   * @throws java.lang.NullPointerException if columnName is null
   * @throws java.lang.IllegalStateException if this is a column-constraint and
   *         there is already one column specified
   *
   * @todo Check column-names as they come in, so that we can ensure that none
   *       are duplicates.
   **/
  public void addColumn(String columnName) {
    if (columnName == null)
      throw new NullPointerException("columnName");

    if (columnNames.size() == 1 && isColumnConstraint()) {
      throw new IllegalStateException(
        "Cannot specify multiple columns in a column-constraint.");
    }

    columnNames.add(columnName);
  }


  /**
   * Add a reference-table to a FOREIGN_KEY constraint.  This specifies the
   * table that constrains the values in the column.
   *
   * @throws java.lang.NullPointerException if tableName is null
   * @throws java.lang.IllegalStateException if this constraint is not a
   *         foreign-key constraint
   *
   * @todo Check column-names as they come in, so that we can ensure that none
   *       are duplicates.
   **/
  public void setRefTable(String tableName) {
    if (type != TableConstraintType.FOREIGN_KEY) {
      throw new IllegalStateException(
        "Reference tables only allowed on FOREIGN_KEY constraints.");
    }

    if (tableName == null)
      throw new NullPointerException("tableName");

    refTableName = tableName;
  }


  /**
   * Add a reference-column to a FOREIGN_KEY constraint.  This specifies the
   * column that constrains the values in the column.  For column-level
   * constraints, only a single column may be specified.  For table-level
   * constraints, one or more columns may be specified.
   *
   * @throws java.lang.NullPointerException if columnName is null
   * @throws java.lang.IllegalStateException if this constraint is not a
   *         foreign-key constraint
   * @throws java.lang.IllegalStateException if this is a column-constraint and
   *         there is already one reference-column specified
   *
   * @todo Check column-names as they come in, so that we can ensure that none
   *       are duplicates.
   **/
  public void addRefColumn(String columnName) {
    if (type != TableConstraintType.FOREIGN_KEY) {
      throw new IllegalStateException(
        "Reference columns only allowed on FOREIGN_KEY constraints.");
    }

    if (columnName == null)
      throw new NullPointerException("columnName");

    if (refColumnNames.size() == 1 && isColumnConstraint()) {
      throw new IllegalStateException(
        "Cannot specify multiple reference-columns in a column-constraint.");
    }

    refColumnNames.add(columnName);
  }


  public String toString() {
    return "Constraint[" + (name != null ? name : "(unnamed)") + " : " +
      type + ", " + (isColumnConstraint() ? "column" : "table") + "]";
  }
}

