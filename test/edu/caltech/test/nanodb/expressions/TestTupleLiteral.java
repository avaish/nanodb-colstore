package edu.caltech.test.nanodb.expressions;


import java.util.SortedMap;

import org.testng.annotations.*;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;


/**
 * This test class exercises the functionality of the
 * {@link edu.caltech.nanodb.expressions.TupleLiteral} class.
 **/
@Test
public class TestTupleLiteral {

  public void testSimpleCtors() {
    TupleLiteral tuple;

    tuple = new TupleLiteral();
    assert tuple.getColumnCount() == 0;

    tuple = new TupleLiteral(5);
    assert tuple.getColumnCount() == 5;
    for (int i = 0; i < 5; i++) {
      assert tuple.getColumnValue(i) == null;
      assert tuple.getColumnInfo(i) == null;
      assert tuple.isNullValue(i);
    }
  }


  /** This test exercises the <code>addValue()</code> methods. **/
  public void testAddValues() {
    TupleLiteral tuple = new TupleLiteral();

    tuple.addValue(new Integer(3));
    tuple.addValue(new String("hello"));

    tuple.addValue(
      new ColumnInfo("a", "t1", new ColumnType(SQLDataType.DOUBLE)),
      new Double(2.1));

    tuple.addValue(null);

    tuple.addValue(
      new ColumnInfo("b", "t2", new ColumnType(SQLDataType.BIGINT)),
      new Long(-6L));

    assert tuple.getColumnCount() == 5;

    assert !tuple.isNullValue(0);
    assert !tuple.isNullValue(1);
    assert !tuple.isNullValue(2);
    assert  tuple.isNullValue(3);
    assert !tuple.isNullValue(4);

    assert tuple.getColumnValue(0).equals(new Integer(3));
    assert tuple.getColumnValue(1).equals(new String("hello"));
    assert tuple.getColumnValue(2).equals(new Double(2.1));
    assert tuple.getColumnValue(3) == null;
    assert tuple.getColumnValue(4).equals(new Long(-6L));

    assert tuple.getColumnInfo(0) == null;
    assert tuple.getColumnInfo(1) == null;

    assert tuple.getColumnInfo(2).getName().equals("a");
    assert tuple.getColumnInfo(2).getTableName().equals("t1");

    assert tuple.getColumnInfo(3) == null;

    assert tuple.getColumnInfo(4).getName().equals("b");
    assert tuple.getColumnInfo(4).getTableName().equals("t2");
  }


  /**
   * This test exercises the
   * {@link edu.caltech.nanodb.expressions.TupleLiteral#findColumns} method.
   **/
  public void testFindColumns() {

    TupleLiteral tuple = new TupleLiteral();

    // Index 0:  a
    tuple.addValue(
      new ColumnInfo("a", new ColumnType(SQLDataType.INTEGER)),
      new Integer(5));

    // Index 1:  t1.a
    tuple.addValue(
      new ColumnInfo("a", "t1", new ColumnType(SQLDataType.INTEGER)),
      new Integer(2));

    // Index 2:  t1.b
    tuple.addValue(
      new ColumnInfo("b", "t1", new ColumnType(SQLDataType.INTEGER)),
      new Integer(3));

    // Index 3:  t2.a
    tuple.addValue(
      new ColumnInfo("a", "t2", new ColumnType(SQLDataType.INTEGER)),
      new Integer(1));

    // Index 4:  t2.b
    tuple.addValue(
      new ColumnInfo("b", "t2", new ColumnType(SQLDataType.INTEGER)),
      new Integer(4));

    // Index 5:  b
    tuple.addValue(
      new ColumnInfo("b", new ColumnType(SQLDataType.INTEGER)),
      new Integer(-1));

    SortedMap<Integer, ColumnInfo> found;

    // Find columns that match *
    found = tuple.findColumns(new ColumnName());
    verifyColumns(tuple, found, new int[] {0, 1, 2, 3, 4, 5});

    // Find columns that match t1.*
    found = tuple.findColumns(new ColumnName("t1", null));
    verifyColumns(tuple, found, new int[] {1, 2});

    // Find columns that match t2.*
    found = tuple.findColumns(new ColumnName("t2", null));
    verifyColumns(tuple, found, new int[] {3, 4});

    // Find columns that match t3.*
    found = tuple.findColumns(new ColumnName("t3", null));
    verifyColumns(tuple, found, new int[] {});

    // Find columns that match a
    found = tuple.findColumns(new ColumnName("a"));
    verifyColumns(tuple, found, new int[] {0, 1, 3});

    // Find columns that match b
    found = tuple.findColumns(new ColumnName("b"));
    verifyColumns(tuple, found, new int[] {2, 4, 5});

    // Find columns that match c
    found = tuple.findColumns(new ColumnName("c"));
    verifyColumns(tuple, found, new int[] {});

    // Find columns that match t1.a
    found = tuple.findColumns(new ColumnName("t1", "a"));
    verifyColumns(tuple, found, new int[] {1});

    // Find columns that match t2.b
    found = tuple.findColumns(new ColumnName("t2", "b"));
    verifyColumns(tuple, found, new int[] {4});

    // Find columns that match t3.a
    found = tuple.findColumns(new ColumnName("t3", "a"));
    verifyColumns(tuple, found, new int[] {});
  }


  /**
   * This helper method is used by the {@link #testFindColumns} method to verify
   * that the results from <code>TupleLiteral.findColumns()</code> are correct.
   **/
  private void verifyColumns(TupleLiteral tuple,
    SortedMap<Integer, ColumnInfo> results, int[] expected) {

    assert results.size() == expected.length;

    for (int i = 0; i < expected.length; i++) {
      ColumnInfo foundColInfo = results.remove(expected[i]);
      assert foundColInfo != null;

      ColumnInfo origColInfo = tuple.getColumnInfo(expected[i]);
      assert foundColInfo.equals(origColInfo);
    }

    assert results.size() == 0;
  }
}
