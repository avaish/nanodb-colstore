package edu.caltech.nanodb.storage;


/** This class is a simple wrapper for table-file statistics. **/
public class TableStats {

  /**
   * The total number of data pages in the table file.  This number is in the
   * range of [0, 2<sup>16</sup>).
   **/
  public int numDataPages;


  /**
   * The total number of tuples in the table file.  This number is in the range
   * of [0, 2<sup>32</sup>).
   **/
  public long numTuples;


  /**
   * The average number of bytes in tuples in this table file.  This value is a
   * float, and usually includes a fractional part.
   **/
  public float avgTupleSize;


  /**
   * Create a new table-statistics object with the stats set to the specified
   * values.
   **/
  public TableStats(int numDataPages, long numTuples, float avgTupleSize) {
    this.numDataPages = numDataPages;
    this.numTuples = numTuples;
    this.avgTupleSize = avgTupleSize;
  }


  /**
   * Create a new table-statistics object with all statistics initialized to
   * zero.
   **/
  public TableStats() {
    this(0, 0, 0);
  }


  public String toString() {
    return "TableStats[numDataPages=" + numDataPages + ", numTuples=" +
      numTuples + ", avgTupleSize=" + avgTupleSize + "]";
  }
}
