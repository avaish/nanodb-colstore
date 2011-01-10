package edu.caltech.nanodb.plans;


/**
 * Represents various useful costing statistics that a node needs
 * to make its cost computations.
 */
public class Cost {
    /** The estimated number of tuples produced by the node. */
    public long numTuples;


    /** The average tuple size of tuples produced by the node. */
    public float tupleSize;


    /** The estimated number of page reads required to execute the node. */
    public long numPageReads;


    /** Some estimate of the total cost? This probably isn't needed. */
    public int totalCost;


    /** Constructs a Cost object from its component fields. */
    public Cost(long numTuples, float tupleSize, long numPageReads, int totalCost) {
        this.numTuples = numTuples;
        this.tupleSize = tupleSize;
        this.numPageReads = numPageReads;
        this.totalCost = totalCost;
    }
  
  
    /** Constructs a Cost object from another cost object. */
    public Cost(Cost c) {
        this(c.numTuples, c.tupleSize, c.numPageReads, c.totalCost);
    }
}
