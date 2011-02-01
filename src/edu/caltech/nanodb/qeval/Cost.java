package edu.caltech.nanodb.qeval;


import java.util.ArrayList;

import edu.caltech.nanodb.storage.ColumnStats;


/**
 * This class holds a collection of values that represent the cost of a
 * plan-node (and its subplans).  Leaf plan-nodes generate these values from
 * {@link edu.caltech.nanodb.storage.TableStats concrete statistics} known about
 * table files (which are hopefully also reasonably accurate), and then other
 * plan-nodes use these costs to estimate their own costs.
 */
public class Cost {
    /**
     * The estimated number of tuples produced by the node.  We use a
     * floating-point value because the computations frequently involve
     * fractional numbers and it's not very effective to use integers or longs.
     */
    public float numTuples;


    /** The average tuple size of tuples produced by the node. */
    public float tupleSize;


    /**
     * The estimated number of disk-block accesses required to execute the node.
     */
    public long numBlockIOs;


    /**
     * Statistics about the values that will appear in various columns of a
     * plan-node's output.
     */
    public ArrayList<ColumnStats> columnStats;


    /**
     * Constructs a Cost object from its component fields.
     *
     * @param numTuples the estimated number of tuples that will be produced
     * @param tupleSize the estimated size of the produced tuples in bytes
     * @param numBlockIOs the estimated number of block reads and writes that
     *        will be performed in evaluating the query
     */
    public Cost(float numTuples, float tupleSize, long numBlockIOs) {
        this.numTuples = numTuples;
        this.tupleSize = tupleSize;
        this.numBlockIOs = numBlockIOs;
    }
  
  
    /**
     * Constructs a Cost object from another cost object.
     *
     * @param c the cost-object to duplicate
     */
    public Cost(Cost c) {
        this(c.numTuples, c.tupleSize, c.numBlockIOs);
    }
}
