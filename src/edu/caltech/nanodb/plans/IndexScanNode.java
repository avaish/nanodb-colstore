package edu.caltech.nanodb.plans;


import java.util.List;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.relations.ColumnInfo;

import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.TableManager;


/**
 * A select plan-node that scans a table file using an index, checking retrieved
 * tuples against the optional predicate.
 */
public abstract class IndexScanNode extends SelectNode {

    /** Reference to the TableManager object for NanoDB for internal use. */
    private TableManager tableManager;


    /** The table to select from if this node is a leaf. */
    public TableFileInfo tblFileInfo;


    public IndexScanNode(TableFileInfo tblFileInfo, Object index,
                         Expression predicate) {
        super(predicate);

        if (tblFileInfo == null)
            throw new NullPointerException("table cannot be null");

        this.tblFileInfo = tblFileInfo;

        // TODO:  Store the index information.
    }



    /**
     * Creates a copy of this simple filter node node and its subtree.  This
     * method is used by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        IndexScanNode node = (IndexScanNode) super.clone();

        // The table-info doesn't need to be copied since it's immutable.
        node.tblFileInfo = tblFileInfo;
        // TODO:  Copy the index information.

        return node;
    }


    /**
     * Currently we will always say that the index-scan node produces unsorted
     * results.  In actuality, an index scan's results will be sorted if the
     * index is an ordered index, which will almost always be the case.
     */
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** This node supports marking. */
    public boolean supportsMarking() {
        return true;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    /** Returns the node's schema from the table file. */
    public List<ColumnInfo> getColumnInfos() {
        // Grab the column info from the table.
        return tblFileInfo.getSchema().getColumnInfos();
    }
}
