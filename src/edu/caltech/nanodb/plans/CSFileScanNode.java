package edu.caltech.nanodb.plans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.colstore.BlockColumnStoreReader;
import edu.caltech.nanodb.storage.colstore.ColStoreBlock;

/**
 * A select plan-node that scans a column store file, checking the optional predicate
 * against each tuple in the file.
 */
public class CSFileScanNode {

	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CSFileScanNode.class);
    
    /** The table to select from. */
    private TableFileInfo tblFileInfo;
    
    /** The column that the node is scanning over. */
    private ColumnInfo colInfo;
    
    /** The index of the column that the node is scanning over. */
    private int columnIndex;
    
    /** The block reader for column store files. */
    private BlockColumnStoreReader reader;
    
    /** The current data page for the column. */
    private DBPage currentPage;
    
    /** The current block of the column. */
    private ColStoreBlock currentBlock;
    
    /** The predicate to filter out the column values. */
    private Expression predicate;
    
    /** A flag to mark whether the node can produce any more values. */
    boolean done;

    /**
     * Constructs a FileScanNode that reads values from columns, blockwise.
     */
	public CSFileScanNode(TableFileInfo tblFileInfo, ColumnInfo colInfo, 
			Expression pred) {
		this.tblFileInfo = tblFileInfo;
		this.colInfo = colInfo;
		predicate = pred;
		reader = new BlockColumnStoreReader();
		currentPage = null;
		currentBlock = null;
		columnIndex = tblFileInfo.getSchema().getColumnIndex(colInfo);
		done = false;
	}

	public void prepare() throws IOException {
		currentPage = reader.getFirstDataPage(tblFileInfo, columnIndex);
		if (currentPage != null) {
			currentBlock = reader.getFirstBlockInPage(tblFileInfo, currentPage, 
				columnIndex);
		}
	}
	
	/**
     * Advances the current object forward in the block. Gets the next block if 
     * the current block is done. Otherwise gets the next object.
     */
	public Object getNextObject() throws IOException {
		if (done) return null;
		
		Object ret = currentBlock.getNext();
		
		while (ret == null) {
			getNextBlock();
			if (currentBlock == null) {
				return null;
			}
			ret = currentBlock.getNext();
		}
		return ret;
	}
	
	/**
     * Advances the current block forward in the page. Gets the next page if 
     * the current page is done. Otherwise gets the next block.
     */
	private void getNextBlock() throws IOException {
		ColStoreBlock ret = reader.getNextBlockInPage(tblFileInfo, currentPage, 
			columnIndex, currentBlock);
		
		while (ret == null) {
			getNextPage();
			if (currentPage == null) {
				currentBlock = null;
				return;
			}
			ret = reader.getFirstBlockInPage(tblFileInfo, currentPage, columnIndex);
		}
		
		currentBlock = ret;
	}
	
	/**
     * Advances the current page forward in the page. Returns null if there are
     * no more pages.
     */
	private void getNextPage() throws IOException {
		currentPage = reader.getNextDataPage(tblFileInfo, currentPage, columnIndex);
		
		if (currentPage == null) {
			done = true;
		}
	}

	public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("CSFileScan[");
        buf.append("table:  ").append(tblFileInfo.getTableName());
        buf.append(", column:  ").append(colInfo.getColumnName());
        if (predicate != null)
            buf.append(", pred:  ").append(predicate.toString());

        buf.append("]");

        return buf.toString();
    }
	
}
