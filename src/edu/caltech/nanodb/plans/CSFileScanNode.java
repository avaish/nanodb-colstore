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

public class CSFileScanNode {

	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CSFileScanNode.class);
    
    private TableFileInfo tblFileInfo;
    
    private ColumnInfo colInfo;
    
    private int columnIndex;
    
    private BlockColumnStoreReader reader;
    
    private DBPage currentPage;
    
    private ColStoreBlock currentBlock;
    
    private Expression predicate;
    
    boolean done;

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
