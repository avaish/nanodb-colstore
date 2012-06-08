package edu.caltech.nanodb.storage.colstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileEncoding;
import edu.caltech.nanodb.storage.heapfile.DataPage;

public class ColStoreBlock {
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(ColStoreBlock.class);
	
	/** The database page that contains the block's data. */
    private DBPage dbPage;


    /** The offset in the page of the block's start. */
    private int pageOffset;
    
    /**
     * The offset in the page where the block's data ends.  Note that this value
     * is <u>one byte past</u> the end of the block's data; as with most Java
     * sequences, the starting offset is inclusive and the ending offset is
     * exclusive.  Also, as a consequence, this value could be past the end of
     * the byte-array that the tuple resides in, if the tuple is at the end of
     * the byte-array.
     */
    private int endOffset;
    
    /**
     * The column that appears within the block.  We don't use a {@link Schema}
     * object so that we can use this class in a wider range of contexts.
     */
    private ColumnInfo colInfo;
    
    private FileEncoding encode;
    
    private ArrayList<Object> blockContents;
    
    private int blockSize;
    
    private int iterator;
    
    public ColStoreBlock(DBPage page, int offset, int end, ColumnInfo col,
    		FileEncoding enc, ArrayList<Object> contents, int size) {
    	dbPage = page;
    	pageOffset = offset;
    	endOffset = end;
    	colInfo = col;
    	blockContents = contents;
    	blockSize = size;
    	encode = enc;
    	iterator = 0;
    }
    
    public DBPage getDBPage() {
        return dbPage;
    }


    public int getOffset() {
        return pageOffset;
    }


    public int getEndOffset() {
        return endOffset;
    }
    
    /**
     * Returns the storage-size of the tuple in bytes.
     *
     * @return the storage-size of the tuple in bytes.
     */
    public int getStorageSize() {
        return endOffset - pageOffset;
    }
    
    public ColumnInfo getColumnInfo() {
        return colInfo;
    }
    
    public FileEncoding getEncoding() {
		return encode;
    }
    
    public Object getNext() {
    	if (iterator >= blockSize) {
    		return null;
    	}
    	else {
    		Object ret;
    		if (isOneValue()) {
    			ret = blockContents.get(0);
    		}
    		else {
    			ret = blockContents.get(iterator);
    		}
    		iterator++;
    		return ret;
    	}
    }
    
    public ArrayList<Object> asArray() {
    	return blockContents;
    }
    
    public boolean isOneValue() {
    	return (encode == FileEncoding.RLE || encode == FileEncoding.NONE);
    }
    
    public boolean isValueSorted() {
    	return encode == FileEncoding.RLE;
    }
}
