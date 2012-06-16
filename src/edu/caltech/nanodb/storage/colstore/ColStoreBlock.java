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

/** 
 * The ColStoreBlock is a basic unit of data in a column store, very much in
 * the same manner as a Tuple in a row architecture. Blocks provide a layer of
 * abstraction over the encoding of an actual data file.
 */
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
    
    /** The encoding of the column in the block. */
    private FileEncoding encode;
    
    /** 
     * The contents of the block. Space efficient - if block contents are one
     * values, the contents are not repeated.
     */
    private ArrayList<Object> blockContents;
    
    /** The size of the block. */
    private int blockSize;
    
    /** The iterator over objects in the block. */
    private int iterator;
    
    /** 
     * Creates a column store block given the proper offsets and data from a 
     * column store data file.
     * @param page The page that the block is stored on
     * @param offset The start offset of the block
     * @param end The end offset of the block
     * @param col The column information that the block represents
     * @param enc The encoding of the column
     * @param contents The contents of the block
     * @param size The size of the block
     */
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
    
    /**
     * Returns the page that contains the block.
     *
     * @return the page that contains the block.
     */
    public DBPage getDBPage() {
        return dbPage;
    }

    /**
     * Returns the start offset of the block.
     *
     * @return the start offset of the block.
     */
    public int getOffset() {
        return pageOffset;
    }

    /**
     * Returns the end offset of the block.
     *
     * @return the end offset of the block.
     */
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
    
    /**
     * Returns the block's column info.
     *
     * @return the block's column info.
     */
    public ColumnInfo getColumnInfo() {
        return colInfo;
    }
    
    /**
     * Returns the block's column encoding.
     *
     * @return the block's column encoding.
     */
    public FileEncoding getEncoding() {
		return encode;
    }
    
    /**
     * Returns the next object in the block.
     *
     * @return the next object in the block.
     */
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
    
    /**
     * Returns the next object in the block.
     *
     * @return the next object in the block.
     */
    public ArrayList<Object> asArray() {
    	return blockContents;
    }
    
    /** True if the block contains only one value. Useful for grouping/aggregation. */
    public boolean isOneValue() {
    	return (encode == FileEncoding.RLE || encode == FileEncoding.NONE);
    }
    
    /** True if the block contains sorted values. Useful for grouping/aggregation. */
    public boolean isValueSorted() {
    	return encode == FileEncoding.RLE;
    }
}
