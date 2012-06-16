package edu.caltech.nanodb.storage.colstore;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileEncoding;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.heapfile.DataPage;
import edu.caltech.nanodb.storage.heapfile.HeapFilePageTuple;

public class BlockColumnStoreReader {
    /**
     * The table reader uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;
    
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BlockColumnStoreReader.class);
    
    /** Information for dictionary encoded pages. */
    private int bitsize;
    
    /** Information for dictionary encoded pages. */
    private int blockNum;

    /** The dictionary for dictionary encoded pages. */
    private HashMap<Integer, Object> dict;
    
    /**
     * Initializes the blocked heap-file table reader.
     */
    public BlockColumnStoreReader() {
        this.storageManager = StorageManager.getInstance();
        bitsize = -1;
        blockNum = -1;
        dict = null;
    }
    
    /** Get first data page. For dictionary encoding, that's the second page. */
    public DBPage getFirstDataPage(TableFileInfo tblFileInfo, int column) throws IOException {
        // Try to fetch the first data page.  If none exists, return null.
        DBPage dbPage = null;
        try {
            dbPage = storageManager.loadDBPage(tblFileInfo.getDBFile(column + 1), 0);
            if (CSDataPage.getEncoding(dbPage) == FileEncoding.DICTIONARY.ordinal()) {
            	bitsize = DictionaryPage.getBitSize(dbPage);
            	blockNum = DictionaryPage.getBlockNum(dbPage);
            	dict = DictionaryPage.constructDictionary(dbPage, 
            		tblFileInfo.getSchema().getColumnInfo(column));
            	
            	dbPage = storageManager.loadDBPage(tblFileInfo.getDBFile(column + 1), 1);
            	
            }
        }
        catch (EOFException e) {
            // Ignore.
        }
        return dbPage;
    }

    /** Get last data page. */
    public DBPage getLastDataPage(TableFileInfo tblFileInfo, int column) throws IOException {
        // Try to fetch the last data page.  If none exists, return null.
        DBFile dbFile = tblFileInfo.getDBFile(column + 1);
        int numPages = dbFile.getNumPages();

        DBPage dbPage = null;
        // If we have at least 2 pages, then we have at least 1 data page!
        if (numPages >= 2)
            dbPage = storageManager.loadDBPage(dbFile, numPages - 1);

        return dbPage;
    }

    /** Get next data page, and return null if it doesn't exist. */
    public DBPage getNextDataPage(TableFileInfo tblFileInfo, DBPage dbPage, int column)
        throws IOException {

        DBFile dbFile = tblFileInfo.getDBFile(column + 1);
        int numPages = dbFile.getNumPages();

        DBPage nextPage = null;
        int nextPageNo = dbPage.getPageNo() + 1;
        if (nextPageNo < numPages)
            nextPage = storageManager.loadDBPage(dbFile, nextPageNo);

        return nextPage;
    }

    /** Get previous data page, and return null if it doesn't exist. */
    public DBPage getPrevDataPage(TableFileInfo tblFileInfo, DBPage dbPage, int column)
        throws IOException {

        DBFile dbFile = tblFileInfo.getDBFile(column + 1);

        DBPage prevPage = null;
        int prevPageNo = dbPage.getPageNo() - 1;
        if (prevPageNo >= 1)
            prevPage = storageManager.loadDBPage(dbFile, prevPageNo);

        return prevPage;
    }

    /** Get the first block in the data page, using data page abstract classes. */
    public ColStoreBlock getFirstBlockInPage(TableFileInfo tblFileInfo, DBPage dbPage, int column) {
        
    	int enc = CSDataPage.getEncoding(dbPage);
    	ColumnInfo colInfo = tblFileInfo.getSchema().getColumnInfo(column);
    	ColumnType colType = colInfo.getType();
    	
    	if (enc == FileEncoding.RLE.ordinal()) {
    		Object object = RLEPage.getFirstBlockData(dbPage, colType);
    		
    		if (object == null) {
    			return null;
    		}
    		
    		ArrayList<Object> contents = new ArrayList<Object>();
    		contents.add(object);
    		
    		int size = RLEPage.getFirstBlockLength(dbPage, colType);
    		int endOffset = RLEPage.getFirstBlockEndOffset(dbPage, colType);
    		
    		return new ColStoreBlock(dbPage, RLEPage.FIRST_BLOCK_OFFSET, 
    			endOffset, colInfo, FileEncoding.RLE, contents, size);
    	}
    	else if (enc == FileEncoding.NONE.ordinal()) {
    		Object object = UncompressedPage.getFirstBlockData(dbPage, colType);
    		
    		if (object == null) {
    			return null;
    		}
    		
    		ArrayList<Object> contents = new ArrayList<Object>();
    		contents.add(object);
    		
    		int size = 1;
    		int endOffset = UncompressedPage.getFirstBlockEndOffset(dbPage, colType);
    		
    		return new ColStoreBlock(dbPage, UncompressedPage.FIRST_BLOCK_OFFSET, 
        		endOffset, colInfo, FileEncoding.NONE, contents, size);
    	}
    	else if (enc == FileEncoding.DICTIONARY.ordinal()) {
    		int iblock = DictionaryPage.getFirstBlockEncodedData(dbPage);
    		
    		if (iblock == Integer.MIN_VALUE) {
    			return null;
    		}
    		
    		short sblock = (short) iblock;
    		
    		ArrayList<Object> contents = new ArrayList<Object>();
    		
    		// Decode the bitstring
    		int mask = (1 << bitsize) - 1;
    		int current;
    		for (int i = 0; i < blockNum; i++) {
    			current = (sblock & mask) >> (i * bitsize);
    			if (current == 0) break;
    			
    			contents.add(dict.get(current));
    			
    			mask = mask << bitsize;
    		}
    		
    		return new ColStoreBlock(dbPage, DictionaryPage.FIRST_BLOCK_OFFSET, 
    			DictionaryPage.FIRST_BLOCK_OFFSET + 2, colInfo, 
    			FileEncoding.DICTIONARY, contents, contents.size());
    	}
 		return null;
    }

    /** Get the next block in the data page, using data page abstract classes. */
    public ColStoreBlock getNextBlockInPage(TableFileInfo tblFileInfo, DBPage dbPage, int column,
        ColStoreBlock block) {
    	
    	if (!(block instanceof ColStoreBlock)) {
    		throw new IllegalArgumentException(
               "Block must be of type ColStoreBlock; got " + block.getClass());
    	}
    	
    	int offset = block.getEndOffset();
    	int enc = CSDataPage.getEncoding(dbPage);
    	ColumnInfo colInfo = tblFileInfo.getSchema().getColumnInfo(column);
    	ColumnType colType = colInfo.getType();
    	
    	if (enc == FileEncoding.RLE.ordinal()) {
    		Object object = RLEPage.getBlockData(dbPage, offset, colType);
    		
    		if (object == null) {
    			return null;
    		}
    		
    		ArrayList<Object> contents = new ArrayList<Object>();
    		contents.add(object);
    		
    		int size = RLEPage.getBlockLength(dbPage, offset, colType);
    		int endOffset = RLEPage.getBlockEndOffset(dbPage, offset, colType);
    		
    		return new ColStoreBlock(dbPage, offset, endOffset, colInfo,
    			FileEncoding.RLE, contents, size);
    	}
    	else if (enc == FileEncoding.NONE.ordinal()) {
    		Object object = UncompressedPage.getBlockData(dbPage, offset, colType);
    		
    		if (object == null) {
    			return null;
    		}
    		
    		ArrayList<Object> contents = new ArrayList<Object>();
    		contents.add(object);
    		
    		int size = 1;
    		int endOffset = UncompressedPage.getBlockEndOffset(dbPage, offset, colType);
    		
    		return new ColStoreBlock(dbPage, offset, endOffset, colInfo, 
    			FileEncoding.NONE, contents, size);
    	}
    	else if (enc == FileEncoding.DICTIONARY.ordinal()) {
    		int iblock = DictionaryPage.getBlockEncodedData(dbPage, offset);
    		
    		if (iblock == Integer.MIN_VALUE) {
    			return null;
    		}
    		
    		short sblock = (short) iblock;
    		
    		ArrayList<Object> contents = new ArrayList<Object>();
    		
    		// Decode the bitstring.
    		int mask = (1 << bitsize) - 1;
    		int current;
    		for (int i = 0; i < blockNum; i++) {
    			current = (sblock & mask) >> (i * bitsize);
    			if (current == 0) break;
    			
    			contents.add(dict.get(current));
    			
    			mask = mask << bitsize;
    		}
    		
    		return new ColStoreBlock(dbPage, offset, offset + 2, colInfo, 
    			FileEncoding.DICTIONARY, contents, contents.size());
    	}
        return null;
    }

}
