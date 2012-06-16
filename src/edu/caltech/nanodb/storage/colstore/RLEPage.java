package edu.caltech.nanodb.storage.colstore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileEncoding;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.heapfile.DataPage;

public class RLEPage {
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DataPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int ENCODING_MARKER = FileEncoding.RLE.ordinal();
    
    public static final int COUNT_OFFSET = 6;
    
    public static final int NEXT_BLOCK_START_OFFSET = 10;

    public static final int FIRST_BLOCK_OFFSET = 14;

    
    /**
     * Initialize a newly allocated RLE page.  Currently this involves setting
     * the number of values to 0 and marking the page as an RLE encoded page.
     *
     * @param dbPage the data page to initialize
     */
    public static void initNewPage(DBPage dbPage) {
        PageWriter rleWriter = new PageWriter(dbPage);
        rleWriter.setPosition(ENCODING_OFFSET);
        rleWriter.writeInt(ENCODING_MARKER);
        rleWriter.writeInt(0);
        rleWriter.writeInt(FIRST_BLOCK_OFFSET);
    }

    /** Writes a RLE block. */
    public static boolean writeBlock(DBPage dbPage, String object, int start, 
    	int length, ColumnType colType) throws IllegalArgumentException {
    	
    	PageReader rleReader = new PageReader(dbPage);
    	PageWriter rleWriter = new PageWriter(dbPage);
    	
    	rleReader.setPosition(ENCODING_OFFSET);
    	
    	if (rleReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	rleReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	int write_offset = rleReader.readInt();
    	
    	if (write_offset + DBPage.getObjectDiskSize(object, colType) + 8
    			> dbPage.getPageSize()) {
    		return false;
    	}
    	
    	rleWriter.setPosition(write_offset + 
    		dbPage.writeObject(write_offset, colType, object));
    	
    	rleWriter.writeInt(start);
    	rleWriter.writeInt(length);
    	
    	rleReader.setPosition(COUNT_OFFSET);
    	int next_write_pos = rleWriter.getPosition();
    	int count = rleReader.readInt() + length;
    	
    	rleWriter.setPosition(COUNT_OFFSET);
    	rleWriter.writeInt(count);
    	rleWriter.writeInt(next_write_pos);
    	
    	return true;
    }
    
    /** Read block from disk. */
    public static Object getBlockData(DBPage dbPage, int blockStart, ColumnType 
    		colType) {
    	
    	PageReader rleReader = new PageReader(dbPage);
    	
    	rleReader.setPosition(ENCODING_OFFSET);
    	
    	if (rleReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	rleReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	
    	if (rleReader.readInt() <= blockStart) {
    		return null;
    	}
    	
    	return dbPage.readObject(blockStart, colType);
    }
    
    /** Read first block from disk. */
    public static Object getFirstBlockData(DBPage dbPage, ColumnType 
    		colType) {
    	return getBlockData(dbPage, FIRST_BLOCK_OFFSET, colType);
    }
    
    /** Read block length from disk. */
    public static int getBlockLength(DBPage dbPage, int blockStart, ColumnType 
    		colType) {
    	
    	PageReader rleReader = new PageReader(dbPage);
    	
    	rleReader.setPosition(ENCODING_OFFSET);
    	
    	if (rleReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	rleReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	
    	if (rleReader.readInt() <= blockStart) {
    		return -1;
    	}
    	
    	rleReader.setPosition(blockStart + DBPage.getObjectDiskSize
    			(dbPage.readObject(blockStart, colType), colType) + 4);
    	
    	return rleReader.readInt();
    }
    
    /** Read first block length from disk. */
    public static int getFirstBlockLength(DBPage dbPage, ColumnType 
    		colType) {
    	return getBlockLength(dbPage, FIRST_BLOCK_OFFSET, colType);
    }
    
    /** Compute block end offset from disk. */
    public static int getBlockEndOffset(DBPage dbPage, int blockStart, ColumnType 
    		colType) {
    	
    	PageReader rleReader = new PageReader(dbPage);
    	
    	rleReader.setPosition(ENCODING_OFFSET);
    	
    	if (rleReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	rleReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	
    	if (rleReader.readInt() <= blockStart) {
    		return -1;
    	}
    	
    	return blockStart + DBPage.getObjectDiskSize(dbPage.readObject(
    		blockStart, colType), colType) + 8;
    }
    
    /** Compute first block end offset from disk. */
    public static int getFirstBlockEndOffset(DBPage dbPage, ColumnType 
    		colType) {
    	return getBlockEndOffset(dbPage, FIRST_BLOCK_OFFSET, colType);
    }
}
