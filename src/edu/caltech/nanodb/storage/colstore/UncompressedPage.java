package edu.caltech.nanodb.storage.colstore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileEncoding;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.heapfile.DataPage;

public class UncompressedPage {
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DataPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int ENCODING_MARKER = FileEncoding.NONE.ordinal();
    
    public static final int COUNT_OFFSET = 6;
    
    public static final int NEXT_BLOCK_START_OFFSET = 10;

    public static final int FIRST_BLOCK_OFFSET = 14;


    /**
     * Initialize a newly allocated page.  Currently this involves setting
     * the number of values to 0 and marking the page as normal page.
     *
     * @param dbPage the data page to initialize
     */
    public static void initNewPage(DBPage dbPage) {
        PageWriter uncWriter = new PageWriter(dbPage);
        uncWriter.setPosition(ENCODING_OFFSET);
        uncWriter.writeInt(ENCODING_MARKER);
        uncWriter.writeInt(0);
        uncWriter.writeInt(FIRST_BLOCK_OFFSET);
    }


	public static boolean writeBlock(DBPage dbPage, String object, int position,
			ColumnType type) {
		
		PageReader uncReader = new PageReader(dbPage);
    	PageWriter uncWriter = new PageWriter(dbPage);
    	
    	uncReader.setPosition(ENCODING_OFFSET);
    	
    	if (uncReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	uncReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	int write_offset = uncReader.readInt();
    	
    	if (write_offset + DBPage.getObjectDiskSize(object, type) + 4 
    			> dbPage.getPageSize()) {
    		return false;
    	}
    	
    	uncWriter.setPosition(write_offset + 
    		dbPage.writeObject(write_offset, type, object));
    	
    	uncWriter.writeInt(position);
    	
    	uncReader.setPosition(COUNT_OFFSET);
    	int next_write_pos = uncWriter.getPosition();
    	int count = uncReader.readInt() + 1;
    	
    	uncWriter.setPosition(COUNT_OFFSET);
    	uncWriter.writeInt(count);
    	uncWriter.writeInt(next_write_pos);
    	
    	return true;
	}
	
	public static Object getBlockData(DBPage dbPage, int blockStart, ColumnType 
    		colType) {
    	
    	PageReader uncReader = new PageReader(dbPage);
    	
    	uncReader.setPosition(ENCODING_OFFSET);
    	
    	if (uncReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	uncReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	
    	if (uncReader.readInt() <= blockStart) {
    		return null;
    	}
    	
    	return dbPage.readObject(blockStart, colType);
    }
    
    public static Object getFirstBlockData(DBPage dbPage, ColumnType 
    		colType) {
    	return getBlockData(dbPage, FIRST_BLOCK_OFFSET, colType);
    }
    
    public static int getBlockEndOffset(DBPage dbPage, int blockStart, ColumnType 
    		colType) {
    	
    	PageReader uncReader = new PageReader(dbPage);
    	
    	uncReader.setPosition(ENCODING_OFFSET);
    	
    	if (uncReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	uncReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	
    	if (uncReader.readInt() <= blockStart) {
    		return -1;
    	}
    	
    	return blockStart + DBPage.getObjectDiskSize(dbPage.readObject(
    		blockStart, colType), colType) + 4;
    }
    
    public static int getFirstBlockEndOffset(DBPage dbPage, ColumnType 
    		colType) {
    	return getBlockEndOffset(dbPage, FIRST_BLOCK_OFFSET, colType);
    }
}
