package edu.caltech.nanodb.storage.colstore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.heapfile.DataPage;

public class RLEPage {
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DataPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int ENCODING_MARKER = 0;
    
    public static final int COUNT_OFFSET = 6;
    
    public static final int NEXT_BLOCK_START_OFFSET = 10;


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
        rleWriter.writeInt(14);
    }

    
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
    	
    	if (write_offset + object.length() + 16 > dbPage.getPageSize()) {
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
}
