package edu.caltech.nanodb.storage.colstore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.heapfile.DataPage;

/**
 * This class provides the constants and operations necessary for manipulating
 * a generic data page within a column store.
 * 
 * Designs are similar to DataPage.
 */
public class CSDataPage {
	private static Logger logger = Logger.getLogger(DataPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int COUNT_OFFSET = 6;
	
    /** Get the encoding of the page. */
	public static int getEncoding(DBPage dbPage) {
		PageReader reader = new PageReader(dbPage);
		reader.setPosition(ENCODING_OFFSET);
		return reader.readInt();
	}
	
	/** Get the count of the page. */
	public static int getCount(DBPage dbPage) {
		PageReader reader = new PageReader(dbPage);
		reader.setPosition(COUNT_OFFSET);
		return reader.readInt();
	}
}
