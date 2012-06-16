package edu.caltech.nanodb.storage.colstore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.heapfile.DataPage;

/**
 * This class provides the constants and operations necessary for manipulating
 * a table header page within a column store.
 * 
 * Designs are similar to HeaderPage.
 */
public class CSHeaderPage {
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DataPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int ENCODING_MARKER = -1;
    
    public static final int SCHEMA_SIZE_OFFSET = 6;
    
    public static final int SCHEMA_START_OFFSET = 10;

	public static void initNewPage(DBPage dbPage) {
		PageWriter rleWriter = new PageWriter(dbPage);
        rleWriter.setPosition(ENCODING_OFFSET);
        rleWriter.writeInt(ENCODING_MARKER);
        rleWriter.writeInt(0);
        rleWriter.writeInt(14);
	}

	public static void setSchemaSize(DBPage dbPage, int schemaSize) {
		PageWriter rleWriter = new PageWriter(dbPage);
        rleWriter.setPosition(SCHEMA_SIZE_OFFSET);
        rleWriter.writeInt(schemaSize);
	}

}
